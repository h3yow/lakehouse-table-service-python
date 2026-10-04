"""表对象：提交、读取、压实、结构演进。

并发与一致性规则（既定规则，详见 README）：
- 提交携带 base_version；若当前版本已前进，则检查期间已提交版本是否触及
  相同分区：未触及则按既定规则合并（rebase 到最新版本），触及则抛出
  CommitConflictError，绝不静默覆盖。压实与结构演进不产生写冲突。
- batch_id 为幂等键：重复提交相同内容返回原版本号；相同 batch_id 但内容
  不同抛出 DuplicateBatchError。
- upsert 按主键在分区内合并，版本序即覆盖序；append 在主键表上拒绝与既有
  数据或批内重复的主键，避免静默产生重复。
- delete 按主键删除：重写该分区文件（去掉命中主键的行）产生新版本，旧版本
  元数据与数据文件不动，因此历史快照仍读得到当时的行；删除主键不存在的行
  是确定的无害结果（分区数据不变，仍产生新版本）。仅在声明了主键的表上可用。
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from typing import Any

from lakehouse.errors import (
    CommitConflictError,
    DuplicateBatchError,
    SchemaEvolutionError,
    TableAlreadyExistsError,
    TableNotFoundError,
    ValidationError,
)
from lakehouse.models import SUPPORTED_TYPES, Column, DataFile, Schema, Version, WriteBatch
from lakehouse.storage import LocalStore

logger = logging.getLogger("lakehouse.table")

_WRITE_OPS = ("append", "upsert", "delete")


class Table:
    def __init__(self, store: LocalStore, name: str):
        self.store = store
        self.name = name
        self._lock = threading.Lock()

    # ---- 元数据 ----
    def current_version(self) -> int:
        return int(self.store.load_table_meta(self.name)["current_version"])

    def get_version(self, version: int | None = None) -> Version:
        if version is None:
            version = self.current_version()
        return self.store.load_version(self.name, version)

    # ---- 写入 ----
    def commit(self, batch: WriteBatch) -> int:
        """提交一个写入批次，返回新版本号（幂等重放时返回原版本号）。"""
        if batch.mode not in _WRITE_OPS:
            raise ValidationError(f"未知写入模式: {batch.mode!r}")
        with self._lock:
            meta = self.store.load_table_meta(self.name)
            current = int(meta["current_version"])
            fingerprint = self._fingerprint(batch)

            # 幂等判定：同一 batch_id 已提交过
            known = meta.get("batches", {}).get(batch.batch_id)
            if known is not None:
                if known["fingerprint"] == fingerprint:
                    logger.info(
                        "幂等重放: table=%s batch=%s -> 已有版本 v%d，不产生新版本",
                        self.name, batch.batch_id, known["version"],
                    )
                    return int(known["version"])
                raise DuplicateBatchError(
                    f"batch_id={batch.batch_id!r} 已以不同内容提交于 v{known['version']}，拒绝"
                )

            cur = self.store.load_version(self.name, current)

            # 冲突检测：base_version 之后是否有其他写提交触及同一分区
            if batch.base_version != current:
                self._check_conflict(batch, base=batch.base_version, current=current)

            # 校验并按当前 schema 转换为列 id 存储；delete 只取主键
            if batch.mode == "delete":
                pk_tuples = self._delete_pks(cur.schema, batch.rows)
                new_files, written, detail = self._apply_delete(
                    cur, batch.partition, pk_tuples, meta
                )
            else:
                id_rows = [self._row_to_ids(cur.schema, r) for r in batch.rows]
                if batch.mode == "append":
                    new_files, written = self._apply_append(cur, batch.partition, id_rows, meta)
                else:
                    new_files, written = self._apply_upsert(cur, batch.partition, id_rows, meta)
                detail = f"rows={len(id_rows)}"

            new_version = current + 1
            version = Version(
                version=new_version,
                batch_id=batch.batch_id,
                operation=batch.mode,
                schema=cur.schema,
                files=tuple(new_files),
                committed_batches=tuple(sorted(set(cur.committed_batches) | {batch.batch_id})),
                parent_version=current,
            )
            self.store.save_version(self.name, version)
            meta["current_version"] = new_version
            meta.setdefault("batches", {})[batch.batch_id] = {
                "fingerprint": fingerprint,
                "version": new_version,
            }
            self.store.save_table_meta(self.name, meta)
            logger.info(
                "提交成功: table=%s batch=%s mode=%s partition=%s %s -> v%d (base=v%d, files=%s)",
                self.name, batch.batch_id, batch.mode, batch.partition,
                detail, new_version, batch.base_version,
                [f.path for f in written],
            )
            return new_version

    def _check_conflict(self, batch: WriteBatch, base: int, current: int) -> None:
        if base < 0 or base > current:
            raise CommitConflictError(
                f"base_version=v{base} 非法（当前 v{current}）"
            )
        for v in range(base + 1, current + 1):
            ver = self.store.load_version(self.name, v)
            if ver.operation not in _WRITE_OPS:
                continue
            prev_paths = {f.path for f in self.store.load_version(self.name, v - 1).files}
            # 写提交的新增文件（含重写分区产生的新文件）所在分区即其触及的分区
            touched = {f.partition for f in ver.files if f.path not in prev_paths}
            if batch.partition in touched:
                raise CommitConflictError(
                    f"提交冲突: batch={batch.batch_id!r} 基于 v{base}，但 v{v} "
                    f"({ver.operation}, batch={ver.batch_id!r}) 已写入分区 "
                    f"{batch.partition!r}；请基于 v{current} 重试"
                )
        logger.info(
            "冲突检测通过: table=%s batch=%s base=v%d current=v%d 分区不相交，按规则 rebase 合并",
            self.name, batch.batch_id, base, current,
        )

    def _apply_append(self, cur: Version, partition: str, id_rows: list[dict[str, Any]], meta: dict):
        pk_ids = self._pk_ids(cur.schema)
        if pk_ids:
            existing = self._partition_rows(cur, partition)
            existing_pks = {self._pk_tuple(r, pk_ids) for r in existing}
            seen: set[tuple] = set()
            for r in id_rows:
                pk = self._pk_tuple(r, pk_ids)
                if pk in seen:
                    raise ValidationError(f"批内主键重复: {pk}")
                if pk in existing_pks:
                    raise ValidationError(
                        f"append 拒绝已存在的主键 {pk}（分区 {partition!r}），请改用 upsert"
                    )
                seen.add(pk)
        rel = self._next_file(meta, partition)
        path = self.store.write_data_file(self.name, partition, rel.rsplit("/", 1)[-1], id_rows)
        new_file = DataFile(path=path, partition=partition, row_count=len(id_rows))
        return list(cur.files) + [new_file], [new_file]

    def _apply_upsert(self, cur: Version, partition: str, id_rows: list[dict[str, Any]], meta: dict):
        pk_ids = self._pk_ids(cur.schema)
        if not pk_ids:
            raise ValidationError("upsert 需要表定义主键")
        merged: dict[tuple, dict[str, Any]] = {}
        order: list[tuple] = []
        for r in self._partition_rows(cur, partition):
            pk = self._pk_tuple(r, pk_ids)
            merged[pk] = r
            order.append(pk)
        for r in id_rows:
            pk = self._pk_tuple(r, pk_ids)
            if pk not in merged:
                order.append(pk)
            merged[pk] = r
        rows = [merged[pk] for pk in order]
        rel = self._next_file(meta, partition)
        path = self.store.write_data_file(self.name, partition, rel.rsplit("/", 1)[-1], rows)
        new_file = DataFile(path=path, partition=partition, row_count=len(rows))
        kept = [f for f in cur.files if f.partition != partition]
        return kept + [new_file], [new_file]

    def _delete_pks(self, schema: Schema, rows: tuple[dict[str, Any], ...]) -> list[tuple]:
        """校验 delete 批次并提取主键元组（批内去重，结果确定）。

        delete 批次只接受主键列：缺主键、含非主键列（含未知列）、类型不符
        都以 ValidationError 明确拒绝；无主键的表直接拒绝。
        """
        if not schema.primary_keys:
            raise ValidationError("delete 需要表定义主键")
        pk_cols = [schema.column_by_name(n) for n in schema.primary_keys]
        allowed = set(schema.primary_keys)
        pks: list[tuple] = []
        seen: set[tuple] = set()
        for r in rows:
            extra = set(r) - allowed
            if extra:
                raise ValidationError(
                    f"delete 批次只能包含主键列 {sorted(allowed)}，收到非主键列: {sorted(extra)}"
                )
            key: list[Any] = []
            for c in pk_cols:
                if c.name not in r or r[c.name] is None:
                    raise ValidationError(f"缺少主键列: {c.name!r}")
                self._check_type(c.type, r[c.name], c.name)
                key.append(r[c.name])
            t = tuple(key)
            if t not in seen:
                seen.add(t)
                pks.append(t)
        return pks

    def _apply_delete(self, cur: Version, partition: str, pk_tuples: list[tuple], meta: dict):
        """按主键删除：重写该分区文件，去掉命中主键的行。

        旧文件不删（旧版本仍引用），因此历史快照可见结果不变。分区本来没有
        文件时不产生新文件（确定的无操作）；删除不存在的 主键只是重写后内容
        不变，不会改坏数据。
        """
        pk_ids = self._pk_ids(cur.schema)
        part_files = [f for f in cur.files if f.partition == partition]
        if not part_files:
            logger.info(
                "删除无操作: table=%s 分区=%s 当前版本无文件，%d 个主键无需处理",
                self.name, partition, len(pk_tuples),
            )
            return list(cur.files), [], f"keys={len(pk_tuples)} matched=0 (分区无文件)"
        targets = set(pk_tuples)
        existing = self._partition_rows(cur, partition)
        matched = sum(1 for r in existing if self._pk_tuple(r, pk_ids) in targets)
        remaining = [r for r in existing if self._pk_tuple(r, pk_ids) not in targets]
        rel = self._next_file(meta, partition)
        path = self.store.write_data_file(self.name, partition, rel.rsplit("/", 1)[-1], remaining)
        new_file = DataFile(path=path, partition=partition, row_count=len(remaining))
        kept = [f for f in cur.files if f.partition != partition]
        detail = f"keys={len(pk_tuples)} matched={matched} 剩余行={len(remaining)}"
        return kept + [new_file], [new_file], detail

    # ---- 读取 ----
    def read(self, version: int | None = None, partition: str | None = None) -> list[dict[str, Any]]:
        """按版本读取快照；version=None 表示最新版本。

        只能看到该版本已提交的文件清单中的数据，按该版本的 schema 投影，
        新增列以声明的缺省值补齐，已删除/改名的列不会错位。
        """
        v = self.get_version(version)
        rows: list[dict[str, Any]] = []
        for f in sorted(v.files, key=lambda f: f.path):
            if partition is not None and f.partition != partition:
                continue
            for raw in self.store.read_data_file(self.name, f.path):
                rows.append(self._project(v.schema, raw))
        logger.info(
            "快照读取: table=%s version=v%d partition=%s -> %d 行 (schema 列=%s)",
            self.name, v.version, partition or "*", len(rows),
            [c.name for c in v.schema.columns],
        )
        return rows

    # ---- 压实 ----
    def compact(self, partition: str) -> int:
        """压实指定分区：合并其数据文件为单文件。

        只产生新版本，不删除旧文件，因此任何已发布版本的可见结果不变。
        压实前后对最新版本的读取结果必须一致（此处显式校验）。
        """
        with self._lock:
            meta = self.store.load_table_meta(self.name)
            current = int(meta["current_version"])
            cur = self.store.load_version(self.name, current)
            before = self.read(current, partition)

            part_files = [f for f in cur.files if f.partition == partition]
            if len(part_files) <= 1:
                logger.info("压实跳过: table=%s partition* 分区 %s 仅 %d 个文件", self.name, partition, len(part_files))
                return current

            pk_ids = self._pk_ids(cur.schema)
            merged: list[dict[str, Any]] = []
            index: dict[tuple, int] = {}
            for f in sorted(part_files, key=lambda f: f.path):
                for raw in self.store.read_data_file(self.name, f.path):
                    if pk_ids:
                        pk = self._pk_tuple(raw, pk_ids)
                        if pk in index:
                            merged[index[pk]] = raw
                            continue
                        index[pk] = len(merged)
                    merged.append(raw)

            rel = self._next_file(meta, partition)
            path = self.store.write_data_file(self.name, partition, rel.rsplit("/", 1)[-1], merged)
            new_file = DataFile(path=path, partition=partition, row_count=len(merged))
            files = [f for f in cur.files if f.partition != partition] + [new_file]
            new_version = current + 1
            version = Version(
                version=new_version,
                batch_id=f"__compact__v{new_version}",
                operation="compact",
                schema=cur.schema,
                files=tuple(files),
                committed_batches=cur.committed_batches,
                parent_version=current,
            )
            self.store.save_version(self.name, version)
            meta["current_version"] = new_version
            self.store.save_table_meta(self.name, meta)

            after = self.read(new_version, partition)
            if [self._row_to_ids(cur.schema, r) for r in before] != \
               [self._row_to_ids(cur.schema, r) for r in after]:
                raise AssertionError(  # 不应发生：压实改变了可见结果
                    f"压实校验失败: 分区 {partition!r} v{current} 与 v{new_version} 可见数据不一致"
                )
            logger.info(
                "压实完成: table=%s 分区=%s %d 文件 -> 1 文件, v%d -> v%d, 可见行数 %d 不变",
                self.name, partition, len(part_files), current, new_version, len(after),
            )
            return new_version

    # ---- 结构演进 ----
    def add_column(self, name: str, type: str, default: Any = None) -> int:
        """新增列。历史数据无此列 id，读取时以 default 补齐（缺省语义稳定）。"""
        with self._lock:
            meta = self.store.load_table_meta(self.name)
            cur = self.store.load_version(self.name, int(meta["current_version"]))
            if cur.schema.column_by_name(name) is not None:
                raise SchemaEvolutionError(f"列已存在: {name!r}")
            if type not in SUPPORTED_TYPES:
                raise SchemaEvolutionError(f"不支持的类型: {type!r}")
            if default is not None:
                self._check_type(type, default, name)
            col = Column(id=int(meta["next_column_id"]), name=name, type=type, default=default)
            meta["next_column_id"] = col.id + 1
            schema = Schema(columns=cur.schema.columns + (col,), primary_keys=cur.schema.primary_keys)
            return self._commit_evolve(meta, cur, schema, f"add_column({name})")

    def drop_column(self, name: str) -> int:
        """删除列。历史数据文件中的列 id 保留但不再投影，不会错位。"""
        with self._lock:
            meta = self.store.load_table_meta(self.name)
            cur = self.store.load_version(self.name, int(meta["current_version"]))
            col = cur.schema.column_by_name(name)
            if col is None:
                raise SchemaEvolutionError(f"列不存在: {name!r}")
            if name in cur.schema.primary_keys:
                raise SchemaEvolutionError(f"不允许删除主键列: {name!r}")
            schema = Schema(
                columns=tuple(c for c in cur.schema.columns if c.name != name),
                primary_keys=cur.schema.primary_keys,
            )
            return self._commit_evolve(meta, cur, schema, f"drop_column({name})")

    def rename_column(self, old: str, new: str) -> int:
        """改名。列 id 不变，历史数据不错位；主键名同步更新。"""
        with self._lock:
            meta = self.store.load_table_meta(self.name)
            cur = self.store.load_version(self.name, int(meta["current_version"]))
            col = cur.schema.column_by_name(old)
            if col is None:
                raise SchemaEvolutionError(f"列不存在: {old!r}")
            if cur.schema.column_by_name(new) is not None:
                raise SchemaEvolutionError(f"目标列名已存在: {new!r}")
            cols = tuple(
                Column(id=c.id, name=new if c.name == old else c.name, type=c.type, default=c.default)
                for c in cur.schema.columns
            )
            pks = tuple(new if p == old else p for p in cur.schema.primary_keys)
            return self._commit_evolve(meta, cur, Schema(columns=cols, primary_keys=pks),
                                       f"rename_column({old}->{new})")

    def _commit_evolve(self, meta: dict, cur: Version, schema: Schema, note: str) -> int:
        current = int(meta["current_version"])
        new_version = current + 1
        version = Version(
            version=new_version,
            batch_id=f"__evolve__v{new_version}",
            operation="evolve",
            schema=schema,
            files=cur.files,
            committed_batches=cur.committed_batches,
            parent_version=current,
        )
        self.store.save_version(self.name, version)
        meta["current_version"] = new_version
        self.store.save_table_meta(self.name, meta)
        logger.info(
            "结构演进: table=%s %s -> v%d (列=%s, 主键=%s)",
            self.name, note, new_version,
            [c.name for c in schema.columns], list(schema.primary_keys),
        )
        return new_version

    # ---- 内部工具 ----
    def _next_file(self, meta: dict, partition: str) -> str:
        seq = int(meta.get("file_seq", 0)) + 1
        meta["file_seq"] = seq
        return f"data/{partition}/f{seq:06d}.json"

    def _partition_rows(self, v: Version, partition: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for f in sorted(v.files, key=lambda f: f.path):
            if f.partition == partition:
                rows.extend(self.store.read_data_file(self.name, f.path))
        return rows

    @staticmethod
    def _pk_ids(schema: Schema) -> tuple[int, ...]:
        ids = []
        for name in schema.primary_keys:
            col = schema.column_by_name(name)
            if col is not None:
                ids.append(col.id)
        return tuple(ids)

    @staticmethod
    def _pk_tuple(id_row: dict[str, Any], pk_ids: tuple[int, ...]) -> tuple:
        return tuple(id_row.get(str(i)) for i in pk_ids)

    @staticmethod
    def _project(schema: Schema, id_row: dict[str, Any]) -> dict[str, Any]:
        return {c.name: id_row.get(str(c.id), c.default) for c in schema.columns}

    def _row_to_ids(self, schema: Schema, row: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for c in schema.columns:
            if c.name in row:
                value = row[c.name]
                if value is not None:
                    self._check_type(c.type, value, c.name)
                out[str(c.id)] = value
            elif c.name in schema.primary_keys:
                raise ValidationError(f"缺少主键列: {c.name!r}")
            else:
                out[str(c.id)] = c.default
        unknown = set(row) - {c.name for c in schema.columns}
        if unknown:
            raise ValidationError(f"未知列: {sorted(unknown)}")
        return out

    @staticmethod
    def _check_type(type_: str, value: Any, name: str) -> None:
        ok = (
            (type_ == "int" and isinstance(value, int) and not isinstance(value, bool))
            or (type_ == "float" and isinstance(value, (int, float)) and not isinstance(value, bool))
            or (type_ == "str" and isinstance(value, str))
            or (type_ == "bool" and isinstance(value, bool))
        )
        if not ok:
            raise ValidationError(f"列 {name!r} 期望类型 {type_}，实际值 {value!r}")

    @staticmethod
    def _fingerprint(batch: WriteBatch) -> str:
        payload = json.dumps(
            {"mode": batch.mode, "partition": batch.partition, "rows": list(batch.rows)},
            sort_keys=True, ensure_ascii=False, default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class TableService:
    """管理多张表的入口。"""

    def __init__(self, store: LocalStore):
        self.store = store
        self._lock = threading.Lock()

    def create_table(self, name: str, schema: Schema) -> Table:
        self._validate_schema(schema)
        with self._lock:
            if self.store.table_exists(name):
                raise TableAlreadyExistsError(f"表已存在: {name}")
            next_id = max((c.id for c in schema.columns), default=0) + 1
            meta = {
                "current_version": 0,
                "next_column_id": next_id,
                "file_seq": 0,
                "batches": {},
            }
            self.store.create_table_dir(name, meta)
            v0 = Version(
                version=0,
                batch_id="__create__",
                operation="create",
                schema=schema,
                files=(),
                committed_batches=(),
                parent_version=0,
            )
            self.store.save_version(name, v0)
        logger.info("建表: %s 列=%s 主键=%s", name, [c.name for c in schema.columns], list(schema.primary_keys))
        return Table(self.store, name)

    def get_table(self, name: str) -> Table:
        if not self.store.table_exists(name):
            raise TableNotFoundError(f"表不存在: {name}")
        return Table(self.store, name)

    def list_tables(self) -> list[str]:
        return self.store.list_tables()

    @staticmethod
    def _validate_schema(schema: Schema) -> None:
        if not schema.columns:
            raise ValidationError("表至少需要一个列")
        names = [c.name for c in schema.columns]
        ids = [c.id for c in schema.columns]
        if len(set(names)) != len(names):
            raise ValidationError("列名重复")
        if len(set(ids)) != len(ids):
            raise ValidationError("列 id 重复")
        for c in schema.columns:
            if c.type not in SUPPORTED_TYPES:
                raise ValidationError(f"列 {c.name!r} 类型不支持: {c.type!r}")
            if c.default is not None:
                Table._check_type(c.type, c.default, c.name)
        for pk in schema.primary_keys:
            if schema.column_by_name(pk) is None:
                raise ValidationError(f"主键列不存在: {pk!r}")
