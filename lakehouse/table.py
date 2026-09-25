"""本地湖仓表：并发提交、结构演进、快照读取与分区压实。

并发模型（乐观并发 + 表级提交锁）：
1. 提交在 FileStorage.commit_lock 临界区内读取当前版本，
   因此 "读当前版本 -> 冲突判定 -> 发布新版本" 对同表提交严格串行；
2. 每个写入可携带 expected_version（乐观基版本）与 batch_id（幂等键）；
3. 冲突判定规则（全部基于已发布快照，确定性、可复现）：
   - batch_id 已存在：不产生新版本，返回既有版本（DuplicateBatchError）；
   - expected_version 与当前版本不一致：CommitConflictError；
   - append vs append（含不同分区）：不冲突，天然可合并为新版本；
   - upsert vs 任意分区重叠提交，或 compaction vs 任意提交：
     按 copy-on-write 语义不再安全，明确 CommitConflictError 拒绝，
     绝不静默覆盖；
4. 结构演进与压实同样占版本号、走同一套提交协议。
"""

from __future__ import annotations

import os

from .commit import CommitDecision, log_decision
from .errors import (
    ColumnNotFoundError,
    CommitConflictError,
    DuplicateBatchError,
    SchemaValidationError,
    TableExistsError,
    TableNotFoundError,
    VersionNotFoundError,
)
from .schema import Schema, SchemaChange, SchemaEvolution
from .storage import DataFile, FileStorage, Snapshot, schema_from_dict, schema_to_dict

# 无分区表在清单中使用的统一分区标识
_NULL_PARTITION = "__null_partition__"


class LakehouseTable:
    """一张本地湖仓表的句柄，所有操作均持久化到 FileStorage。"""

    def __init__(self, storage: FileStorage, name: str) -> None:
        self.storage = storage
        self.name = name

    # ------------------------------------------------------------------
    # 建表
    # ------------------------------------------------------------------
    @classmethod
    def create(
        cls,
        storage: FileStorage,
        name: str,
        schema: Schema,
        primary_key_columns: tuple[str, ...] = (),
        partition_column: str | None = None,
    ) -> "LakehouseTable":
        if storage.table_exists(name):
            raise TableExistsError(f"表已存在: {name}")
        if partition_column is not None:
            schema.column_by_name(partition_column)  # 分区列必须存在
        pk_ids: list[int] = []
        for pk in primary_key_columns:
            pk_ids.append(schema.column_by_name(pk).column_id)

        storage.create_table_dir(name)
        # v0：空表初始快照，携带初始 schema；它不是一次数据提交
        v0 = Snapshot(
            version=0,
            schema_id=schema.schema_id,
            parent_version=None,
            operation="schema_change",
            partition=None,
            schema=schema,
        )
        storage.write_snapshot(name, v0)
        storage.write_metadata(
            name,
            {
                "name": name,
                "partition_column": partition_column,
                "primary_key_columns": list(primary_key_columns),
                "primary_key_ids": pk_ids,
                "current_version": 0,
                "current_schema_id": schema.schema_id,
                "batch_index": {},
            },
        )
        log_decision(
            CommitDecision(name, "create", 0, 0, None, None, "accepted", "empty table created at v0")
        )
        return cls(storage, name)

    # ------------------------------------------------------------------
    # 内部：元数据 / schema / live 文件集
    # ------------------------------------------------------------------
    def _meta(self) -> dict:
        return self.storage.read_metadata(self.name)

    def _partition_value(self, row: dict[str, object], meta: dict) -> str:
        pc = meta["partition_column"]
        if pc is None:
            return _NULL_PARTITION
        if pc not in row:
            raise SchemaValidationError(f"分区列 {pc} 在数据行中缺失")
        return str(row[pc])

    def _snapshot_schema(self, version: int) -> Schema:
        """取指定版本发布时刻的完整 schema（快照内携带，保证旧版本结构不漂移）。"""
        snap = self.storage.read_snapshot(self.name, version)
        if snap.schema is not None:
            return snap.schema
        # 兼容路径：沿父链找到携带 schema 的最近快照
        if snap.parent_version is None:
            raise RuntimeError(f"v{version} 缺少 schema 且无父版本")
        return self._snapshot_schema(snap.parent_version)

    def _live_files(self, version: int, partition: str | None = None) -> dict[str, DataFile]:
        """从 v0 顺序回放清单到指定版本，得到该版本可见的 live 数据文件。

        返回 {file_name: DataFile}。压实通过 added/removed 表达文件替换，
        对旧版本回放时其 removed 不会影响旧版本自身的 live 集，
        因此压实不可能改变任何已发布版本的可见结果。
        """
        live: dict[str, DataFile] = {}
        for v in range(0, version + 1):
            snap = self.storage.read_snapshot(self.name, v)
            for f in snap.added_files:
                live[os.path.basename(f.path)] = f
            for removed in snap.removed_files:
                live.pop(os.path.basename(removed), None)
        if partition is not None:
            live = {p: f for p, f in live.items() if f.partition == partition}
        return live

    # ------------------------------------------------------------------
    # 写入 / 演进 / 压实
    # ------------------------------------------------------------------
    def _validate_partition_arg(
        self, rows: list[dict[str, object]], meta: dict, partition: str | None
    ) -> str:
        """统一分区来源：有分区列时以数据行中的分区列为准（且同批一致），
        无分区列时可由参数 partition 显式指定，仅用于分组与过滤。
        """
        pc = meta["partition_column"]
        if pc is not None:
            parts = {str(r[pc]) for r in rows if pc in r}
            if len(parts) == 0:
                raise SchemaValidationError(f"分区列 {pc} 在数据行中缺失")
            if len(parts) > 1:
                raise SchemaValidationError(
                    f"单批次不得跨多个分区提交，发现分区: {sorted(parts)}"
                )
            part = next(iter(parts))
            if partition is not None and partition != part:
                raise SchemaValidationError(
                    f"显式分区参数 {partition!r} 与行内分区列 {part!r} 不一致"
                )
            return part
        return partition if partition is not None else _NULL_PARTITION

    def _write_row_file(
        self,
        rows_by_id: list[dict[int, object]],
        schema: Schema,
        part: str,
    ) -> DataFile:
        """把一批已校验行写成不可变数据文件（按 column_id 存储）。"""
        file_name = self.storage.write_data_file(
            self.name,
            {
                "schema_id": schema.schema_id,
                "partition": part,
                "rows": [{str(cid): val for cid, val in row.items()} for row in rows_by_id],
            },
        )
        return DataFile(
            path=file_name,
            partition=part,
            schema_id=schema.schema_id,
            row_count=len(rows_by_id),
        )

    def _publish(
        self,
        operation: str,
        schema: Schema,
        added: list[DataFile],
        removed: list[str],
        part: str | None,
        batch_id: str | None,
        parent_version: int,
        compaction_of_version: int | None = None,
    ) -> int:
        """调用方已持锁：分配新版本、写快照、推进元数据指针。"""
        new_version = parent_version + 1
        snap = Snapshot(
            version=new_version,
            schema_id=schema.schema_id,
            parent_version=parent_version,
            operation=operation,
            partition=part,
            added_files=tuple(added),
            removed_files=tuple(removed),
            batch_id=batch_id,
            compaction_of_version=compaction_of_version,
            schema=schema,
        )
        self.storage.write_snapshot(self.name, snap)
        meta = self.storage.read_metadata(self.name)
        meta["current_version"] = new_version
        meta["current_schema_id"] = schema.schema_id
        if batch_id is not None:
            meta["batch_index"][batch_id] = {
                "version": new_version,
                "operation": operation,
                "partition": part,
            }
        self.storage.write_metadata(self.name, meta)
        return new_version

    def _check_duplicate(
        self,
        operation: str,
        part: str,
        batch_id: str | None,
        meta: dict,
    ) -> None:
        """幂等批次校验（持锁调用）：同 batch_id 绝不产生第二个版本。"""
        if batch_id is not None and batch_id in meta["batch_index"]:
            rec = meta["batch_index"][batch_id]
            reason = (
                f"batch_id already committed as v{rec['version']} "
                f"(operation={rec['operation']}, partition={rec['partition']}); "
                "idempotent replay, no new version/data"
            )
            log_decision(
                CommitDecision(
                    self.name,
                    operation,
                    meta["current_version"],
                    None,
                    part,
                    batch_id,
                    "duplicate",
                    reason,
                    conflicting_version=rec["version"],
                )
            )
            raise DuplicateBatchError(
                f"批次 {batch_id!r} 已提交为 v{rec['version']} "
                f"(operation={rec['operation']}, partition={rec['partition']})，"
                "重复提交不产生新版本",
                existing_version=rec["version"],
            )

    def _resolve_client_base(
        self, operation: str, part: str | None, expected_version: int | None, current: int
    ) -> int:
        """解析并校验客户端乐观基版本，返回基版本（>current 的未来版本一律拒绝）。

        append 对过期基版本容忍（append 之间按并集合并，既定合并规则），
        具体冲突由调用方按分区触碰情况另行判定。
        """
        if expected_version is None:
            return current
        if expected_version > current:
            raise CommitConflictError(
                f"乐观基版本不存在: expected_version={expected_version} > "
                f"current_version={current} (operation={operation}, partition={part})"
            )
        return expected_version

    def _normalize_row_to_schema(
        self, row: dict[int, object], schema: Schema
    ) -> dict[int, object]:
        """把历史文件中的行投影到给定 schema：保留已知 id 数据，补齐新增列默认值。

        - 已删除列的数据原样保留在文件里（按 id 存储，不会错位），读取时不投影；
        - 新增列缺失时：声明默认值取默认值，否则取 NULL（稳定缺省语义）。
        """
        out = dict(row)
        for col in schema.live_columns():
            if col.column_id not in out:
                out[col.column_id] = col.default_value if col.has_default else None
        return out

    def append(
        self,
        rows: list[dict[str, object]],
        partition: str | None = None,
        batch_id: str | None = None,
        expected_version: int | None = None,
    ) -> int:
        """按分区增量追加。

        同 batch_id 重复提交抛 DuplicateBatchError（携带既有版本号），
        不产生重复数据或重复版本。append 之间语义无冲突（结果集为并集），
        因此即使 expected_version 缺省也会串行排队接受。
        """
        if not rows:
            raise SchemaValidationError("空批次拒绝提交")
        with self.storage.commit_lock(self.name):
            meta = self._meta()
            part = self._validate_partition_arg(rows, meta, partition)
            self._check_duplicate("append", part, batch_id, meta)
            base_version = meta["current_version"]
            client_base = self._resolve_client_base(
                "append", part, expected_version, base_version
            )
            schema = self._snapshot_schema(base_version)

            prepared = [schema.validate_row(r) for r in rows]
            df = self._write_row_file(prepared, schema, part)
            new_version = self._publish(
                "append", schema, [df], [], part, batch_id, base_version
            )
            merged_note = (
                f"; {base_version - client_base} commit(s) since client base, merged as union"
                if client_base < base_version
                else ""
            )
            log_decision(
                CommitDecision(
                    self.name, "append", base_version, new_version, part, batch_id,
                    "accepted", f"1 new file with {len(rows)} rows appended{merged_note}",
                )
            )
            return new_version

    def _partition_touched_since(
        self, base_version: int, current_version: int, part: str
    ) -> int | None:
        """返回 (base_version, current_version] 区间内触碰该分区的最高版本。

        触碰分区的操作：partition 匹配的数据提交（append/upsert/compaction）。
        schema_change 不触碰任何分区。压实的 partition 为 None 表示全表压实，
        视为触碰所有分区。
        """
        touched: int | None = None
        for v in range(base_version + 1, current_version + 1):
            snap = self.storage.read_snapshot(self.name, v)
            if snap.operation in ("append", "upsert", "compaction"):
                if snap.partition == part or snap.partition is None:
                    touched = v
        return touched

    def upsert(
        self,
        rows: list[dict[str, object]],
        partition: str | None = None,
        batch_id: str | None = None,
        expected_version: int | None = None,
    ) -> int:
        """按主键 upsert，采用 copy-on-write：重写受影响分区的 live 文件。

        语义（确定且不静默覆盖）：
        - 同主键的旧行被整行替换为新行；新行未给出的可空列按 NULL 落盘；
        - 主键必须在表上声明，同批内主键不得重复（否则拒绝，拒绝依据可解释）；
        - 基版本之后若该分区已被任何提交触碰，判 CommitConflictError 拒绝，
          由调用方基于新版本重试，绝不静默合并或覆盖。
        """
        if not rows:
            raise SchemaValidationError("空批次拒绝提交")
        with self.storage.commit_lock(self.name):
            meta = self._meta()
            if not meta["primary_key_ids"]:
                raise SchemaValidationError("表未声明主键，不支持 upsert")
            part = self._validate_partition_arg(rows, meta, partition)
            self._check_duplicate("upsert", part, batch_id, meta)
            base_version = meta["current_version"]
            client_base = self._resolve_client_base(
                "upsert", part, expected_version, base_version
            )
            schema = self._snapshot_schema(base_version)

            pk_ids = meta["primary_key_ids"]
            prepared: list[dict[int, object]] = []
            seen: set[tuple] = set()
            for r in rows:
                pr = schema.validate_row(r)
                key = tuple(pr[cid] for cid in pk_ids)
                if any(v is None for v in key):
                    raise SchemaValidationError("upsert 主键列不允许为 NULL")
                if key in seen:
                    raise SchemaValidationError(f"同批次内主键重复: {key}")
                seen.add(key)
                prepared.append(pr)

            # 冲突判定（持锁，依据全部是已发布快照，确定可复现）：
            # 客户端基版本之后该分区被任何提交触碰 -> 拒绝，不静默合并
            conflict_v = self._partition_touched_since(client_base, base_version, part)
            if conflict_v is not None:
                log_decision(
                    CommitDecision(
                        self.name, "upsert", base_version, None, part, batch_id,
                        "conflict",
                        f"partition touched by v{conflict_v} after client base; "
                        "copy-on-write rewrite no longer safe, rejected",
                        conflicting_version=conflict_v,
                    )
                )
                raise CommitConflictError(
                    f"upsert 冲突: 分区 {part} 在基版本之后已被 v{conflict_v} 修改，"
                    "请基于最新版本重试"
                )

            # 读当前 live 文件，按主键整行替换；未受影响文件原样保留
            incoming = {
                tuple(pr[cid] for cid in pk_ids): pr for pr in prepared
            }
            live = self._live_files(base_version, part)
            removed_names: list[str] = []
            surviving_rows: list[dict[int, object]] = []
            touched_files = 0
            for file_name, df in live.items():
                payload = self.storage.read_data_file(self.name, file_name)
                old_rows = [{int(cid): val for cid, val in r.items()} for r in payload["rows"]]
                kept = []
                file_hit = False
                for old in old_rows:
                    key = tuple(old.get(cid) for cid in pk_ids)
                    if key in incoming:
                        file_hit = True  # 该主键由新行替换
                    else:
                        kept.append(old)
                if file_hit:
                    touched_files += 1
                    removed_names.append(file_name)
                    # 存活行投影到当前 schema（补齐演进新增列的稳定默认值）
                    surviving_rows.extend(
                        self._normalize_row_to_schema(old, schema) for old in kept
                    )
                # 未命中文件不动（不在 added/removed 中）

            # 受影响文件中的存活行 + 全部新行 -> 一个重写文件
            rewritten = surviving_rows + list(incoming.values())
            new_file = self._write_row_file(rewritten, schema, part)
            new_version = self._publish(
                "upsert", schema, [new_file], removed_names, part, batch_id, base_version
            )
            log_decision(
                CommitDecision(
                    self.name, "upsert", base_version, new_version, part, batch_id,
                    "accepted",
                    f"rewrote {touched_files} live file(s), {len(incoming)} pk(s) upserted",
                )
            )
            return new_version

    def evolve_schema(
        self, change: SchemaChange, expected_version: int | None = None
    ) -> int:
        """发布一次结构演进，占用一个新版本。

        - 演进不触碰任何数据文件与分区，因此历史版本读取完全不受影响；
        - expected_version 过期时明确拒绝（演进必须串行，避免列 id 歧义）；
        - 新 schema 的完整定义写入新版本快照，旧版本快照保留旧 schema。
        """
        with self.storage.commit_lock(self.name):
            meta = self._meta()
            base_version = meta["current_version"]
            if expected_version is not None and expected_version != base_version:
                log_decision(
                    CommitDecision(
                        self.name, "schema_change", base_version, None, None, None,
                        "conflict",
                        f"expected_version={expected_version} != current={base_version}",
                        conflicting_version=base_version,
                    )
                )
                raise CommitConflictError(
                    f"结构演进冲突: expected_version={expected_version}, "
                    f"current_version={base_version}"
                )

            base_schema = self._snapshot_schema(base_version)
            # 用基 schema 重建演进器（保留下一个 column_id，保证 id 单调不复用）
            next_id = max(c.column_id for c in base_schema.columns) + 1
            evo = SchemaEvolution(current=base_schema, _next_column_id=next_id)
            try:
                new_schema = evo.apply(change)
            except Exception as exc:
                log_decision(
                    CommitDecision(
                        self.name, "schema_change", base_version, None, None, None,
                        "rejected", f"{type(exc).__name__}: {exc}",
                    )
                )
                raise
            new_version = self._publish(
                "schema_change", new_schema, [], [], None, None, base_version
            )
            log_decision(
                CommitDecision(
                    self.name, "schema_change", base_version, new_version, None, None,
                    "accepted",
                    f"{change.kind} applied; schema_id {base_schema.schema_id} -> "
                    f"{new_schema.schema_id}",
                )
            )
            return new_version

    def compact(
        self, partition: str | None = None, expected_version: int | None = None
    ) -> int:
        """分区压实：把一个分区（或全表所有分区）当前 live 文件合并为重写文件。

        不可变保证：
        - 压实产生新快照版本，旧数据文件物理保留，仅在新版本清单中 removed；
        - 对任一历史版本回放清单时，其 live 集不含压实的 removed 效果，
          因此压实不可能改变任何已发布版本的可见结果；
        - 压实要求 expected_version 等于当前版本（缺省即为当前），
          与并发提交交错时明确冲突拒绝，不静默交错。
        """
        with self.storage.commit_lock(self.name):
            meta = self._meta()
            base_version = meta["current_version"]
            if expected_version is not None and expected_version != base_version:
                log_decision(
                    CommitDecision(
                        self.name, "compaction", base_version, None, partition, None,
                        "conflict",
                        f"expected_version={expected_version} != current={base_version}",
                        conflicting_version=base_version,
                    )
                )
                raise CommitConflictError(
                    f"压实冲突: expected_version={expected_version}, "
                    f"current_version={base_version}"
                )
            schema = self._snapshot_schema(base_version)
            live = self._live_files(base_version)
            if partition is not None:
                live = {p: f for p, f in live.items() if f.partition == partition}
            if not live:
                raise CommitConflictError(
                    f"压实被拒绝: 分区 {partition} 在 v{base_version} 没有 live 文件"
                )

            # 按分区分组，每个分区产出一个合并文件；行投影到当前 schema
            by_part: dict[str, list[dict[int, object]]] = {}
            for file_name in sorted(live):
                df = live[file_name]
                payload = self.storage.read_data_file(self.name, file_name)
                rows = [{int(cid): val for cid, val in r.items()} for r in payload["rows"]]
                by_part.setdefault(df.partition, []).extend(
                    self._normalize_row_to_schema(r, schema) for r in rows
                )

            added: list[DataFile] = []
            for part in sorted(by_part):
                added.append(self._write_row_file(by_part[part], schema, part))
            removed = sorted(os.path.basename(f.path) for f in live.values())

            new_version = self._publish(
                "compaction",
                schema,
                added,
                removed,
                partition,
                None,
                base_version,
                compaction_of_version=base_version,
            )
            log_decision(
                CommitDecision(
                    self.name, "compaction", base_version, new_version, partition, None,
                    "accepted",
                    f"compacted {len(removed)} file(s) into {len(added)} file(s) "
                    f"across {len(by_part)} partition(s); old files retained on disk",
                )
            )
            return new_version

    def read(
        self,
        version: int | None = None,
        partition: str | None = None,
        columns: list[str] | None = None,
    ) -> list[dict[str, object]]:
        """快照读取。

        - version=None 读取最新已发布版本；否则读取指定版本，只能看到
          截至该版本已提交的数据（回放该版本快照的 live 文件集）；
        - 返回行按该版本发布时刻的 schema 解释：新增列对历史行按其
          不可变默认值（或 NULL）补值；改名列以新名返回；已删列不返回；
        - columns 显式投影时，若包含该版本不存在/已删列，抛
          ColumnNotFoundError —— 绝不静默返回错误结果或换列；
        - partition 按物理分区值过滤；返回顺序按 (文件创建顺序, 文件内行序)
          稳定排列，保证同一版本重复读取可复现。
        """
        meta = self._meta()
        if version is None:
            version = meta["current_version"]
        if version < 0 or version > meta["current_version"]:
            raise VersionNotFoundError(
                f"版本不存在或尚未发布: v{version} (latest=v{meta['current_version']})"
            )

        schema = self._snapshot_schema(version)
        if columns is not None:
            projected = [schema.column_by_name(name) for name in columns]
        else:
            projected = list(schema.live_columns())

        part_filter = partition
        live = self._live_files(version)
        rows: list[dict[str, object]] = []
        # 按文件名排序，保证多文件合并顺序稳定可复现
        for file_name in sorted(live):
            df = live[file_name]
            if part_filter is not None and df.partition != part_filter:
                continue
            payload = self.storage.read_data_file(self.name, file_name)
            for raw in payload["rows"]:
                id_row = {int(cid): val for cid, val in raw.items()}
                projected_row = {}
                for col in projected:
                    if col.column_id in id_row:
                        projected_row[col.name] = id_row[col.column_id]
                    else:
                        # 历史文件早于该列加入：稳定缺省语义
                        projected_row[col.name] = (
                            col.default_value if col.has_default else None
                        )
                rows.append(projected_row)
        return rows

    def history(self) -> list[Snapshot]:
        """返回全部已发布快照（按版本升序，不含 v0 初始空快照）。"""
        versions = self.storage.list_snapshot_versions(self.name)
        return [
            self.storage.read_snapshot(self.name, v)
            for v in versions
            if v > 0
        ]
