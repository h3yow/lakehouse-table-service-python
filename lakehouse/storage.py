"""本地文件存储层：表元数据、快照清单与数据文件的持久化。

目录布局（每张表一个目录）：
    <root>/<table>/metadata/_table.json     表定义（当前指针、批次索引等）
    <root>/<table>/metadata/lock            提交互斥锁（flock）
    <root>/<table>/metadata/v-<n>.json      每次提交产生的快照清单（append-only）
    <root>/<table>/data/<uuid>.json         数据文件，行按 column_id 存储

关键持久化约定：
- 元数据/快照写入采用 "同目录临时文件 + os.replace" 原子替换，崩溃不会出现半文件；
- 快照文件一旦写入永不修改、永不删除（旧版本读取依赖它们）；
- 压实只会让旧数据文件从新版本的 live 集合中移除，物理文件保留，
  因此任何已发布版本的可见结果都不会被压实改变。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
import uuid
from dataclasses import dataclass, field

from .errors import TableNotFoundError, VersionNotFoundError
from .schema import Column, Schema


@dataclass(frozen=True)
class DataFile:
    """一个不可变数据文件的清单记录。"""

    path: str
    partition: str
    schema_id: int
    row_count: int
    # 该文件内包含的主键值（用于 upsert 定位与提交冲突判定）
    primary_key_values: tuple[object, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Snapshot:
    """一次提交的不可变快照清单。"""

    version: int
    schema_id: int
    parent_version: int | None
    operation: str  # append / upsert / compaction / schema_change
    partition: str | None
    added_files: tuple[DataFile, ...] = field(default_factory=tuple)
    removed_files: tuple[str, ...] = field(default_factory=tuple)
    batch_id: str | None = None
    compaction_of_version: int | None = None
    schema: Schema | None = None  # 快照发布时刻的完整表结构


# ---------------------------------------------------------------------------
# 序列化
# ---------------------------------------------------------------------------

def column_to_dict(c: Column) -> dict:
    return {
        "column_id": c.column_id,
        "name": c.name,
        "dtype": c.dtype,
        "nullable": c.nullable,
        "has_default": c.has_default,
        "default_value": c.default_value,
        "dropped": c.dropped,
    }


def column_from_dict(d: dict) -> Column:
    return Column(
        column_id=d["column_id"],
        name=d["name"],
        dtype=d["dtype"],
        nullable=d["nullable"],
        has_default=d["has_default"],
        default_value=d.get("default_value"),
        dropped=d["dropped"],
    )


def schema_to_dict(s: Schema) -> dict:
    return {
        "schema_id": s.schema_id,
        "columns": [column_to_dict(c) for c in s.columns],
    }


def schema_from_dict(d: dict) -> Schema:
    return Schema(
        schema_id=d["schema_id"],
        columns=tuple(column_from_dict(c) for c in d["columns"]),
    )


def data_file_to_dict(f: DataFile) -> dict:
    return {
        "path": f.path,
        "partition": f.partition,
        "schema_id": f.schema_id,
        "row_count": f.row_count,
        "primary_key_values": list(f.primary_key_values),
    }


def data_file_from_dict(d: dict) -> DataFile:
    return DataFile(
        path=d["path"],
        partition=d["partition"],
        schema_id=d["schema_id"],
        row_count=d["row_count"],
        primary_key_values=tuple(d.get("primary_key_values", [])),
    )


def snapshot_to_dict(s: Snapshot) -> dict:
    out = {
        "version": s.version,
        "schema_id": s.schema_id,
        "parent_version": s.parent_version,
        "operation": s.operation,
        "partition": s.partition,
        "added_files": [data_file_to_dict(f) for f in s.added_files],
        "removed_files": list(s.removed_files),
        "batch_id": s.batch_id,
        "compaction_of_version": s.compaction_of_version,
    }
    if s.schema is not None:
        out["schema"] = schema_to_dict(s.schema)
    return out


def snapshot_from_dict(d: dict) -> Snapshot:
    return Snapshot(
        version=d["version"],
        schema_id=d["schema_id"],
        parent_version=d.get("parent_version"),
        operation=d["operation"],
        partition=d.get("partition"),
        added_files=tuple(data_file_from_dict(f) for f in d.get("added_files", [])),
        removed_files=tuple(d.get("removed_files", [])),
        batch_id=d.get("batch_id"),
        compaction_of_version=d.get("compaction_of_version"),
        schema=schema_from_dict(d["schema"]) if "schema" in d else None,
    )


# ---------------------------------------------------------------------------
# 存储实现
# ---------------------------------------------------------------------------

class FileStorage:
    """基于本地目录的存储实现。"""

    def __init__(self, root_dir: str) -> None:
        self.root_dir = root_dir

    # ---- 路径 ----
    def table_dir(self, table_name: str) -> str:
        return os.path.join(self.root_dir, table_name)

    def metadata_dir(self, table_name: str) -> str:
        return os.path.join(self.table_dir(table_name), "metadata")

    def data_dir(self, table_name: str) -> str:
        return os.path.join(self.table_dir(table_name), "data")

    def metadata_path(self, table_name: str) -> str:
        return os.path.join(self.metadata_dir(table_name), "_table.json")

    def snapshot_path(self, table_name: str, version: int) -> str:
        return os.path.join(self.metadata_dir(table_name), f"v-{version}.json")

    def lock_path(self, table_name: str) -> str:
        return os.path.join(self.metadata_dir(table_name), "lock")

    # ---- 表管理 ----
    def table_exists(self, table_name: str) -> bool:
        return os.path.isfile(self.metadata_path(table_name))

    def create_table_dir(self, table_name: str) -> str:
        d = self.table_dir(table_name)
        os.makedirs(self.metadata_dir(table_name))
        os.makedirs(self.data_dir(table_name))
        # 预先创建锁文件，避免加锁时的创建竞态
        open(self.lock_path(table_name), "a").close()
        return d

    @contextlib.contextmanager
    def commit_lock(self, table_name: str):
        """提交串行化锁：同一表上的所有提交互斥。

        锁在 read_metadata 之前获取，在 快照+元数据 落盘之后释放，
        保证 "读当前版本 -> 判定 -> 发布新版本" 整个临界区原子。
        """
        if not self.table_exists(table_name):
            raise TableNotFoundError(f"表不存在: {table_name}")
        with open(self.lock_path(table_name), "r+") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    # ---- 元数据 ----
    def read_metadata(self, table_name: str) -> dict:
        path = self.metadata_path(table_name)
        if not os.path.isfile(path):
            raise TableNotFoundError(f"表不存在: {table_name}")
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def write_metadata(self, table_name: str, metadata: dict) -> None:
        _atomic_write_json(self.metadata_path(table_name), metadata)

    # ---- 快照清单 ----
    def write_snapshot(self, table_name: str, snapshot: Snapshot) -> None:
        path = self.snapshot_path(table_name, snapshot.version)
        if os.path.exists(path):  # 防御：版本号绝不复用
            raise RuntimeError(f"快照文件已存在，版本号不得复用: {path}")
        _atomic_write_json(path, snapshot_to_dict(snapshot))

    def read_snapshot(self, table_name: str, version: int) -> Snapshot:
        path = self.snapshot_path(table_name, version)
        if not os.path.isfile(path):
            raise VersionNotFoundError(f"版本不存在: {table_name}@v{version}")
        with open(path, "r", encoding="utf-8") as fh:
            return snapshot_from_dict(json.load(fh))

    def list_snapshot_versions(self, table_name: str) -> list[int]:
        mdir = self.metadata_dir(table_name)
        if not os.path.isdir(mdir):
            return []
        versions = []
        for name in os.listdir(mdir):
            if name.startswith("v-") and name.endswith(".json"):
                versions.append(int(name[2:-5]))
        return sorted(versions)

    # ---- 数据文件 ----
    def write_data_file(self, table_name: str, payload: dict) -> str:
        """写入一个不可变数据文件，返回相对 data 目录的文件名。"""
        file_id = uuid.uuid4().hex
        path = os.path.join(self.data_dir(table_name), f"{file_id}.json")
        _atomic_write_json(path, payload)
        return f"{file_id}.json"

    def read_data_file(self, table_name: str, file_name: str) -> dict:
        path = os.path.join(self.data_dir(table_name), file_name)
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def delete_data_file(self, table_name: str, file_name: str) -> None:
        """物理删除被刻意禁用：历史快照仍可能引用该文件。

        压实只会在清单层面移除文件（removed_files），物理文件保留，
        以保证任意已发布版本的时间旅行读取。如需回收空间，必须在
        实现独立的、带快照保留策略的 GC 后另行开放。
        """
        raise PermissionError(
            "数据文件为历史版本可见性保留，禁止物理删除；"
            "请通过快照清单的 removed_files 表达逻辑移除"
        )


def _atomic_write_json(path: str, obj: dict) -> None:
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        raise
