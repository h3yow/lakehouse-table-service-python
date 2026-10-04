"""表格式核心类型定义。

设计要点：
- 列以稳定递增的 ``id`` 标识，``name`` 仅是对外展示名。改名只改 name，
  删列只从 schema 移除条目，历史数据文件中的列 id 不变，因此不会错位。
- 数据文件中的行以 ``{str(column_id): value}`` 存储，与列名解耦。
- 每个版本（Version）记录当时的 schema，快照读取按该版本的 schema 投影，
  新增列对历史数据以声明的缺省值补齐。
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any

# 支持的列类型（写入时做校验）
SUPPORTED_TYPES = ("int", "float", "str", "bool")


@dataclass(frozen=True)
class Column:
    id: int
    name: str
    type: str
    default: Any = None

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "Column":
        return Column(id=d["id"], name=d["name"], type=d["type"], default=d.get("default"))


@dataclass(frozen=True)
class Schema:
    """某一版本的表结构。primary_keys 为主键列名列表。"""

    columns: tuple[Column, ...]
    primary_keys: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "columns": [c.to_dict() for c in self.columns],
            "primary_keys": list(self.primary_keys),
        }

    @staticmethod
    def from_dict(d: dict) -> "Schema":
        return Schema(
            columns=tuple(Column.from_dict(c) for c in d["columns"]),
            primary_keys=tuple(d.get("primary_keys", ())),
        )

    def column_by_name(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def column_by_id(self, col_id: int) -> Column | None:
        for c in self.columns:
            if c.id == col_id:
                return c
        return None


@dataclass(frozen=True)
class DataFile:
    """一个数据文件：属于某分区，包含若干行。"""

    path: str  # 相对表根目录的路径
    partition: str
    row_count: int

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "DataFile":
        return DataFile(path=d["path"], partition=d["partition"], row_count=d["row_count"])


@dataclass
class Version:
    """一次提交产生的版本（快照元数据）。

    - ``batch_id``：产生该版本的批次 id（压实等内部操作使用保留前缀）。
    - ``schema``：该版本生效的表结构。
    - ``files``：该版本可见的全部数据文件清单。
    - ``committed_batches``：截至该版本已提交的全部业务批次 id（幂等判定依据）。
    - ``operation``：append / upsert / delete / compact / evolve。
    """

    version: int
    batch_id: str
    operation: str
    schema: Schema
    files: tuple[DataFile, ...]
    committed_batches: tuple[str, ...] = ()
    parent_version: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "batch_id": self.batch_id,
            "operation": self.operation,
            "schema": self.schema.to_dict(),
            "files": [f.to_dict() for f in self.files],
            "committed_batches": list(self.committed_batches),
            "parent_version": self.parent_version,
            "timestamp": self.timestamp,
        }

    @staticmethod
    def from_dict(d: dict) -> "Version":
        return Version(
            version=d["version"],
            batch_id=d["batch_id"],
            operation=d["operation"],
            schema=Schema.from_dict(d["schema"]),
            files=tuple(DataFile.from_dict(f) for f in d["files"]),
            committed_batches=tuple(d.get("committed_batches", ())),
            parent_version=d.get("parent_version", 0),
            timestamp=d.get("timestamp", 0.0),
        )


@dataclass(frozen=True)
class WriteBatch:
    """一次写入批次。

    - ``batch_id``：调用方提供的幂等键；同一 batch_id 重复提交相同内容不产生
      重复数据或新版本。
    - ``base_version``：调用方读取数据时基于的版本，用于乐观并发控制。
    - ``rows``：以列名为键的行数据（写入时按当前 schema 转换为列 id 存储）。
      ``mode="delete"`` 时 rows 只含主键列，表示要删除的主键集合。
    """

    batch_id: str
    base_version: int
    partition: str
    rows: tuple[dict[str, Any], ...]
    mode: str = "append"  # append | upsert | delete
