"""本地文件系统存储层。

目录布局（root 为存储根目录）::

    <root>/<table>/
        metadata/
            table.json            # 表级元数据（当前版本号、下一列 id 等）
            versions/v<N>.json    # 每个版本的快照元数据
        data/
            <partition>/<file>.json   # 数据文件，行以列 id 为键

所有写文件操作先写临时文件再原子 rename，保证读取方不会看到半截文件。
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from lakehouse.errors import TableAlreadyExistsError, TableNotFoundError, VersionNotFoundError
from lakehouse.models import Version

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.\-=,]+$")


def _check_name(kind: str, name: str) -> None:
    if not name or not _SAFE_NAME.match(name) or ".." in name:
        raise ValueError(f"非法{kind}: {name!r}")


class LocalStore:
    """表数据与元数据的本地存储。"""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # ---- 路径 ----
    def table_dir(self, table: str) -> Path:
        _check_name("表名", table)
        return self.root / table

    def _versions_dir(self, table: str) -> Path:
        return self.table_dir(table) / "metadata" / "versions"

    # ---- 表级 ----
    def create_table_dir(self, table: str, meta: dict) -> None:
        tdir = self.table_dir(table)
        if tdir.exists():
            raise TableAlreadyExistsError(f"表已存在: {table}")
        (tdir / "metadata" / "versions").mkdir(parents=True)
        (tdir / "data").mkdir()
        self._atomic_write_json(tdir / "metadata" / "table.json", meta)

    def table_exists(self, table: str) -> bool:
        return self.table_dir(table).is_dir()

    def load_table_meta(self, table: str) -> dict:
        path = self.table_dir(table) / "metadata" / "table.json"
        if not path.is_file():
            raise TableNotFoundError(f"表不存在: {table}")
        return json.loads(path.read_text(encoding="utf-8"))

    def save_table_meta(self, table: str, meta: dict) -> None:
        self._atomic_write_json(self.table_dir(table) / "metadata" / "table.json", meta)

    def list_tables(self) -> list[str]:
        return sorted(p.name for p in self.root.iterdir() if (p / "metadata" / "table.json").is_file())

    # ---- 版本元数据 ----
    def save_version(self, table: str, version: Version) -> None:
        path = self._versions_dir(table) / f"v{version.version}.json"
        if path.exists():
            raise VersionNotFoundError(f"版本文件已存在，拒绝覆盖: v{version.version}")
        self._atomic_write_json(path, version.to_dict())

    def load_version(self, table: str, version: int) -> Version:
        path = self._versions_dir(table) / f"v{version}.json"
        if not path.is_file():
            raise VersionNotFoundError(f"表 {table} 不存在版本 v{version}")
        return Version.from_dict(json.loads(path.read_text(encoding="utf-8")))

    # ---- 数据文件 ----
    def write_data_file(self, table: str, partition: str, filename: str, rows: list[dict[str, Any]]) -> str:
        """写入数据文件，返回相对表根目录的路径。"""
        _check_name("分区", partition)
        _check_name("文件名", filename)
        rel = f"data/{partition}/{filename}"
        self._atomic_write_json(self.table_dir(table) / rel, rows)
        return rel

    def read_data_file(self, table: str, rel_path: str) -> list[dict[str, Any]]:
        path = self.table_dir(table) / rel_path
        if not path.is_file():
            raise FileNotFoundError(f"数据文件不存在: {rel_path}")
        return json.loads(path.read_text(encoding="utf-8"))

    # ---- 工具 ----
    @staticmethod
    def _atomic_write_json(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)
