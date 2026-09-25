"""表目录：按名称创建/打开/列出本地湖仓表。"""

from __future__ import annotations

import os

from .errors import TableExistsError, TableNotFoundError
from .schema import SchemaEvolution
from .storage import FileStorage
from .table import LakehouseTable


class Catalog:
    """本地目录型 Catalog：一个 root 目录下每张表一个子目录。"""

    def __init__(self, root_dir: str) -> None:
        self.root_dir = root_dir
        self.storage = FileStorage(root_dir)
        os.makedirs(root_dir, exist_ok=True)

    def create_table(
        self,
        name: str,
        columns: list[tuple[str, str]],
        primary_key: tuple[str, ...] = (),
        partition_column: str | None = None,
    ) -> LakehouseTable:
        if self.storage.table_exists(name):
            raise TableExistsError(f"表已存在: {name}")
        schema = SchemaEvolution.begin(columns).current
        return LakehouseTable.create(
            self.storage,
            name,
            schema,
            primary_key_columns=tuple(primary_key),
            partition_column=partition_column,
        )

    def open_table(self, name: str) -> LakehouseTable:
        if not self.storage.table_exists(name):
            raise TableNotFoundError(f"表不存在: {name}")
        return LakehouseTable(self.storage, name)

    def list_tables(self) -> list[str]:
        if not os.path.isdir(self.root_dir):
            return []
        return sorted(
            name
            for name in os.listdir(self.root_dir)
            if self.storage.table_exists(name)
        )
