import logging

import pytest

from lakehouse.models import Column, Schema
from lakehouse.storage import LocalStore
from lakehouse.table import TableService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


@pytest.fixture()
def service(tmp_path):
    return TableService(LocalStore(tmp_path / "store"))


@pytest.fixture()
def kv_table(service):
    """主键表：id(int, pk), val(str)。"""
    return service.create_table("kv", Schema(
        columns=(Column(1, "id", "int"), Column(2, "val", "str")),
        primary_keys=("id",),
    ))
