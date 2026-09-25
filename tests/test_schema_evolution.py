"""结构演进：新增列缺省语义稳定，删列/改名不使历史数据错位。"""

import logging

import pytest

from lakehouse.errors import SchemaEvolutionError
from lakehouse.models import WriteBatch

log = logging.getLogger("test.schema")


def test_add_column_default_semantics(kv_table):
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    v2 = kv_table.add_column("score", "float", 9.5)
    new_rows = kv_table.read()
    old_rows = kv_table.read(version=v1)
    log.info(
        "判定依据: v%d 新增列 score 缺省 9.5; 最新读取=%s 应含缺省值, 历史 v%d=%s 不应出现新列",
        v2, new_rows, v1, old_rows,
    )
    assert new_rows == [{"id": 1, "val": "a", "score": 9.5}]
    assert old_rows == [{"id": 1, "val": "a"}]


def test_add_column_then_write_explicit_value(kv_table):
    kv_table.add_column("score", "int", 0)
    kv_table.commit(WriteBatch("b1", 1, "p=1", ({"id": 1, "val": "a", "score": 7},), "upsert"))
    kv_table.commit(WriteBatch("b2", 2, "p=1", ({"id": 2, "val": "b"},), "upsert"))
    rows = kv_table.read()
    log.info("判定依据: 显式值 7 与缺省值 0 并存: %s", rows)
    assert rows == [
        {"id": 1, "val": "a", "score": 7},
        {"id": 2, "val": "b", "score": 0},
    ]


def test_rename_column_keeps_history(kv_table):
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    kv_table.rename_column("val", "value")
    new_rows = kv_table.read()
    old_rows = kv_table.read(version=v1)
    log.info("判定依据: 改名后新读取键为 value=%s, 历史 v%d 仍为 val=%s", new_rows, v1, old_rows)
    assert new_rows == [{"id": 1, "value": "a"}]
    assert old_rows == [{"id": 1, "val": "a"}]
    # 改名后写入新名，历史数据不错位
    kv_table.commit(WriteBatch("b2", kv_table.current_version(), "p=1",
                               ({"id": 1, "value": "A"},), "upsert"))
    assert kv_table.read() == [{"id": 1, "value": "A"}]


def test_drop_column_no_misalignment(kv_table):
    kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    kv_table.add_column("score", "int", 3)
    kv_table.commit(WriteBatch("b2", 2, "p=1", ({"id": 2, "val": "b", "score": 8},), "upsert"))
    kv_table.drop_column("score")
    rows = kv_table.read()
    log.info("判定依据: 删列后读取=%s, val 不得错位到 score 的值", rows)
    assert rows == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    # 删列前的版本仍能看到 score
    assert kv_table.read(version=3)[1]["score"] == 8


def test_drop_primary_key_rejected(kv_table):
    with pytest.raises(SchemaEvolutionError) as e:
        kv_table.drop_column("id")
    log.info("判定依据: 主键列不可删除 -> SchemaEvolutionError: %s", e.value)


def test_rename_to_existing_rejected(kv_table):
    with pytest.raises(SchemaEvolutionError):
        kv_table.rename_column("val", "id")


def test_write_with_old_name_after_rename_rejected(kv_table):
    kv_table.rename_column("val", "value")
    from lakehouse.errors import ValidationError
    with pytest.raises(ValidationError) as e:
        kv_table.commit(WriteBatch("b1", 1, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    log.info("判定依据: 改名后用旧名写入 -> ValidationError（不静默吞掉）: %s", e.value)
