"""结构演进测试。

覆盖：
- 新增列的稳定缺省语义：声明默认值 -> 历史行取默认值；否则 NULL；
- 改名：column_id 不变，历史版本仍按旧名读取，新版本按新名读取，不错位；
- 删列：逻辑删除，历史快照不受影响，当前查询显式引用已删列明确报错；
- upsert/压实重写文件后历史行按新 schema 补列，不与旧列错位；
- 演进串行：过期基版本的演进被拒绝，列 id 单调不复用。
"""

from __future__ import annotations

import pytest

from lakehouse.catalog import Catalog
from lakehouse.errors import (
    ColumnNotFoundError,
    CommitConflictError,
    SchemaValidationError,
)
from lakehouse.schema import SchemaChange

COLUMNS = [("id", "long"), ("p", "string"), ("v", "int")]


@pytest.fixture
def table(tmp_path):
    catalog = Catalog(str(tmp_path))
    catalog.create_table("events", COLUMNS, primary_key=("id",), partition_column="p")
    t = catalog.open_table("events")
    t.append([{"id": 1, "p": "a", "v": 1}, {"id": 2, "p": "a", "v": 2}], batch_id="init")
    return t


def test_add_column_default_applies_to_historical_rows(table):
    # 无默认值的新增列：历史行为 NULL
    v1 = table.evolve_schema(SchemaChange.add_column("tag", "string"))
    assert v1 == 2
    rows = sorted(table.read(), key=lambda r: r["id"])
    assert rows[0]["tag"] is None

    # 带常量默认值的新增列：所有历史行确定性取默认值
    table.evolve_schema(
        SchemaChange.add_column("region", "string", has_default=True, default_value="cn")
    )
    rows = sorted(table.read(), key=lambda r: r["id"])
    assert {r["region"] for r in rows} == {"cn"}
    # 新数据可以覆盖默认值
    table.append([{"id": 3, "p": "b", "v": 3, "tag": "hot", "region": "us"}], batch_id="a3")
    r3 = [r for r in table.read() if r["id"] == 3][0]
    assert r3["tag"] == "hot" and r3["region"] == "us"


def test_added_column_does_not_change_published_versions(table):
    snap_v1_rows = table.read(version=1)
    table.evolve_schema(SchemaChange.add_column("region", "string", has_default=True, default_value="cn"))
    # 已发布版本 v1 的结果必须与演进前逐字节一致
    assert table.read(version=1) == snap_v1_rows
    assert "region" not in table.read(version=1)[0]


def test_nonnullable_add_without_default_is_rejected(table):
    with pytest.raises(SchemaValidationError):
        table.evolve_schema(SchemaChange.add_column("x", "int", nullable=False))


def test_rename_keeps_column_id_and_history_aligned(table):
    # v=2 演进改名；列 id 仍为 2
    table.evolve_schema(SchemaChange.rename_column("v", "score"))
    assert table.storage.read_snapshot(table.name, 2).schema.column_by_name("score").column_id == 2

    # 新版本按新名读到旧数据（没有错位，没有丢失）
    assert sorted(r["score"] for r in table.read()) == [1, 2]
    # 历史版本仍按旧名
    assert sorted(r["v"] for r in table.read(version=1)) == [1, 2]
    # 当前版本显式使用旧名 -> 明确报错，绝不静默映射到别的列
    with pytest.raises(ColumnNotFoundError):
        table.read(columns=["v"])
    # 改名后写入必须用新名；用旧名写入被拒绝而不是错位成新列
    with pytest.raises(SchemaValidationError):
        table.append([{"id": 3, "p": "a", "v": 3}], batch_id="old-name")
    table.append([{"id": 3, "p": "a", "score": 3}], batch_id="new-name")
    assert [r["score"] for r in table.read() if r["id"] == 3] == [3]


def test_drop_column_is_logical_and_remains_in_history(table):
    table.evolve_schema(SchemaChange.drop_column("v"))
    # 当前结果不再包含该列
    assert all("v" not in r for r in table.read())
    # 历史版本结果不变
    assert sorted(r["v"] for r in table.read(version=1)) == [1, 2]
    # 显式查询已删列 -> 明确报错，不静默返回 NULL 或错位
    with pytest.raises(ColumnNotFoundError):
        table.read(columns=["v"])
    # column id 不复用：之后新增列拿到新 id（2 已被 v 占用）
    table.evolve_schema(SchemaChange.add_column("v2", "int"))
    snap = table.history()[-1]
    assert snap.schema.column_by_name("v2").column_id == 3
    assert snap.schema.column_by_id(2).dropped is True


def test_upsert_after_add_column_normalizes_surviving_rows(table):
    table.evolve_schema(
        SchemaChange.add_column("region", "string", has_default=True, default_value="cn")
    )
    # upsert 重写 live 文件后，未更新的存活行必须带上新列默认值
    table.upsert([{"id": 1, "p": "a", "v": 10, "region": "us"}], batch_id="u1")
    rows = {r["id"]: r for r in table.read()}
    assert rows[1]["region"] == "us" and rows[1]["v"] == 10
    assert rows[2]["region"] == "cn" and rows[2]["v"] == 2
    # 旧版本依旧看不到新列
    assert "region" not in table.read(version=1)[0]


def test_concurrent_schema_evolution_is_serialized(table):
    # 两者都以 v1 为基：只允许一个成功
    v_a = table.evolve_schema(SchemaChange.add_column("a_col", "int"), expected_version=1)
    assert v_a == 2
    with pytest.raises(CommitConflictError):
        table.evolve_schema(SchemaChange.add_column("b_col", "int"), expected_version=1)
    # 基于新版本再演进成功，两列的 id 不同且都可见
    v_c = table.evolve_schema(SchemaChange.add_column("b_col", "int"))
    assert v_c == 3
    names = {c.name for c in table._snapshot_schema(v_c).live_columns()}
    assert {"a_col", "b_col"} <= names


def test_duplicate_and_conflicting_rename_are_rejected(table):
    table.evolve_schema(SchemaChange.add_column("extra", "int"))
    with pytest.raises(SchemaValidationError):
        table.evolve_schema(SchemaChange.add_column("extra", "int"))  # 重名新增
    with pytest.raises(SchemaValidationError):
        table.evolve_schema(SchemaChange.rename_column("id", "p"))  # 目标名冲突


def test_add_column_default_must_match_dtype(table):
    with pytest.raises(SchemaValidationError):
        table.evolve_schema(
            SchemaChange.add_column("bad", "int", has_default=True, default_value="x")
        )
