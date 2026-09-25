"""提交语义：幂等、冲突、upsert/append 规则。"""

import logging

import pytest

from lakehouse.errors import CommitConflictError, DuplicateBatchError, ValidationError
from lakehouse.models import WriteBatch

log = logging.getLogger("test.commit")


def test_idempotent_replay_same_content(kv_table):
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    v2 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    log.info("判定依据: 相同 batch_id+相同内容 -> 幂等，v1=%d v2=%d 应相等", v1, v2)
    assert v1 == v2 == 1
    assert kv_table.current_version() == 1, "重复提交不得产生新版本"
    assert kv_table.read() == [{"id": 1, "val": "a"}], "重复提交不得产生重复数据"


def test_duplicate_batch_different_content_rejected(kv_table):
    kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    with pytest.raises(DuplicateBatchError) as e:
        kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "DIFFERENT"},), "upsert"))
    log.info("判定依据: 相同 batch_id 不同内容 -> DuplicateBatchError: %s", e.value)


def test_upsert_overwrites_by_primary_key(kv_table):
    kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}), "upsert"))
    kv_table.commit(WriteBatch("b2", 1, "p=1", ({"id": 2, "val": "B"},), "upsert"))
    rows = kv_table.read()
    log.info("判定依据: 版本序即覆盖序, id=2 应取 v2 的值 B: %s", rows)
    assert rows == [{"id": 1, "val": "a"}, {"id": 2, "val": "B"}]


def test_conflicting_same_partition_rejected(kv_table):
    kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    # 另一个基于 v0 的提交写入同一分区 -> 必须拒绝
    with pytest.raises(CommitConflictError) as e:
        kv_table.commit(WriteBatch("b2", 0, "p=1", ({"id": 2, "val": "b"},), "upsert"))
    log.info("判定依据: base=v0 与 v1 触及同一分区 p=1 -> CommitConflictError: %s", e.value)
    assert kv_table.read() == [{"id": 1, "val": "a"}], "冲突提交不得静默覆盖"


def test_disjoint_partitions_merge(kv_table):
    kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    # 基于旧版本但分区不相交 -> 按既定规则 rebase 合并
    v = kv_table.commit(WriteBatch("b2", 0, "p=2", ({"id": 2, "val": "b"},), "upsert"))
    rows_p1 = kv_table.read(partition="p=1")
    rows_p2 = kv_table.read(partition="p=2")
    log.info("判定依据: 分区不相交 -> 合并成功 v%d, p=1:%s p=2:%s", v, rows_p1, rows_p2)
    assert v == 2
    assert rows_p1 == [{"id": 1, "val": "a"}]
    assert rows_p2 == [{"id": 2, "val": "b"}]


def test_append_rejects_existing_primary_key(kv_table):
    kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "append"))
    with pytest.raises(ValidationError) as e:
        kv_table.commit(WriteBatch("b2", 1, "p=1", ({"id": 1, "val": "x"},), "append"))
    log.info("判定依据: append 遇既有主键 -> ValidationError: %s", e.value)


def test_validation_unknown_column(kv_table):
    with pytest.raises(ValidationError) as e:
        kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "nope": 1},), "upsert"))
    log.info("判定依据: 未知列 -> ValidationError: %s", e.value)
