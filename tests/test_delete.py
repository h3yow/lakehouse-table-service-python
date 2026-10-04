"""行级删除：按主键删除的可见性、幂等、并发冲突、与压实/演进的兼容。"""

import logging
import threading

import pytest

from lakehouse.errors import CommitConflictError, DuplicateBatchError, LakehouseError, ValidationError
from lakehouse.models import Column, Schema, WriteBatch

log = logging.getLogger("test.delete")


def _write(table, batch_id, base, partition, rows, mode="upsert"):
    return table.commit(WriteBatch(batch_id, base, partition, rows, mode))


def _delete(table, batch_id, base, partition, keys):
    return table.commit(WriteBatch(batch_id, base, partition, keys, "delete"))


def test_delete_hides_row_in_latest_but_history_kept(kv_table):
    """删除后最新快照不再出现；删除前的版本仍读到当时的样子。"""
    v1 = _write(kv_table, "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}))
    v2 = _delete(kv_table, "d1", v1, "p=1", ({"id": 1},))
    latest = kv_table.read()
    history = kv_table.read(version=v1)
    log.info(
        "判定依据: v%d 删除 id=1 -> 最新 v%d 应只剩 id=2: %s; 历史 v%d 不变: %s",
        v1, v2, latest, v1, history,
    )
    assert latest == [{"id": 2, "val": "b"}]
    assert history == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    # 删除版本自己的元数据也可读
    assert kv_table.read(version=v2) == [{"id": 2, "val": "b"}]


def test_deleted_pk_can_be_written_back(kv_table):
    """删除过的主键再写回来，要当成一条正常的行重新出现。"""
    _write(kv_table, "b1", 0, "p=1", ({"id": 1, "val": "a"},))
    _delete(kv_table, "d1", 1, "p=1", ({"id": 1},))
    assert kv_table.read() == []
    v3 = _write(kv_table, "b2", 2, "p=1", ({"id": 1, "val": "comeback"},))
    rows = kv_table.read()
    log.info("判定依据: 删除后同主键 upsert 写回 -> v%d 重新出现: %s", v3, rows)
    assert rows == [{"id": 1, "val": "comeback"}]
    # append 也应允许（该主键在最新版本已不存在）
    _delete(kv_table, "d2", v3, "p=1", ({"id": 1},))
    v5 = _write(kv_table, "b3", 4, "p=1", ({"id": 1, "val": "via-append"},), "append")
    log.info("判定依据: 删除后同主键 append 写回 -> v%d: %s", v5, kv_table.read())
    assert kv_table.read() == [{"id": 1, "val": "via-append"}]


def test_delete_idempotent_replay(kv_table):
    """重复提交同一批删除（内容一致）：返回原版本，不产生新版本或痕迹。"""
    _write(kv_table, "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}))
    v1 = _delete(kv_table, "d1", 1, "p=1", ({"id": 1},))
    v2 = _delete(kv_table, "d1", 1, "p=1", ({"id": 1},))
    log.info("判定依据: 相同 batch_id+相同删除内容 -> 幂等 v1=%d v2=%d 相等", v1, v2)
    assert v1 == v2 == 2
    assert kv_table.current_version() == 2, "重复删除不得产生新版本"
    assert kv_table.read() == [{"id": 2, "val": "b"}]
    # 相同 batch_id 但删除内容不同 -> 可区分的失败
    with pytest.raises(DuplicateBatchError) as e:
        _delete(kv_table, "d1", 1, "p=1", ({"id": 2},))
    log.info("判定依据: 相同 batch_id 不同删除内容 -> DuplicateBatchError: %s", e.value)
    assert kv_table.current_version() == 2


def test_delete_nonexistent_keys_deterministic(kv_table):
    """删除本来就不存在的主键：确定成功，数据不被改坏。"""
    _write(kv_table, "b1", 0, "p=1", ({"id": 1, "val": "a"},))
    v = _delete(kv_table, "d1", 1, "p=1", ({"id": 999}, {"id": 998}))
    rows = kv_table.read()
    log.info("判定依据: 删除不存在的主键 -> v%d 成功且数据不变: %s", v, rows)
    assert rows == [{"id": 1, "val": "a"}]
    # 对空分区删除也是确定的无操作
    v2 = _delete(kv_table, "d2", v, "p=empty", ({"id": 1},))
    log.info("判定依据: 空分区删除 -> v%d 无新文件, 数据不变", v2)
    assert kv_table.read() == [{"id": 1, "val": "a"}]
    assert kv_table.read(partition="p=empty") == []


def test_delete_rejected_without_primary_key(service):
    """不支持主键的表上做删除：明确拒绝，不给含糊结果。"""
    t = service.create_table("nopk", Schema(columns=(Column(1, "v", "str"),)))
    with pytest.raises(ValidationError) as e:
        _delete(t, "d1", 0, "p=1", ({"v": "x"},))
    log.info("判定依据: 无主键表 delete -> ValidationError: %s", e.value)
    assert "主键" in str(e.value)
    assert t.current_version() == 0, "被拒绝的删除不得产生版本"


def test_delete_batch_validation(kv_table):
    """删除批次只允许主键列：缺主键、带非主键列、类型不符都明确拒绝。"""
    _write(kv_table, "b1", 0, "p=1", ({"id": 1, "val": "a"},))
    for bad, why in (
        (({"val": "x"},), "缺主键列"),
        (({"id": 1, "val": "x"},), "含非主键列"),
        (({"id": "not-an-int"},), "主键类型不符"),
        (({"id": 1, "nope": 2},), "含未知列"),
    ):
        with pytest.raises(ValidationError) as e:
            _delete(kv_table, f"bad-{why}", 1, "p=1", bad)
        log.info("判定依据: %s -> ValidationError: %s", why, e.value)
    assert kv_table.current_version() == 1, "非法删除批次不得产生版本"
    assert kv_table.read() == [{"id": 1, "val": "a"}]


def test_delete_conflicts_with_concurrent_write_same_partition(kv_table):
    """删除与同分区并发提交撞车：明确拒绝且原因可与普通写入失败区分；重读后重试结论稳定。"""
    _write(kv_table, "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}))
    # 另一个写提交先落地（v2），删除仍基于 v1 -> 必须冲突
    _write(kv_table, "b2", 1, "p=1", ({"id": 3, "val": "c"},))
    with pytest.raises(CommitConflictError) as e:
        _delete(kv_table, "d1", 1, "p=1", ({"id": 1},))
    log.info("判定依据: 删除基于 v1 但 v2 已写同分区 -> CommitConflictError: %s", e.value)
    assert kv_table.current_version() == 2, "冲突的删除不得产生版本"
    # 客户端重新读最新版本再提交 -> 成功且结论确定
    v = _delete(kv_table, "d1-retry", kv_table.current_version(), "p=1", ({"id": 1},))
    rows = kv_table.read()
    log.info("判定依据: 重读 v%d 后重试删除 -> v%d, 最新=%s", v - 1, v, rows)
    assert rows == [{"id": 2, "val": "b"}, {"id": 3, "val": "c"}]
    # 反向：删除先落地，基于旧版本的写也必须冲突
    with pytest.raises(CommitConflictError) as e2:
        _write(kv_table, "b3", 2, "p=1", ({"id": 9, "val": "z"},))
    log.info("判定依据: 写基于 v2 但 v3 是同分区删除 -> CommitConflictError: %s", e2.value)


def test_delete_disjoint_partition_merges(kv_table):
    """删除与写分区不重叠：按既定规则 rebase 合并，互不破坏。"""
    _write(kv_table, "b1", 0, "p=1", ({"id": 1, "val": "a"},))
    _write(kv_table, "b2", 1, "p=2", ({"id": 2, "val": "b"},))
    # 删除基于 v1（不知道 v2），但分区不同 -> 合并成功
    v = _delete(kv_table, "d1", 1, "p=1", ({"id": 1},))
    log.info("判定依据: 删除 p=1 与 v2 写 p=2 不相交 -> 合并为 v%d", v)
    assert v == 3
    assert kv_table.read(partition="p=1") == []
    assert kv_table.read(partition="p=2") == [{"id": 2, "val": "b"}]


def test_concurrent_deletes_and_writes_same_partition(kv_table):
    """多线程删除与写撞同一分区：冲突可区分、重试后全部成功，最终结论可复现。"""
    _write(kv_table, "seed", 0, "p=hot",
           tuple({"id": i, "val": f"v{i}"} for i in range(20)))
    results, errors = [], []

    def delete_worker(i):
        attempts = 0
        while True:
            attempts += 1
            try:
                v = _delete(kv_table, f"del-{i}", kv_table.current_version(),
                            "p=hot", ({"id": i},))
                results.append(("del", i, v, attempts))
                return
            except CommitConflictError as e:
                log.info("删除冲突重试: del-%d 第%d次, 原因=%s", i, attempts, e)

    def write_worker(i):
        attempts = 0
        while True:
            attempts += 1
            try:
                v = _write(kv_table, f"w-{i}", kv_table.current_version(),
                           "p=hot", ({"id": 100 + i, "val": "new"},))
                results.append(("write", i, v, attempts))
                return
            except CommitConflictError as e:
                log.info("写入冲突重试: w-%d 第%d次, 原因=%s", i, attempts, e)
            except LakehouseError as e:  # 不应发生：非冲突类失败
                errors.append(e)
                return

    threads = []
    for i in range(4):
        threads.append(threading.Thread(target=delete_worker, args=(i,)))
        threads.append(threading.Thread(target=write_worker, args=(i,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"出现非冲突类失败: {errors}"
    rows = kv_table.read()
    ids = {r["id"] for r in rows}
    log.info(
        "判定依据: 8 个任务全部成功(版本=%s); id 0..3 应被删除, id 100..103 应写入, 其余不变",
        sorted(v for _, _, v, _ in results),
    )
    assert len(results) == 8
    assert not ({0, 1, 2, 3} & ids), "被删除的主键不得出现在最新快照"
    assert {100, 101, 102, 103} <= ids, "并发写入的行不得丢失"
    assert ids == set(range(4, 20)) | {100, 101, 102, 103}
    assert len(rows) == len(ids), "不得有重复行"
    log.info("最终版本=v%d, 行数=%d", kv_table.current_version(), len(rows))


def test_delete_then_compact_keeps_rows_gone(kv_table):
    """删除之后压实：删掉的行不得回来，任何已发布版本的可见结果不变。"""
    _write(kv_table, "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}))
    _write(kv_table, "b2", 1, "p=1", ({"id": 3, "val": "c"},), "append")
    v_del = _delete(kv_table, "d1", 2, "p=1", ({"id": 1},))
    snapshots = {v: kv_table.read(version=v) for v in range(0, v_del + 1)}
    v_compact = kv_table.compact("p=1")
    after = kv_table.read()
    log.info(
        "判定依据: v%d 删除 id=1, v%d 压实 -> 最新应只剩 id=2,3: %s",
        v_del, v_compact, after,
    )
    assert after == [{"id": 2, "val": "b"}, {"id": 3, "val": "c"}]
    assert all(r["id"] != 1 for r in after), "压实不得把删掉的行带回来"
    for v, expected in snapshots.items():
        got = kv_table.read(version=v)
        assert got == expected, f"压实改变了已发布版本 v{v} 的可见结果: {got} != {expected}"
    log.info("判定依据: v0..v%d 全部已发布版本压实前后逐一比对一致", v_del)


def test_delete_then_schema_evolution_history_intact(kv_table):
    """删除后再改名/删列：历史版本仍按各自版本的列结构读出，不错位。"""
    _write(kv_table, "b1", 0, "p=1",
           ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}))
    v_del = _delete(kv_table, "d1", 1, "p=1", ({"id": 1},))
    kv_table.rename_column("val", "label")
    kv_table.add_column("score", "int", 0)
    latest = kv_table.read()
    history = kv_table.read(version=v_del)
    log.info(
        "判定依据: 删除 v%d 后改名 val->label 并加列 -> 最新=%s; 历史 v%d 仍用旧结构=%s",
        v_del, latest, v_del, history,
    )
    assert latest == [{"id": 2, "label": "b", "score": 0}]
    assert history == [{"id": 2, "val": "b"}], "历史版本按当时 schema 投影，不得错位"
    assert kv_table.read(version=1) == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
