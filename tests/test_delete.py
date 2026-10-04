"""行级删除：版本可见性、幂等、并发冲突、与压实/演进共存、失败原因可区分。

日志约定：每个用例打印覆盖场景、涉及的版本/分区与每一步的判定依据。
"""

import logging
import threading

import pytest

from lakehouse.errors import (
    CommitConflictError,
    DeleteConflictError,
    DeleteNotSupportedError,
    DuplicateBatchError,
    ValidationError,
)
from lakehouse.models import Column, DeleteBatch, Schema, WriteBatch

log = logging.getLogger("test.delete")


def _ids(rows):
    return {r["id"] for r in rows}


# ---------- 基本语义：最新不可见、历史仍可见、可写回 ----------

def test_delete_hides_row_latest_but_history_remains(kv_table):
    v1 = kv_table.commit(WriteBatch(
        "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}), "upsert"))
    v2 = kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    latest = kv_table.read()
    history = kv_table.read(version=v1)
    at_delete = kv_table.read(version=v2)
    log.info(
        "场景: 删除前后版本不同 / 分区=p=1 / 版本: v1(写入)->v2(删除)；"
        "判定依据: 最新=%s 不含 id=1；v2=%s 同样不含；删除前 v1=%s 原样保留",
        latest, at_delete, history,
    )
    assert latest == [{"id": 2, "val": "b"}]
    assert at_delete == [{"id": 2, "val": "b"}]
    assert history == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    assert kv_table.get_version(v2).operation == "delete"
    assert kv_table.get_version(v2).partition == "p=1"


def test_deleted_row_can_be_written_back(kv_table):
    v1 = kv_table.commit(WriteBatch(
        "b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    v2 = kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    v3 = kv_table.commit(WriteBatch("b2", v2, "p=1", ({"id": 1, "val": "A"},), "upsert"))
    rows = kv_table.read()
    log.info(
        "场景: 删除后同主键写回 / 分区=p=1 / 版本 v1->v2(delete)->v3(upsert)；"
        "判定依据: 写回后最新=%s 应把 id=1 当正常行重新出现，历史 v1 仍为旧值",
        rows,
    )
    assert rows == [{"id": 1, "val": "A"}]
    assert kv_table.read(version=v1) == [{"id": 1, "val": "a"}]
    assert kv_table.read(version=v2) == []


def test_delete_scoped_to_partition(kv_table):
    """同一主键值在不同分区是不同行；只删目标分区，其它分区不受影响。"""
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    v2 = kv_table.commit(WriteBatch("b2", v1, "p=2", ({"id": 1, "val": "c"},), "upsert"))
    v3 = kv_table.commit_delete(DeleteBatch("d1", v2, "p=1", ({"id": 1},)))
    log.info(
        "场景: 跨分区隔离 / 分区 p=1,p=2；判定依据: 删除 p=1 后 p=1=%s, p=2=%s",
        kv_table.read(partition="p=1"), kv_table.read(partition="p=2"),
    )
    assert kv_table.read(partition="p=1") == []
    assert kv_table.read(partition="p=2") == [{"id": 1, "val": "c"}]
    assert len(kv_table.read(version=v3)) == 1


def test_delete_composite_primary_key(service):
    t = service.create_table("ck", Schema(
        columns=(Column(1, "a", "int"), Column(2, "b", "str"), Column(3, "v", "int")),
        primary_keys=("a", "b"),
    ))
    t.commit(WriteBatch("b1", 0, "p=1", (
        {"a": 1, "b": "x", "v": 10}, {"a": 1, "b": "y", "v": 20},
        {"a": 2, "b": "x", "v": 30},
    ), "upsert"))
    t.commit_delete(DeleteBatch("d1", 1, "p=1", ({"a": 1, "b": "x"},)))
    rows = t.read()
    log.info("场景: 联合主键删除 / 分区=p=1；判定依据: 仅 (1,x) 被删, 剩余=%s", rows)
    assert rows == [{"a": 1, "b": "y", "v": 20}, {"a": 2, "b": "x", "v": 30}]


# ---------- 幂等与确定性 ----------

def test_delete_idempotent_replay(kv_table):
    v1 = kv_table.commit(WriteBatch(
        "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}), "upsert"))
    v2 = kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    # 同 batch_id 同内容、甚至带过期 base_version 重放：不报错、不产生新版本
    v2_again = kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    log.info(
        "场景: 删除重复提交 / 分区=p=1；判定依据: 首次 v%d, 重放返回 v%d, 当前 v%d（应相等且无新版本）",
        v2, v2_again, kv_table.current_version(),
    )
    assert v2 == v2_again
    assert kv_table.current_version() == 2
    assert kv_table.read() == [{"id": 2, "val": "b"}]
    files_after = len(kv_table.get_version().files)
    kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    assert len(kv_table.get_version().files) == files_after, "重放不得再产生数据文件痕迹"


def test_delete_same_batch_id_different_content_rejected(kv_table):
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    with pytest.raises(DuplicateBatchError) as e:
        kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 2},)))
    log.info("场景: 同 batch_id 删除内容不一致；判定依据: DuplicateBatchError: %s", e.value)
    # 数据保持首次删除的结论
    assert _ids(kv_table.read()) == set()


def test_delete_nonexistent_keys_is_deterministic(kv_table):
    v1 = kv_table.commit(WriteBatch(
        "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}), "upsert"))
    # 一批全部不存在：确定性 no-op，不报错；其它行原样
    v2 = kv_table.commit_delete(DeleteBatch("d-missing", v1, "p=1", ({"id": 901}, {"id": 902})))
    log.info(
        "场景: 删除不存在的主键 / 分区=p=1；判定依据: 不存在主键删除后版本=v%d, 行=%s 保持不变",
        v2, kv_table.read(),
    )
    assert _ids(kv_table.read()) == {1, 2}
    # 混合批次：存在的删掉，不存在的忽略，结论确定
    v3 = kv_table.commit_delete(DeleteBatch("d-mix", v2, "p=1", ({"id": 1}, {"id": 999})))
    assert _ids(kv_table.read()) == {2}
    # 对已删除的键再删一次：仍是确定的 no-op，不复活任何行
    v4 = kv_table.commit_delete(DeleteBatch("d-again", v3, "p=1", ({"id": 1},)))
    assert _ids(kv_table.read()) == {2}
    # 空批次删除：不报错，结论稳定
    v5 = kv_table.commit_delete(DeleteBatch("d-empty", v4, "p=1", ()))
    assert _ids(kv_table.read()) == {2}
    log.info(
        "场景: 混合/重复/空删除批次 / 分区=p=1；版本 v3,v4,v5；判定依据: 最终仅 id=2: %s",
        kv_table.read(),
    )


# ---------- 并发：冲突拒绝、原因可区分、重试后结论稳定 ----------

def test_delete_conflict_with_concurrent_upsert_rejected_and_retryable(kv_table):
    v1 = kv_table.commit(WriteBatch(
        "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}), "upsert"))
    # 另一个提交先基于 v1 落到 v2（同分区 upsert）
    v2 = kv_table.commit(WriteBatch("b2", v1, "p=1", ({"id": 3, "val": "c"},), "upsert"))
    # 删除仍基于 v1 -> 必须明确拒绝
    with pytest.raises(DeleteConflictError) as e:
        kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    log.info(
        "场景: 删除与同分区并发提交撞车 / 分区=p=1 / v1 之后有 v2(upsert)；"
        "判定依据: DeleteConflictError: %s", e.value,
    )
    assert kv_table.current_version() == v2, "被拒绝的删除不得落版本"
    assert _ids(kv_table.read()) == {1, 2, 3}
    # 客户端重读最新版本后重试 -> 成功，结论稳定可复现
    v3 = kv_table.commit_delete(DeleteBatch("d1", v2, "p=1", ({"id": 1},)))
    rows1 = kv_table.read()
    v3_again = kv_table.current_version()
    rows2 = kv_table.read()
    assert v3 == v3_again and rows1 == rows2
    assert _ids(rows1) == {2, 3}
    log.info("场景: 冲突后重读重试 / 判定依据: v%d 删除成功且结果可复现: %s", v3, rows1)


def test_delete_vs_upsert_conflict_both_directions(kv_table):
    """删除先落地，滞后的普通写入同分区 -> CommitConflictError；两类失败可区分。"""
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    with pytest.raises(CommitConflictError):
        kv_table.commit(WriteBatch("b2", v1, "p=1", ({"id": 9, "val": "z"},), "upsert"))
    # DeleteConflictError 是 CommitConflictError 的子类，但类型名可区分
    assert issubclass(DeleteConflictError, CommitConflictError)
    try:
        kv_table.commit_delete(DeleteBatch("d2", v1, "p=1", ({"id": 1},)))
    except CommitConflictError as e:
        log.info(
            "场景: 失败原因可区分 / 分区=p=1；判定依据: 普通写入撞车=CommitConflictError，"
            "删除撞车类型=%s（也是其子类）: %s", type(e).__name__, e,
        )
        assert type(e) is DeleteConflictError


def test_delete_disjoint_partition_merges(kv_table):
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    v2 = kv_table.commit(WriteBatch("b2", v1, "p=2", ({"id": 2, "val": "b"},), "upsert"))
    # 删除基于旧版本但分区与 v2 不相交 -> 按既定 rebase 规则合并
    v3 = kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    log.info(
        "场景: 分区不相交合并 / 分区 p=1,p=2；v2 写 p=2, 删除基于 v1 删 p=1 -> v%d; "
        "判定依据: p=1=%s（已删）, p=2=%s（保留）",
        v3, kv_table.read(partition="p=1"), kv_table.read(partition="p=2"),
    )
    assert v3 == 3
    assert kv_table.read(partition="p=1") == []
    assert kv_table.read(partition="p=2") == [{"id": 2, "val": "b"}]


def test_concurrent_deletes_and_writes_same_partition(kv_table):
    """删除与写入在同分区并发：冲突可重试，最终结论确定且版本号为 1..N 排列。"""
    # 预置待删行 id 1..10；各线程另有不与删除集合相交的新 id
    seed = tuple({"id": i, "val": "seed"} for i in range(1, 11))
    base = kv_table.commit(WriteBatch("seed", 0, "p=hot", seed, "upsert"))
    outcomes, errors = [], []

    def delete_worker(i):
        batch = f"del{i}"
        keys = ({"id": 2 * i + 1}, {"id": 2 * i + 2})  # 每个线程删两互不相同的种子行
        attempts = 0
        while True:
            attempts += 1
            try:
                v = kv_table.commit_delete(DeleteBatch(batch, kv_table.current_version(), "p=hot", keys))
                outcomes.append(("delete", v, attempts))
                return
            except CommitConflictError as e:
                assert type(e) is DeleteConflictError
                log.info("删除冲突重试: %s 第%d次 (%s)", batch, attempts, e)

    def write_worker(i):
        batch = f"up{i}"
        rows = ({"id": 1000 + i, "val": f"w{i}"},)
        attempts = 0
        while True:
            attempts += 1
            try:
                v = kv_table.commit(WriteBatch(batch, kv_table.current_version(), "p=hot", rows, "upsert"))
                outcomes.append(("write", v, attempts))
                return
            except CommitConflictError as e:
                assert type(e) is CommitConflictError and not isinstance(e, DeleteConflictError)
                log.info("写入冲突重试: %s 第%d次 (%s)", batch, attempts, e)

    threads = [threading.Thread(target=delete_worker, args=(i,)) for i in range(5)]
    threads += [threading.Thread(target=write_worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    versions = sorted(v for _, v, _ in outcomes)
    final_ids = _ids(kv_table.read())
    log.info(
        "场景: 5 删除 + 5 写入同分区并发 / 分区=p=hot / 起始 v%d；版本序列=%s；"
        "判定依据: 版本号应为 %d..%d 的排列；最终 id 集合大小=%d",
        base, versions, base + 1, base + len(threads), len(final_ids),
    )
    assert versions == list(range(base + 1, base + len(threads) + 1))
    # 10 个种子行全部被删；5 个新行全部保留
    assert final_ids == {1000 + i for i in range(5)}
    # 可复现：再读一次结论一致
    assert _ids(kv_table.read()) == final_ids


# ---------- 能力边界：无主键表拒绝；删除键校验 ----------

def test_delete_rejected_on_table_without_primary_key(service):
    t = service.create_table("nopk", Schema(columns=(Column(1, "id", "int"), Column(2, "v", "str"))))
    t.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "v": "x"},), "append"))
    with pytest.raises(DeleteNotSupportedError) as e:
        t.commit_delete(DeleteBatch("d1", 1, "p=1", ({"id": 1},)))
    log.info("场景: 无主键表删除；判定依据: DeleteNotSupportedError 明确拒绝: %s", e.value)
    assert t.read() == [{"id": 1, "v": "x"}], "被拒绝的删除不得影响数据"
    assert t.current_version() == 1


def test_delete_key_validation(kv_table):
    kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    # 缺主键列
    with pytest.raises(ValidationError) as e1:
        kv_table.commit_delete(DeleteBatch("d1", 1, "p=1", ({"val": "a"},)))
    # 带非主键列
    with pytest.raises(ValidationError) as e2:
        kv_table.commit_delete(DeleteBatch("d2", 1, "p=1", ({"id": 1, "val": "a"},)))
    # 类型不符
    with pytest.raises(ValidationError) as e3:
        kv_table.commit_delete(DeleteBatch("d3", 1, "p=1", ({"id": "not-int"},)))
    log.info(
        "场景: 删除键校验 / 分区=p=1；判定依据: 缺列=%s；多列=%s；类型错=%s",
        e1.value, e2.value, e3.value,
    )
    assert kv_table.current_version() == 1, "非法删除批次不得落版本"


# ---------- 与压实共存：不复活、不改变任何已发布版本 ----------

def test_compact_after_delete_never_resurrects(kv_table):
    # 三个 append 批次 + 一次删除，制造多文件
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "append"))
    v2 = kv_table.commit(WriteBatch("b2", v1, "p=1", ({"id": 2, "val": "b"},), "append"))
    v3 = kv_table.commit(WriteBatch("b3", v2, "p=1", ({"id": 3, "val": "c"},), "append"))
    v4 = kv_table.commit_delete(DeleteBatch("d1", v3, "p=1", ({"id": 2},)))
    published = {v: kv_table.read(version=v) for v in range(1, v4 + 1)}

    vc = kv_table.compact("p=1")
    log.info(
        "场景: 删除后压实 / 分区=p=1 / 删除=v%d, 压实=v%d；判定依据: 逐一比对 v1..v4 快照，"
        "最新不得复活 id=2", v4, vc,
    )
    assert _ids(kv_table.read()) == {1, 3}, "压实不得让已删除行复活"
    for v, expected in published.items():
        assert kv_table.read(version=v) == expected, f"压实改变了已发布版本 v{v}"
    # 压实后把删除的行写回，是一条正常新行；历史仍不变
    v6 = kv_table.commit(WriteBatch("b4", vc, "p=1", ({"id": 2, "val": "B"},), "upsert"))
    assert _ids(kv_table.read()) == {1, 2, 3}
    assert kv_table.read(version=v4) == [
        {"id": 1, "val": "a"}, {"id": 3, "val": "c"}
    ]
    log.info("场景: 压实后写回 / 新版本 v%d；判定依据: id=2 以新值正常出现, v4 历史不变", v6)


def test_delete_after_compact_remains_hidden(kv_table):
    """先压实再删除，再压实：删除结论依旧稳定。"""
    for i, vid in enumerate((1, 2, 3)):
        kv_table.commit(WriteBatch(
            f"b{i}", kv_table.current_version(), "p=1",
            ({"id": vid, "val": str(vid)},), "append"))
    vc = kv_table.compact("p=1")
    vd = kv_table.commit_delete(DeleteBatch("d1", vc, "p=1", ({"id": 2},)))
    assert _ids(kv_table.read()) == {1, 3}
    vc2 = kv_table.compact("p=1")
    log.info(
        "场景: 压实->删除->压实 / 分区=p=1 / 版本 压实v%d 删除v%d 再压实v%d；"
        "判定依据: 最终=%s 仍无 id=2", vc, vd, vc2, kv_table.read(),
    )
    assert _ids(kv_table.read()) == {1, 3}
    assert kv_table.read(version=vd) == [
        {"id": 1, "val": "1"}, {"id": 3, "val": "3"}
    ]


# ---------- 与结构演进共存：历史版本按各自 schema 读取 ----------

def test_delete_history_readable_after_rename_and_drop(kv_table):
    v1 = kv_table.commit(WriteBatch(
        "b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}), "upsert"))
    v2 = kv_table.commit_delete(DeleteBatch("d1", v1, "p=1", ({"id": 1},)))
    v3 = kv_table.add_column("score", "int", 0)
    v4 = kv_table.rename_column("val", "value")
    v5 = kv_table.rename_column("id", "uid")          # 主键改名：列 id 不变
    v6 = kv_table.drop_column("score")
    latest = kv_table.read()
    log.info(
        "场景: 删除后加列/改名/删列 / 分区=p=1 / v2(delete) v3(add) v4(rename val) "
        "v5(rename pk) v6(drop)；判定依据: 最新=%s；v1=%s；v2=%s",
        latest, kv_table.read(version=v1), kv_table.read(version=v2),
    )
    # 最新：删除生效，按新列名读取，不错位
    assert latest == [{"uid": 2, "value": "b"}]
    # 删除前的历史版本：按当时列结构读出旧名与被删的行
    assert kv_table.read(version=v1) == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    # 删除版本的历史快照：无 id=1，仍是当时列名
    assert kv_table.read(version=v2) == [{"id": 2, "val": "b"}]
    # 演进后用新主键名删除剩余行，正常工作
    v7 = kv_table.commit_delete(DeleteBatch("d2", v6, "p=1", ({"uid": 2},)))
    assert kv_table.read() == []
    # 写回同样用新主键名
    kv_table.commit(WriteBatch("b2", v7, "p=1", ({"uid": 1, "value": "A"},), "upsert"))
    assert kv_table.read() == [{"uid": 1, "value": "A"}]
    log.info("场景: 演进后按新主键名删除/写回 / v7 删除 uid=2 后再写回: %s", kv_table.read())
