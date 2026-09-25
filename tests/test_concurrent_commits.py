"""并发提交测试。

覆盖：
- 多线程并发 append：全部成功、无重复行/重复版本，结果为并集；
- 同一 batch_id 并发重复提交：只有一个版本，其余全部识别为 duplicate，
  返回同一个既有版本号；
- upsert 与触碰同分区的并发提交交错：恰好一方成功，冲突方抛
  CommitConflictError（与 duplicate / validation 错误可区分）；
- 判定日志必须包含版本、分区与判定依据（caplog 断言）。
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from lakehouse.catalog import Catalog
from lakehouse.errors import (
    CommitConflictError,
    DuplicateBatchError,
    SchemaValidationError,
)
from lakehouse.schema import SchemaEvolution
from lakehouse.table import LakehouseTable

COLUMNS = [("id", "long"), ("p", "string"), ("v", "int")]


@pytest.fixture
def table(tmp_path):
    catalog = Catalog(str(tmp_path))
    catalog.create_table("events", COLUMNS, primary_key=("id",), partition_column="p")
    return catalog.open_table("events")


def _make_table(tmp_path, name="events", pk=("id",), partition_column="p"):
    catalog = Catalog(str(tmp_path))
    catalog.create_table(name, COLUMNS, primary_key=pk, partition_column=partition_column)
    return catalog.open_table(name)


def test_concurrent_appends_all_accepted_without_duplicates(tmp_path):
    t = _make_table(tmp_path)
    n_threads, per_thread = 8, 25
    barrier = threading.Barrier(n_threads)

    def worker(tid):
        barrier.wait()  # 尽量放大竞态
        versions = []
        for i in range(per_thread):
            rid = tid * per_thread + i + 1
            versions.append(
                t.append(
                    [{"id": rid, "p": "a", "v": rid}],
                    batch_id=f"app-{tid}-{i}",
                )
            )
        return versions

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        all_versions = [v for vs in pool.map(worker, range(n_threads)) for v in vs]

    # 版本号连续唯一，无重复版本
    assert len(all_versions) == n_threads * per_thread
    assert sorted(all_versions) == list(range(1, n_threads * per_thread + 1))

    rows = t.read()
    assert len(rows) == n_threads * per_thread
    assert len({r["id"] for r in rows}) == len(rows)  # 无重复数据
    assert all(r["v"] == r["id"] for r in rows)


def test_concurrent_same_batch_id_is_idempotent(tmp_path):
    t = _make_table(tmp_path)
    # 先提交一次建立批次
    winner_version = t.append([{"id": 1, "p": "a", "v": 1}], batch_id="dup-batch")
    assert winner_version == 1

    n = 10
    outcomes = []

    def worker(_):
        try:
            v = t.append([{"id": 999, "p": "a", "v": 999}], batch_id="dup-batch")
            outcomes.append(("accepted", v))
        except DuplicateBatchError as exc:
            outcomes.append(("duplicate", exc.existing_version))

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(worker, range(n)))

    assert all(o[0] == "duplicate" for o in outcomes)
    assert {o[1] for o in outcomes} == {1}  # 全部指向同一个既有版本
    assert t._meta()["current_version"] == 1  # 没有产生任何新版本
    rows = t.read()
    assert rows == [{"id": 1, "p": "a", "v": 1}]  # 重复批次数据未写入


def test_concurrent_upsert_conflict_is_rejected_not_silently_merged(tmp_path):
    t = _make_table(tmp_path)
    base = t.append([{"id": 1, "p": "a", "v": 1}], batch_id="seed")
    assert base == 1

    # 两个 upsert 都基于 v1；串行化后只有先到者成功，后者必须冲突
    outcomes: list[tuple[str, int | Exception]] = []
    lock = threading.Lock()
    start = threading.Event()

    def upsert(value, batch):
        start.wait()
        try:
            v = t.upsert(
                [{"id": 1, "p": "a", "v": value}],
                batch_id=batch,
                expected_version=1,
            )
            with lock:
                outcomes.append(("accepted", v))
        except Exception as exc:  # noqa: BLE001 - 测试要按类型区分
            with lock:
                outcomes.append(("rejected", exc))

    threads = [
        threading.Thread(target=upsert, args=(100, "u-100")),
        threading.Thread(target=upsert, args=(200, "u-200")),
    ]
    for th in threads:
        th.start()
    start.set()
    for th in threads:
        th.join()

    accepted = [o for o in outcomes if o[0] == "accepted"]
    rejected = [o for o in outcomes if o[0] == "rejected"]
    assert len(accepted) == 1, outcomes
    assert accepted[0][1] == 2
    assert len(rejected) == 1
    exc = rejected[0][1]
    assert isinstance(exc, CommitConflictError)
    assert not isinstance(exc, DuplicateBatchError)  # 失败原因可区分
    assert "v2" in str(exc)  # 判定依据指出冲突版本

    # 成功者的值完整保留，失败方没有静默覆盖；历史版本不变
    final_v = t.read()[0]["v"]
    assert final_v in (100, 200)
    assert t.read(version=1) == [{"id": 1, "p": "a", "v": 1}]


def test_append_after_stale_base_merges_but_upsert_rejects(tmp_path):
    """既定合并规则：append 过期基版本按并集接受；upsert 过期基版本拒绝。"""
    t = _make_table(tmp_path)
    t.append([{"id": 1, "p": "a", "v": 1}], batch_id="a1")  # v1

    # append 基于 v0 仍然接受（append 语义为并集）
    v = t.append([{"id": 2, "p": "a", "v": 2}], batch_id="a2", expected_version=0)
    assert v == 2
    assert {r["id"] for r in t.read()} == {1, 2}

    # upsert 基于 v0：区间内 v1/v2 都触碰分区 a -> 拒绝且报告具体冲突版本
    with pytest.raises(CommitConflictError) as ei:
        t.upsert([{"id": 1, "p": "a", "v": 9}], batch_id="u-stale", expected_version=0)
    assert "v2" in str(ei.value)


def test_error_types_are_distinguishable(tmp_path):
    t = _make_table(tmp_path)
    t.append([{"id": 1, "p": "a", "v": 1}], batch_id="b1")

    with pytest.raises(DuplicateBatchError) as ei:
        t.append([{"id": 1, "p": "a", "v": 1}], batch_id="b1")
    assert ei.value.existing_version == 1

    with pytest.raises(SchemaValidationError):
        t.append([{"id": 2, "p": "a", "v": "not-an-int"}], batch_id="b2")

    with pytest.raises(CommitConflictError):
        t.upsert([{"id": 1, "p": "a", "v": 2}], batch_id="u", expected_version=0)


def test_commit_decision_log_contains_version_partition_and_reason(tmp_path, caplog):
    t = _make_table(tmp_path)
    with caplog.at_level(logging.INFO, logger="lakehouse.commit"):
        t.append([{"id": 1, "p": "2024-01", "v": 1}], batch_id="log-batch")
        with pytest.raises(DuplicateBatchError):
            t.append([{"id": 2, "p": "2024-01", "v": 2}], batch_id="log-batch")
        with pytest.raises(CommitConflictError):
            t.upsert(
                [{"id": 1, "p": "2024-01", "v": 2}],
                batch_id="log-upsert",
                expected_version=0,
            )

    text = caplog.text
    assert '"operation": "append"' in text
    assert '"candidate_version": 1' in text
    assert '"partition": "2024-01"' in text
    assert '"batch_id": "log-batch"' in text
    assert '"outcome": "duplicate"' in text
    assert '"outcome": "conflict"' in text
    assert "partition touched by v1" in text  # 判定依据


def test_upsert_replaces_rows_across_appended_files(tmp_path):
    """多次 append 后 upsert 跨文件替换，结果确定且无旧值残留。"""
    t = _make_table(tmp_path)
    t.append([{"id": 1, "p": "a", "v": 1}], batch_id="a1")
    t.append([{"id": 2, "p": "a", "v": 2}], batch_id="a2")
    t.upsert([{"id": 1, "p": "a", "v": 10}, {"id": 2, "p": "a", "v": 20}], batch_id="u")
    rows = sorted(t.read(), key=lambda r: r["id"])
    assert [(r["id"], r["v"]) for r in rows] == [(1, 10), (2, 20)]
