"""快照读取与分区压实测试。

覆盖：
- 时间旅行：每个已发布版本只能看到截至该版本的数据，未来数据不可见；
- 压实前后：所有历史版本读取结果逐行一致，最新版结果不变；
- 压实只做清单替换：物理文件保留、每分区 live 文件收敛为一个；
- 并发交错：提交与压实同时进行时，每个成功提交结果可解释，
  冲突方拿到 CommitConflictError；读取者永远读到某个已发布版本；
- 跨分区压实隔离、空分区压实被拒绝。
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from lakehouse.catalog import Catalog
from lakehouse.errors import CommitConflictError, VersionNotFoundError

COLUMNS = [("id", "long"), ("p", "string"), ("v", "int")]


@pytest.fixture
def table(tmp_path):
    catalog = Catalog(str(tmp_path))
    catalog.create_table("events", COLUMNS, primary_key=("id",), partition_column="p")
    return catalog.open_table("events")


def _snapshot_all_versions(t):
    current = t._meta()["current_version"]
    return {v: t.read(version=v) for v in range(1, current + 1)}


def test_snapshot_reads_only_committed_data(table):
    table.append([{"id": 1, "p": "a", "v": 1}], batch_id="a1")  # v1
    table.append([{"id": 2, "p": "a", "v": 2}], batch_id="a2")  # v2
    table.append([{"id": 3, "p": "b", "v": 3}], batch_id="a3")  # v3

    assert [r["id"] for r in table.read(version=1)] == [1]
    assert sorted(r["id"] for r in table.read(version=2)) == [1, 2]
    assert sorted(r["id"] for r in table.read(version=3)) == [1, 2, 3]
    assert sorted(r["id"] for r in table.read(version=3, partition="b")) == [3]
    assert table.read(version=3, partition="c") == []
    # 默认读取最新
    assert sorted(r["id"] for r in table.read()) == [1, 2, 3]
    # 未发布/不存在版本
    with pytest.raises(VersionNotFoundError):
        table.read(version=4)
    with pytest.raises(VersionNotFoundError):
        table.read(version=-1)


def test_compaction_preserves_every_published_version(table):
    for i in range(1, 7):
        part = "a" if i <= 4 else "b"
        table.append([{"id": i, "p": part, "v": i}], batch_id=f"a{i}")
    before = _snapshot_all_versions(table)
    latest_before = table.read()

    vc = table.compact()  # 全表压实
    assert vc == 7

    # 所有已发布版本逐行一致（排序后比较，消除顺序差异）
    for v, rows in before.items():
        assert sorted(table.read(version=v), key=lambda r: r["id"]) == sorted(
            rows, key=lambda r: r["id"]
        ), f"v{v} changed after compaction"
    assert sorted(table.read(), key=lambda r: r["id"]) == sorted(
        latest_before, key=lambda r: r["id"]
    )

    # 每个分区 live 文件收敛为一个
    live = table._live_files(vc)
    parts = {f.partition for f in live.values()}
    assert parts == {"a", "b"}
    assert all(
        sum(1 for f in live.values() if f.partition == p) == 1 for p in parts
    )

    # 旧数据文件物理保留（旧版本时间旅行依赖）
    v6_files = table._live_files(6)
    import os
    for f in v6_files.values():
        assert os.path.isfile(
            os.path.join(table.storage.data_dir(table.name), os.path.basename(f.path))
        )


def test_partition_compaction_is_isolated(table):
    for i in range(1, 5):
        part = "a" if i <= 2 else "b"
        table.append([{"id": i, "p": part, "v": i}], batch_id=f"a{i}")
    before = _snapshot_all_versions(table)

    vc = table.compact(partition="a")
    live = table._live_files(vc)
    assert sum(1 for f in live.values() if f.partition == "a") == 1
    assert sum(1 for f in live.values() if f.partition == "b") == 2  # b 未被触碰

    for v, rows in before.items():
        assert sorted(table.read(version=v), key=lambda r: r["id"]) == sorted(
            rows, key=lambda r: r["id"]
        )
    assert sorted((r["id"], r["p"]) for r in table.read()) == [
        (1, "a"), (2, "a"), (3, "b"), (4, "b")
    ]


def test_compact_empty_or_stale_base_rejected(table):
    with pytest.raises(CommitConflictError):
        table.compact()  # 空表无 live 文件
    table.append([{"id": 1, "p": "a", "v": 1}], batch_id="a1")
    with pytest.raises(CommitConflictError):
        table.compact(expected_version=0)  # 基版本过期


def test_writes_after_compaction_stay_correct(table):
    table.append([{"id": 1, "p": "a", "v": 1}], batch_id="a1")
    table.append([{"id": 2, "p": "a", "v": 2}], batch_id="a2")
    table.compact(partition="a")  # v3
    # 压实后 append + upsert
    table.append([{"id": 3, "p": "a", "v": 3}], batch_id="a3")  # v4
    table.upsert([{"id": 1, "p": "a", "v": 10}], batch_id="u1")  # v5
    assert sorted((r["id"], r["v"]) for r in table.read()) == [
        (1, 10), (2, 2), (3, 3)
    ]
    # 再次压实后仍正确（多代 compaction）
    table.compact(partition="a")  # v6
    assert sorted((r["id"], r["v"]) for r in table.read()) == [
        (1, 10), (2, 2), (3, 3)
    ]


def test_concurrent_compaction_and_appends_are_explainable(table):
    table.append([{"id": 0, "p": "a", "v": 0}], batch_id="seed")
    results: list[tuple[str, object]] = []
    lock = threading.Lock()

    def do_append(i):
        try:
            v = table.append([{"id": i, "p": "a", "v": i}], batch_id=f"c{i}")
            with lock:
                results.append(("append-ok", v))
        except CommitConflictError as exc:
            with lock:
                results.append(("conflict", str(exc)))

    def do_compact():
        try:
            v = table.compact(partition="a")
            with lock:
                results.append(("compact-ok", v))
        except CommitConflictError as exc:
            with lock:
                results.append(("conflict", str(exc)))

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(do_compact)]
        futures += [pool.submit(do_append, i) for i in range(1, 6)]
        [f.result() for f in futures]

    # 每个返回版本号的操作都真实发布；所有最终行可读且无重复
    versions = [v for kind, v in results if kind.endswith("-ok")]
    assert len(versions) == len(set(versions))  # 版本号唯一
    ids = {r["id"] for r in table.read()}
    # seed 行 + 每个成功 append 的行；冲突的 append 不落数据
    assert ids <= {0, 1, 2, 3, 4, 5}
    assert 0 in ids
    assert len(table.read()) == len(ids)

    # 最终发布版本必然是一个可完整复现的状态：再压一次，结果不变
    before = sorted(table.read(), key=lambda r: r["id"])
    table.compact(partition="a")
    assert sorted(table.read(), key=lambda r: r["id"]) == before


def test_reader_during_concurrent_commits_always_sees_published_version(table):
    table.append([{"id": 0, "p": "a", "v": 0}], batch_id="seed")
    stop = threading.Event()
    seen_counts: set[int] = set()

    def reader():
        while not stop.is_set():
            rows = table.read()  # 不显式给版本 -> 读某个已发布 current
            seen_counts.add(len(rows))

    def writer():
        for i in range(1, 30):
            table.append([{"id": i, "p": "a", "v": i}], batch_id=f"w{i}")

    tr = threading.Thread(target=reader)
    tw = threading.Thread(target=writer)
    tr.start()
    tw.start()
    tw.join()
    stop.set()
    tr.join()

    # 读者看到的行数必须是 1..30 的前缀快照（单调、可解释）
    assert seen_counts <= set(range(1, 31))
    assert len(table.read()) == 30


def test_history_records_operations_and_batches(table):
    table.append([{"id": 1, "p": "a", "v": 1}], batch_id="a1")
    table.compact(partition="a")
    history = table.history()
    assert [(h.version, h.operation) for h in history] == [(1, "append"), (2, "compaction")]
    assert history[0].batch_id == "a1"
    assert history[1].compaction_of_version == 1
