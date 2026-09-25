"""快照读取与分区压实：版本可见性、压实不变性、交错可复现性。"""

import logging
import threading

from lakehouse.errors import VersionNotFoundError
from lakehouse.models import WriteBatch

log = logging.getLogger("test.snapshot")


def _seed(kv_table, partitions=("p=1", "p=2"), batches=3):
    """每个分区追加 batches 个单文件批次，返回各版本号。"""
    versions = []
    for i in range(batches):
        for p in partitions:
            v = kv_table.commit(WriteBatch(
                f"seed-{p}-{i}", kv_table.current_version(), p,
                ({"id": hash((p, i)) % 10_000, "val": f"{p}#{i}"},), "append",
            ))
            versions.append(v)
    return versions


def test_snapshot_sees_only_committed_data(kv_table):
    v1 = kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    v2 = kv_table.commit(WriteBatch("b2", v1, "p=1", ({"id": 2, "val": "b"},), "upsert"))
    r1, r2 = kv_table.read(version=v1), kv_table.read(version=v2)
    log.info("判定依据: v%d 只能看到 b1 的数据=%s, v%d 看到两批=%s", v1, r1, v2, r2)
    assert r1 == [{"id": 1, "val": "a"}]
    assert r2 == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]


def test_read_unknown_version_rejected(kv_table):
    import pytest
    with pytest.raises(VersionNotFoundError) as e:
        kv_table.read(version=99)
    log.info("判定依据: 未提交的版本 -> VersionNotFoundError: %s", e.value)


def test_compact_preserves_all_published_versions(kv_table):
    versions = _seed(kv_table)
    snapshots = {v: kv_table.read(version=v) for v in versions}
    files_before = len(kv_table.get_version().files)
    vc = kv_table.compact("p=1")
    files_after = len(kv_table.get_version().files)
    log.info(
        "判定依据: 压实前文件数=%d, 压实后=%d; 逐一比对 %d 个已发布版本的可见结果",
        files_before, files_after, len(snapshots),
    )
    assert files_after == files_before - 2, "p=1 的 3 个文件应合并为 1 个"
    for v, expected in snapshots.items():
        actual = kv_table.read(version=v)
        assert actual == expected, f"压实改变了已发布版本 v{v} 的可见结果"
    assert kv_table.read() == snapshots[versions[-1]], "压实不得改变最新可见结果"
    log.info("全部 %d 个版本在压实后可见结果一致", len(snapshots))


def test_compact_interleaved_with_commit_and_read(kv_table):
    """压实、提交、读取交错：任何时刻读取任一已发布版本结果稳定。"""
    _seed(kv_table, partitions=("p=1",), batches=3)
    published = {v: kv_table.read(version=v) for v in range(1, kv_table.current_version() + 1)}
    errors = []

    def compactor():
        for _ in range(3):
            try:
                kv_table.compact("p=1")
            except Exception as e:  # noqa: BLE001
                errors.append(("compact", e))

    def writer():
        for i in range(5):
            kv_table.commit(WriteBatch(
                f"w{i}", kv_table.current_version(), "p=1",
                ({"id": 50_000 + i, "val": "w"},), "append",
            ))

    def reader():
        for v, expected in list(published.items()):
            for _ in range(3):
                actual = kv_table.read(version=v)
                if actual != expected:
                    errors.append(("read", f"v{v} 结果漂移: {actual} != {expected}"))

    threads = [threading.Thread(target=f) for f in (compactor, writer, reader)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("判定依据: 交错执行后错误数=%d（应为 0）, 最终版本=v%d", len(errors), kv_table.current_version())
    assert not errors
    # 已发布版本在交错后依旧稳定
    for v, expected in published.items():
        assert kv_table.read(version=v) == expected


def test_compact_empty_partition_is_noop(kv_table):
    kv_table.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"},), "upsert"))
    v = kv_table.current_version()
    v2 = kv_table.compact("p=empty")
    log.info("判定依据: 空分区压实应为 no-op, v%d == v%d", v, v2)
    assert v2 == v
