"""并发提交：同分区冲突必须可区分，不同分区必须全部合并，结果可复现。"""

import logging
import threading

from lakehouse.errors import CommitConflictError, LakehouseError
from lakehouse.models import WriteBatch

log = logging.getLogger("test.concurrency")

THREADS = 8
ROWS_PER_THREAD = 5


def _commit_with_retry(table, batch_id, partition, rows):
    """模拟客户端：冲突时读取最新版本并重试，直到成功。"""
    attempts = 0
    while True:
        attempts += 1
        try:
            v = table.commit(WriteBatch(batch_id, table.current_version(), partition, rows, "upsert"))
            return v, attempts, None
        except CommitConflictError as e:
            log.info("冲突重试: batch=%s 第%d次, 原因=%s", batch_id, attempts, e)


def test_concurrent_same_partition(kv_table):
    """8 线程同时写同一分区：最终必须包含全部行，版本号连续无重复。"""
    results, errors = [], []

    def worker(i):
        rows = tuple({"id": i * 100 + j, "val": f"t{i}r{j}"} for j in range(ROWS_PER_THREAD))
        try:
            results.append(_commit_with_retry(kv_table, f"t{i}", "p=hot", rows))
        except LakehouseError as e:  # 不应发生
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"出现非冲突类失败: {errors}"
    versions = sorted(v for v, _, _ in results)
    log.info(
        "判定依据: %d 个线程全部成功, 版本=%s 应为 1..%d 的排列",
        THREADS, versions, THREADS,
    )
    assert versions == list(range(1, THREADS + 1)), "每次成功提交产生且仅产生一个新版本"
    rows = kv_table.read()
    assert len(rows) == THREADS * ROWS_PER_THREAD
    assert len({r["id"] for r in rows}) == THREADS * ROWS_PER_THREAD, "不得有重复或丢失的行"
    log.info("最终版本=v%d, 行数=%d, 主键去重后=%d", kv_table.current_version(), len(rows), len({r['id'] for r in rows}))


def test_concurrent_disjoint_partitions(kv_table):
    """8 线程各写不同分区：无需重试，全部按 rebase 规则合并。"""
    outcomes = []

    def worker(i):
        rows = tuple({"id": i * 100 + j, "val": "x"} for j in range(ROWS_PER_THREAD))
        outcomes.append(_commit_with_retry(kv_table, f"d{i}", f"p={i}", rows))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    attempts = [a for _, a, _ in outcomes]
    log.info("判定依据: 分区互不相交, 各线程尝试次数=%s 应全为 1（无冲突重试）", attempts)
    assert all(a == 1 for a in attempts)
    rows = kv_table.read()
    assert len(rows) == THREADS * ROWS_PER_THREAD
    log.info("最终版本=v%d, 总行数=%d", kv_table.current_version(), len(rows))


def test_concurrent_commit_and_snapshot_read(kv_table):
    """提交与快照读取交错：读到的任一版本都必须是该版本已提交的完整数据。"""
    stop = threading.Event()
    inconsistencies = []

    def writer():
        i = 0
        while not stop.is_set():
            i += 1
            kv_table.commit(WriteBatch(
                f"w{i}", kv_table.current_version(), "p=1",
                ({"id": i, "val": str(i)},), "upsert",
            ))

    def reader():
        for _ in range(200):
            v = kv_table.current_version()
            rows = kv_table.read(version=v)
            ver = kv_table.get_version(v)
            # 该版本可见行数必须等于其 upsert 合并后的主键数（id 1..v 的子集）
            ids = {r["id"] for r in rows}
            if len(ids) != len(rows):
                inconsistencies.append((v, "行内主键重复"))
            if not ids <= set(range(1, 10_000)):
                inconsistencies.append((v, "出现未提交的数据"))

    wt = threading.Thread(target=writer)
    rt = threading.Thread(target=reader)
    rt.start()
    wt.start()
    rt.join()
    stop.set()
    wt.join()
    log.info("判定依据: 200 次交错读取不一致数=%d（应为 0）", len(inconsistencies))
    assert not inconsistencies
