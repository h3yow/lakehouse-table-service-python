"""提交判定日志：为每次提交/拒绝/去重输出可解释、可复现的判定记录。

日志走标准 logging，统一字段：
    table          表名
    operation      append / upsert / compaction / schema_change
    base_version   提交时所基于的当前版本
    cand_version   若接受，新发布的版本号；否则为 null
    partition      涉及分区
    batch_id       批次幂等键
    outcome        accepted / duplicate / conflict / rejected
    reason         人可读的判定依据
    conflict_with  冲突的已发布版本号（如有）

判定依据写入日志后，测试可以打开 caplog 断言具体版本与分区，
保证"失败原因可区分、一致性结论可复现"。
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass

logger = logging.getLogger("lakehouse.commit")


@dataclass(frozen=True)
class CommitDecision:
    table: str
    operation: str
    base_version: int
    candidate_version: int | None
    partition: str | None
    batch_id: str | None
    outcome: str  # accepted / duplicate / conflict / rejected
    reason: str
    conflicting_version: int | None = None


def log_decision(decision: CommitDecision) -> None:
    """输出一条结构化（单行 JSON）判定日志。"""
    payload = asdict(decision)
    line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if decision.outcome == "accepted":
        logger.info("commit-decision %s", line)
    else:
        # duplicate/conflict/rejected 用 warning，便于在嘈杂日志中定位
        logger.warning("commit-decision %s", line)
