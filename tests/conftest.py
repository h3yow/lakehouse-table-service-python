"""pytest 公共配置：

- 强制 lakehouse.commit 判定日志在 INFO 级别传播到 pytest 捕获系统，
  使单测日志中可以看到每次提交涉及的版本、分区、批次与判定依据；
- 失败用例的日志通过 "Captured log call" 段直接打印，便于复盘。
"""

from __future__ import annotations

import logging

import pytest

commit_logger = logging.getLogger("lakehouse.commit")
commit_logger.setLevel(logging.INFO)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):  # pragma: no cover - 测试基础设施
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    commit_logger.addHandler(handler)
    try:
        yield
    finally:
        commit_logger.removeHandler(handler)
