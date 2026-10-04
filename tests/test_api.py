"""HTTP API 冒烟：建表、提交、读取、演进、压实，错误可区分。"""

import logging

from fastapi.testclient import TestClient

from main import app

log = logging.getLogger("test.api")


def test_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path / "data"))
    # main 模块在导入时初始化存储，这里重建 client 前重载
    import importlib

    import main
    importlib.reload(main)
    client = TestClient(main.app)

    r = client.post("/tables/t", json={
        "columns": [{"name": "id", "type": "int"}, {"name": "val", "type": "str"}],
        "primary_keys": ["id"],
    })
    assert r.status_code == 201, r.text

    r = client.post("/tables/t/commits", json={
        "batch_id": "b1", "base_version": 0, "partition": "p=1", "mode": "upsert",
        "rows": [{"id": 1, "val": "a"}],
    })
    assert r.status_code == 200 and r.json()["version"] == 1, r.text

    # 幂等重放
    r2 = client.post("/tables/t/commits", json={
        "batch_id": "b1", "base_version": 0, "partition": "p=1", "mode": "upsert",
        "rows": [{"id": 1, "val": "a"}],
    })
    assert r2.json()["version"] == 1
    log.info("判定依据: 重放 batch=b1 返回原版本 v1，未产生新版本")

    # 相同 batch_id 不同内容 -> 409 且原因可区分
    r3 = client.post("/tables/t/commits", json={
        "batch_id": "b1", "base_version": 0, "partition": "p=1", "mode": "upsert",
        "rows": [{"id": 1, "val": "CHANGED"}],
    })
    assert r3.status_code == 409
    assert "DuplicateBatchError" in r3.json()["detail"]
    log.info("判定依据: 内容不同的重放 -> 409 DuplicateBatchError: %s", r3.json()["detail"])

    # 演进 + 快照读取
    client.post("/tables/t/schema/add-column", json={"name": "score", "type": "float", "default": 1.5})
    rows = client.get("/tables/t/rows").json()
    assert rows["rows"] == [{"id": 1, "val": "a", "score": 1.5}]
    old = client.get("/tables/t/rows", params={"version": 1}).json()
    assert old["rows"] == [{"id": 1, "val": "a"}]
    log.info("判定依据: 最新版本含缺省列, v1 快照不含新列: %s vs %s", rows["rows"], old["rows"])

    r = client.post("/tables/t/compact", params={"partition": "p=1"})
    assert r.status_code == 200
    assert client.get("/tables/t/rows").json()["rows"] == [{"id": 1, "val": "a", "score": 1.5}]


def test_delete_end_to_end(tmp_path, monkeypatch):
    """行级删除 HTTP 端到端：删除生效、历史可读、幂等、冲突原因可区分、无主键拒绝。"""
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path / "data"))
    import importlib

    import main
    importlib.reload(main)
    client = TestClient(main.app)

    client.post("/tables/t", json={
        "columns": [{"name": "id", "type": "int"}, {"name": "val", "type": "str"}],
        "primary_keys": ["id"],
    })
    client.post("/tables/t/commits", json={
        "batch_id": "b1", "base_version": 0, "partition": "p=1", "mode": "upsert",
        "rows": [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}],
    })

    # 删除 id=1（基于 v1）
    r = client.post("/tables/t/deletes", json={
        "batch_id": "d1", "base_version": 1, "partition": "p=1", "keys": [{"id": 1}],
    })
    assert r.status_code == 200 and r.json()["version"] == 2, r.text
    assert client.get("/tables/t/rows").json()["rows"] == [{"id": 2, "val": "b"}]
    # 删除前版本历史仍可见
    old = client.get("/tables/t/rows", params={"version": 1}).json()["rows"]
    assert old == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    log.info("判定依据: 删除后最新不含 id=1, v1 历史仍含 id=1: %s", old)

    # 幂等重放：不产生新版本
    r2 = client.post("/tables/t/deletes", json={
        "batch_id": "d1", "base_version": 1, "partition": "p=1", "keys": [{"id": 1}],
    })
    assert r2.json()["version"] == 2
    log.info("判定依据: 删除重放返回原版本 v2，未产生新版本")

    # 同 batch_id 不同内容 -> 409 DuplicateBatchError
    r3 = client.post("/tables/t/deletes", json={
        "batch_id": "d1", "base_version": 2, "partition": "p=1", "keys": [{"id": 2}],
    })
    assert r3.status_code == 409 and "DuplicateBatchError" in r3.json()["detail"]

    # 基于旧版本同分区删除 -> 409 DeleteConflictError（与普通冲突可区分）
    # 先让版本前进：v3 同分区写入
    client.post("/tables/t/commits", json={
        "batch_id": "b2", "base_version": 2, "partition": "p=1", "mode": "upsert",
        "rows": [{"id": 3, "val": "c"}],
    })
    r4 = client.post("/tables/t/deletes", json={
        "batch_id": "d2", "base_version": 2, "partition": "p=1", "keys": [{"id": 2}],
    })
    assert r4.status_code == 409
    assert "DeleteConflictError" in r4.json()["detail"]
    log.info("判定依据: 删除撞车 -> 409 DeleteConflictError: %s", r4.json()["detail"])

    # 分区不相交 -> 合并成功（p=2 与 v3 的 p=1 不相交）
    r5 = client.post("/tables/t/deletes", json={
        "batch_id": "d3", "base_version": 2, "partition": "p=2", "keys": [{"id": 9}],
    })
    assert r5.status_code == 200 and r5.json()["version"] == 4, r5.text
    log.info("判定依据: 分区不相交的删除 rebase 合并 -> v4")

    # 无主键表删除 -> 409 DeleteNotSupportedError
    client.post("/tables/np", json={"columns": [{"name": "id", "type": "int"}]})
    r6 = client.post("/tables/np/deletes", json={
        "batch_id": "x", "base_version": 0, "partition": "p=1", "keys": [{"id": 1}],
    })
    assert r6.status_code == 409 and "DeleteNotSupportedError" in r6.json()["detail"]
    log.info("判定依据: 无主键表删除 -> 409 DeleteNotSupportedError: %s", r6.json()["detail"])
