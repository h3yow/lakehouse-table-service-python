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


def test_delete_over_http(tmp_path, monkeypatch):
    """HTTP 接口的删除：与 Python 接口语义一致，失败原因可区分。"""
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
    # 删除 id=1
    r = client.post("/tables/t/commits", json={
        "batch_id": "d1", "base_version": 1, "partition": "p=1", "mode": "delete",
        "rows": [{"id": 1}],
    })
    assert r.status_code == 200 and r.json()["version"] == 2, r.text
    latest = client.get("/tables/t/rows").json()["rows"]
    history = client.get("/tables/t/rows", params={"version": 1}).json()["rows"]
    log.info("判定依据: HTTP 删除后 最新=%s, 历史 v1=%s", latest, history)
    assert latest == [{"id": 2, "val": "b"}]
    assert history == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]

    # 幂等重放同一删除
    r = client.post("/tables/t/commits", json={
        "batch_id": "d1", "base_version": 1, "partition": "p=1", "mode": "delete",
        "rows": [{"id": 1}],
    })
    assert r.json()["version"] == 2

    # 基于旧版本的删除与同分区已落地提交撞车 -> 409 CommitConflictError
    client.post("/tables/t/commits", json={
        "batch_id": "b2", "base_version": 2, "partition": "p=1", "mode": "upsert",
        "rows": [{"id": 3, "val": "c"}],
    })
    r = client.post("/tables/t/commits", json={
        "batch_id": "d2", "base_version": 2, "partition": "p=1", "mode": "delete",
        "rows": [{"id": 2}],
    })
    assert r.status_code == 409
    assert "CommitConflictError" in r.json()["detail"]
    log.info("判定依据: 删除撞车 -> 409 CommitConflictError: %s", r.json()["detail"])

    # 删除批次带非主键列 -> 409 ValidationError（原因可区分）
    r = client.post("/tables/t/commits", json={
        "batch_id": "d3", "base_version": 3, "partition": "p=1", "mode": "delete",
        "rows": [{"id": 2, "val": "x"}],
    })
    assert r.status_code == 409
    assert "ValidationError" in r.json()["detail"]
    log.info("判定依据: 删除含非主键列 -> 409 ValidationError: %s", r.json()["detail"])
