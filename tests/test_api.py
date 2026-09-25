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
