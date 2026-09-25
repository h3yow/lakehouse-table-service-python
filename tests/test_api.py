"""HTTP API 冒烟测试：错误原因 -> 状态码/错误码可区分。"""

from __future__ import annotations

import pytest

# 让每个 app 使用独立目录
os_environ = pytest.MonkeyPatch


@pytest.fixture
def client(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from lakehouse.api import create_app

    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    return TestClient(create_app(str(tmp_path)))


def _create(client, name="events"):
    r = client.post(
        "/tables",
        json={
            "name": name,
            "columns": [["id", "long"], ["p", "string"], ["v", "int"]],
            "primary_key": ["id"],
            "partition_column": "p",
        },
    )
    assert r.status_code == 200, r.text


def test_api_happy_path_and_error_mapping(client):
    _create(client)
    r = client.post(
        "/tables/events/append",
        json={"rows": [{"id": 1, "p": "a", "v": 1}], "batch_id": "b1"},
    )
    assert r.status_code == 200 and r.json()["version"] == 1

    # 重复批次：409 + 明确错误码 + 既有版本
    r = client.post(
        "/tables/events/append",
        json={"rows": [{"id": 2, "p": "a", "v": 2}], "batch_id": "b1"},
    )
    assert r.status_code == 409
    assert r.json()["error"] == "duplicate_batch"
    assert r.json()["existing_version"] == 1

    # upsert 冲突：409 + 不同错误码
    r = client.post(
        "/tables/events/upsert",
        json={"rows": [{"id": 1, "p": "a", "v": 9}], "expected_version": 0},
    )
    assert r.status_code == 409 and r.json()["error"] == "commit_conflict"

    # 数据校验错误：422
    r = client.post(
        "/tables/events/append",
        json={"rows": [{"id": "x", "p": "a", "v": 1}], "batch_id": "b2"},
    )
    assert r.status_code == 422 and r.json()["error"] == "schema_validation"

    # 不存在的表 / 版本 / 列：404
    assert client.get("/tables/nope/read").status_code == 404
    assert client.get("/tables/events/read", params={"version": 99}).status_code == 404
    assert client.get("/tables/events/read", params={"columns": "ghost"}).status_code == 404


def test_api_evolve_compact_read_history(client):
    _create(client)
    client.post(
        "/tables/events/append",
        json={"rows": [{"id": 1, "p": "a", "v": 1}], "batch_id": "b1"},
    )
    r = client.post(
        "/tables/events/schema/evolve",
        json={
            "change": "add_column",
            "name": "region",
            "dtype": "string",
            "has_default": True,
            "default_value": "cn",
        },
    )
    assert r.status_code == 200
    r = client.get("/tables/events/read")
    assert r.json()["rows"] == [{"id": 1, "p": "a", "v": 1, "region": "cn"}]
    r = client.post("/tables/events/compact", json={"partition": "a"})
    assert r.status_code == 200
    r = client.get("/tables/events/history")
    assert [h["operation"] for h in r.json()["history"]] == [
        "append",
        "schema_change",
        "compaction",
    ]
