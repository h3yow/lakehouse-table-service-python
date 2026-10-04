"""湖仓表服务 HTTP API。

存储根目录通过环境变量 LAKEHOUSE_ROOT 指定，默认 ./lakehouse_data。
"""

import os
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from lakehouse.errors import LakehouseError
from lakehouse.models import Column, DeleteBatch, Schema, WriteBatch
from lakehouse.storage import LocalStore
from lakehouse.table import TableService

store = LocalStore(os.environ.get("LAKEHOUSE_ROOT", "./lakehouse_data"))
service = TableService(store)
app = FastAPI(title="lakehouse-table-service")


class ColumnIn(BaseModel):
    name: str
    type: str
    default: Any = None


class CreateTableIn(BaseModel):
    columns: list[ColumnIn]
    primary_keys: list[str] = []


class CommitIn(BaseModel):
    batch_id: str
    base_version: int
    partition: str
    mode: str = "append"
    rows: list[dict[str, Any]]


class DeleteIn(BaseModel):
    batch_id: str
    base_version: int
    partition: str
    keys: list[dict[str, Any]]


@app.exception_handler(LakehouseError)
def lakehouse_error_handler(_, exc: LakehouseError):
    # 每种错误类型对应一类可区分的失败原因
    return JSONResponse(status_code=409, content={"detail": f"{type(exc).__name__}: {exc}"})


@app.post("/tables/{name}", status_code=201)
def create_table(name: str, body: CreateTableIn):
    schema = Schema(
        columns=tuple(Column(i + 1, c.name, c.type, c.default) for i, c in enumerate(body.columns)),
        primary_keys=tuple(body.primary_keys),
    )
    service.create_table(name, schema)
    return {"table": name, "version": 0}


@app.get("/tables")
def list_tables():
    return {"tables": service.list_tables()}


@app.post("/tables/{name}/commits")
def commit(name: str, body: CommitIn):
    table = service.get_table(name)
    version = table.commit(WriteBatch(
        batch_id=body.batch_id,
        base_version=body.base_version,
        partition=body.partition,
        rows=tuple(body.rows),
        mode=body.mode,
    ))
    return {"table": name, "version": version}


@app.post("/tables/{name}/deletes")
def commit_delete(name: str, body: DeleteIn):
    table = service.get_table(name)
    version = table.commit_delete(DeleteBatch(
        batch_id=body.batch_id,
        base_version=body.base_version,
        partition=body.partition,
        keys=tuple(body.keys),
    ))
    return {"table": name, "version": version}


@app.get("/tables/{name}/rows")
def read(name: str, version: int | None = None, partition: str | None = None):
    table = service.get_table(name)
    return {"table": name, "version": table.get_version(version).version,
            "rows": table.read(version, partition)}


@app.post("/tables/{name}/compact")
def compact(name: str, partition: str):
    table = service.get_table(name)
    return {"table": name, "version": table.compact(partition)}


@app.post("/tables/{name}/schema/add-column")
def add_column(name: str, column: ColumnIn):
    table = service.get_table(name)
    return {"table": name, "version": table.add_column(column.name, column.type, column.default)}


@app.post("/tables/{name}/schema/drop-column")
def drop_column(name: str, column: str):
    return {"table": name, "version": service.get_table(name).drop_column(column)}


@app.post("/tables/{name}/schema/rename-column")
def rename_column(name: str, old: str, new: str):
    return {"table": name, "version": service.get_table(name).rename_column(old, new)}


@app.get("/tables/{name}/versions/{version}")
def get_version(name: str, version: int):
    return service.get_table(name).get_version(version).to_dict()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
