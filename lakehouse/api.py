"""HTTP API（FastAPI）：把湖仓表能力暴露为 REST 接口。

接口：
    POST   /tables                       建表
    GET    /tables                       表列表
    POST   /tables/{name}/append         分区增量写入（batch_id 幂等）
    POST   /tables/{name}/upsert         按主键 upsert
    POST   /tables/{name}/schema/evolve  结构演进（add/rename/drop）
    POST   /tables/{name}/compact        分区压实
    GET    /tables/{name}/read           快照读取（version / partition / columns）
    GET    /tables/{name}/history        已发布版本历史

失败原因与 HTTP 状态码的映射保证"可区分"：
    404 表/版本/列不存在；409 提交冲突；409+code=duplicate_batch 重复批次
    （返回既有版本，不产生新版本）；422 结构/数据校验错误；409 表已存在。
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .catalog import Catalog
from .errors import (
    ColumnNotFoundError,
    CommitConflictError,
    DuplicateBatchError,
    LakehouseError,
    SchemaValidationError,
    TableExistsError,
    TableNotFoundError,
    VersionNotFoundError,
)
from .schema import SchemaChange


class CreateTableRequest(BaseModel):
    name: str
    columns: list[tuple[str, str]]
    primary_key: tuple[str, ...] = ()
    partition_column: str | None = None


class WriteRequest(BaseModel):
    rows: list[dict[str, Any]]
    partition: str | None = None
    batch_id: str | None = None
    expected_version: int | None = None


class EvolveRequest(BaseModel):
    change: str = Field(description="add_column | rename_column | drop_column")
    name: str
    new_name: str | None = None
    dtype: str | None = None
    nullable: bool = True
    has_default: bool = False
    default_value: Any = None
    expected_version: int | None = None


class CompactRequest(BaseModel):
    partition: str | None = None
    expected_version: int | None = None


def create_app(catalog_root: str | None = None) -> FastAPI:
    root = catalog_root or os.environ.get("LAKEHOUSE_ROOT", "./lakehouse_data")
    catalog = Catalog(root)
    app = FastAPI(title="Local Lakehouse Table Service", version="0.1.0")

    # ---- 统一错误映射：错误类型即可区分的失败原因 ----
    @app.exception_handler(DuplicateBatchError)
    async def _dup(_req, exc: DuplicateBatchError):
        return JSONResponse(
            status_code=409,
            content={
                "error": "duplicate_batch",
                "message": str(exc),
                "existing_version": exc.existing_version,
            },
        )

    @app.exception_handler(CommitConflictError)
    async def _conflict(_req, exc: CommitConflictError):
        return JSONResponse(
            status_code=409,
            content={"error": "commit_conflict", "message": str(exc)},
        )

    @app.exception_handler(TableExistsError)
    async def _exists(_req, exc: TableExistsError):
        return JSONResponse(
            status_code=409, content={"error": "table_exists", "message": str(exc)}
        )

    @app.exception_handler(TableNotFoundError)
    @app.exception_handler(VersionNotFoundError)
    @app.exception_handler(ColumnNotFoundError)
    async def _not_found(_req, exc: LakehouseError):
        return JSONResponse(
            status_code=404,
            content={"error": type(exc).__name__, "message": str(exc)},
        )

    @app.exception_handler(SchemaValidationError)
    async def _invalid(_req, exc: SchemaValidationError):
        return JSONResponse(
            status_code=422,
            content={"error": "schema_validation", "message": str(exc)},
        )

    # ---- 表管理 ----
    @app.post("/tables")
    def create_table(req: CreateTableRequest):
        table = catalog.create_table(
            req.name,
            list(req.columns),
            primary_key=req.primary_key,
            partition_column=req.partition_column,
        )
        return {"table": table.name, "version": 0}

    @app.get("/tables")
    def list_tables():
        return {"tables": catalog.list_tables()}

    # ---- 写入 ----
    @app.post("/tables/{name}/append")
    def append(name: str, req: WriteRequest):
        v = catalog.open_table(name).append(
            req.rows,
            partition=req.partition,
            batch_id=req.batch_id,
            expected_version=req.expected_version,
        )
        return {"table": name, "version": v}

    @app.post("/tables/{name}/upsert")
    def upsert(name: str, req: WriteRequest):
        v = catalog.open_table(name).upsert(
            req.rows,
            partition=req.partition,
            batch_id=req.batch_id,
            expected_version=req.expected_version,
        )
        return {"table": name, "version": v}

    # ---- 结构演进 ----
    @app.post("/tables/{name}/schema/evolve")
    def evolve(name: str, req: EvolveRequest):
        change = SchemaChange(
            kind=req.change,
            name=req.name,
            new_name=req.new_name,
            dtype=req.dtype,
            nullable=req.nullable,
            has_default=req.has_default,
            default_value=req.default_value,
        )
        v = catalog.open_table(name).evolve_schema(
            change, expected_version=req.expected_version
        )
        return {"table": name, "version": v}

    # ---- 压实 ----
    @app.post("/tables/{name}/compact")
    def compact(name: str, req: CompactRequest):
        v = catalog.open_table(name).compact(
            partition=req.partition, expected_version=req.expected_version
        )
        return {"table": name, "version": v}

    # ---- 读取 ----
    @app.get("/tables/{name}/read")
    def read(
        name: str,
        version: int | None = None,
        partition: str | None = None,
        columns: str | None = None,
    ):
        col_list = [c.strip() for c in columns.split(",")] if columns else None
        rows = catalog.open_table(name).read(
            version=version, partition=partition, columns=col_list
        )
        return {"table": name, "version": version, "rows": rows}

    @app.get("/tables/{name}/history")
    def history(name: str):
        snaps = catalog.open_table(name).history()
        return {
            "table": name,
            "history": [
                {
                    "version": s.version,
                    "schema_id": s.schema_id,
                    "parent_version": s.parent_version,
                    "operation": s.operation,
                    "partition": s.partition,
                    "batch_id": s.batch_id,
                    "added": len(s.added_files),
                    "removed": len(s.removed_files),
                }
                for s in snaps
            ],
        }

    return app


app = create_app()
