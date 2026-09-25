# 本地湖仓表服务（Python）

用本地存储模拟的湖仓表能力：**分区增量写入、按主键 upsert、表结构演进、
快照时间旅行与分区压实**，在同一份元数据下提供一致、可解释、可复现的结果。

格式约定与语义细节见 [`docs/table-format.md`](docs/table-format.md)。

## 能力概览

- **增量写入**：按分区 append，单批次不允许跨分区；
- **幂等提交**：`batch_id` 去重，重复提交同一批次返回同一个既有版本，
  不产生重复数据或重复版本；
- **按主键 upsert**：copy-on-write 重写受影响文件；基版本之后同分区被触碰则
  明确 `CommitConflictError` 拒绝，绝不静默覆盖；append 之间按并集合并；
- **结构演进**：新增列有确定的常量默认值/NULL 缺省语义；改名不改列 id；
  删列逻辑删除；历史版本按发布时刻的 schema 解释，查询旧列/已删列明确报错；
- **快照读取**：按版本或最新版本读取，只能看到该版本已提交数据；
- **分区压实**：live 文件合并、发布新版本，旧文件物理保留，
  任一已发布版本的可见结果不变；
- **可解释性**：提交锁（`flock`）串行化提交，判定过程输出结构化日志
  （版本、分区、批次、outcome、判定依据）。

## 快速开始

```bash
uv sync
# 运行服务（数据根目录可用 LAKEHOUSE_ROOT 覆盖，默认 ./lakehouse_data）
uv run python main.py
```

```bash
# 建表
curl -s -X POST localhost:8000/tables -d '{
  "name": "events",
  "columns": [["id","long"],["p","string"],["v","int"]],
  "primary_key": ["id"],
  "partition_column": "p"}' -H 'Content-Type: application/json'

# 幂等写入（重复同样的 batch_id 不会产生第二版本）
curl -s -X POST localhost:8000/tables/events/append -d '{
  "rows":[{"id":1,"p":"a","v":10}], "batch_id":"b1"}' \
  -H 'Content-Type: application/json'

# 时间旅行 / 最新读取
curl -s 'localhost:8000/tables/events/read?version=1'
curl -s 'localhost:8000/tables/events/read'

# 结构演进（新增带默认值列）
curl -s -X POST localhost:8000/tables/events/schema/evolve -d '{
  "change":"add_column","name":"region","dtype":"string",
  "has_default":true,"default_value":"cn"}' -H 'Content-Type: application/json'

# 分区压实
curl -s -X POST localhost:8000/tables/events/compact -d '{"partition":"a"}' \
  -H 'Content-Type: application/json'

# 版本历史
curl -s localhost:8000/tables/events/history
```

### Python API

```python
from lakehouse.catalog import Catalog

catalog = Catalog("./lakehouse_data")
t = catalog.create_table(
    "events",
    [("id", "long"), ("p", "string"), ("v", "int")],
    primary_key=("id",),
    partition_column="p",
)
t.append([{"id": 1, "p": "a", "v": 10}], batch_id="b1")
t.upsert([{"id": 1, "p": "a", "v": 11}], batch_id="u1")
t.read(version=1)          # [{'id': 1, 'p': 'a', 'v': 10}]
t.read()                   # [{'id': 1, 'p': 'a', 'v': 11}]
```

## 失败原因区分

| 错误 | 类型 | HTTP |
| --- | --- | --- |
| 重复批次（返回既有版本） | `DuplicateBatchError` | 409 `duplicate_batch` |
| 提交冲突（需重试） | `CommitConflictError` | 409 `commit_conflict` |
| 表/版本/列不存在 | `TableNotFoundError` / `VersionNotFoundError` / `ColumnNotFoundError` | 404 |
| 数据或演进不合法 | `SchemaValidationError` | 422 |
| 表已存在 | `TableExistsError` | 409 |

## 验证方法

```bash
uv run pytest -q          # 全部 25 个用例
```

测试覆盖：

- `tests/test_concurrent_commits.py`：多线程并发 append（无重复行/版本）、
  同批次并发幂等、upsert 冲突拒绝、append 并集合并、失败类型区分、判定日志断言；
- `tests/test_schema_evolution.py`：新增列缺省语义、改名列 id 稳定、
  删列逻辑删除、历史不错位、演进串行化、重写文件补列；
- `tests/test_snapshot_compaction.py`：时间旅行、压实前后所有历史版本逐行一致、
  旧文件物理保留、分区隔离、压实与提交并发交错、读者只见已发布版本、历史清单；
- `tests/test_api.py`：REST 接口与错误码映射。

日志示例（判定依据包含版本、分区、批次）：

```
commit-decision {"base_version": 1, "batch_id": "u2", "candidate_version": null,
 "conflicting_version": 2, "operation": "upsert", "outcome": "conflict",
 "partition": "a", "reason": "partition touched by v2 after client base; ...",
 "table": "events"}
```

## 目录结构

```
lakehouse/
├── errors.py    # 可区分的异常类型
├── schema.py    # 列/schema 定义与结构演进（column id 身份）
├── storage.py   # 原子文件存储、快照清单、flock 提交锁
├── commit.py    # 结构化提交判定日志
├── table.py     # append/upsert/evolve/compact/read 核心协议
├── catalog.py   # 表目录
└── api.py       # FastAPI REST 接口
main.py          # 服务入口
tests/           # 并发/演进/快照压实/API 测试
docs/            # 表格式与语义文档
```
