# 本地湖仓表服务

用本地文件系统模拟湖仓表的数据与元数据，在同一份元数据下让并发写入、结构演进
与历史查询给出一致且可解释的结果。提供 Python API（`lakehouse` 包）与
HTTP API（`main.py`，FastAPI）。

## 表格式约定

存储根目录（默认 `./lakehouse_data`，可用环境变量 `LAKEHOUSE_ROOT` 覆盖）：

```
<root>/<table>/
    metadata/
        table.json              # 表级元数据：当前版本号、下一列 id、文件序号、已提交批次指纹
        versions/v<N>.json      # 每个版本的快照：schema、可见文件清单、已提交批次集合
    data/<partition>/<file>.json  # 数据文件：行以 {列id: 值} 存储
```

核心约定：

- **列以稳定 id 标识**。数据文件按列 id 存值，列名只是当前 schema 中的展示名。
  改名只改名字，删列只从 schema 移除条目，历史数据文件不受影响、不会错位。
- **版本即快照**。每次提交（写入/压实/演进）产生一个单调递增的版本，
  `versions/v<N>.json` 完整记录该版本可见的文件清单与当时的 schema。
  旧版本的元数据与数据文件永不修改。
- **写文件原子化**。所有文件先写临时文件再 `rename`，读取方不会看到半截文件。
- **行级规则**：`append` 追加新文件；`upsert` 按主键在分区内合并
  （读取既有分区数据、按主键覆盖、写出一个新文件，版本序即覆盖序）。
  主键表上 `append` 拒绝与既有数据或批内重复的主键，避免静默产生重复。
- **行级删除**：`delete` 按主键在分区内移除行（读取既有分区数据、剔除命中
  主键、写出一个新文件），只有声明了主键的表支持；删除只让**新版本之后**
  的快照看不见这些行，删除之前的历史版本原样可读。删除后用同一主键写回
  会被当作一条正常的新行重新出现。
- **版本元数据增加 `partition`**：每个写/删版本记录其触及的分区，并发冲突
  据此判定；早于该字段产生的旧版本文件按“相对上一版本新增文件”回退判定，
  无需迁移。

## 并发与幂等规则

- **幂等键**：每个写入批次携带 `batch_id`。重复提交**相同内容**返回原版本号，
  不产生重复数据或新版本；相同 `batch_id` 提交**不同内容**抛出
  `DuplicateBatchError`。
- **乐观并发**：提交携带 `base_version`（调用方读取数据时基于的版本）。
  若当前版本已前进，检查期间已提交的写版本触及的分区：
  - 与本批次分区**不相交** → 按既定规则 rebase 合并到最新版本，提交成功；
  - **相交** → 抛出 `CommitConflictError`（消息含冲突版本与分区），
    绝不静默覆盖。客户端读取最新版本后重试即可。
- 压实与结构演进不参与写冲突判定（压实不改变可见数据，演进只改 schema）。
- 所有提交在表级锁内串行判定，因此并发结论可复现：同分区并发提交最终
  全部成功（经冲突重试）且版本号为 1..N 的排列。

## 行级删除语义

- **用法（Python）**：`table.commit_delete(DeleteBatch(batch_id, base_version,
  partition, keys))`，`keys` 是若干 `{主键列名: 值}`（联合主键就给全所有
  主键列），返回新版本号。
- **用法（HTTP）**：`POST /tables/{name}/deletes`，请求体
  `{"batch_id", "base_version", "partition", "keys": [{"id": 1}]}`。
- **时间旅行**：删除产生一个 `operation="delete"` 的新版本；该版本及其后
  看不到被删行，**删除提交之前的版本读到的仍是当时的行**——旧版本元数据
  与数据文件永不修改，删除不会改写历史。
- **可写回**：删除生效后用同一主键 upsert/append，行作为正常行重新出现；
  历史版本各自保留当时的样子，互不影响。
- **幂等**：相同 `batch_id` + 相同删除内容（含分区与主键集合）重放返回原
  版本号，不报错、不产生新版本或额外数据文件；相同 `batch_id` 内容不同
  抛 `DuplicateBatchError`。
- **确定性 no-op**：删除一批当时不存在（或已被删除）的主键不报错，存在的
  行原样保留；混合批次里存在的键正常删除、不存在的忽略；空批次同样安全。
  删除永远不会把数据改坏。
- **并发规则与普通写入完全一致**：携带 `base_version`，与同分区其它写/删
  提交撞车时抛 `DeleteConflictError`（是 `CommitConflictError` 的子类，
  客户端可沿用同一套“重读最新版本再提交”的重试逻辑；类型名让删除冲突与
  普通写入冲突在失败原因上可区分）；分区不相交时按既定规则 rebase 合并。
  冲突后重读最新版本重试，最终结论稳定、可复现。
- **能力边界**：未声明主键的表调用删除明确抛 `DeleteNotSupportedError`
  （HTTP 409），不返回含糊结果；删除键必须恰好包含全部主键列且类型正确，
  否则抛 `ValidationError`。

### 与压实、结构演进的兼容边界

- **压实不复活、不改历史**：压实只合并最新版本可见文件、只产生新版本且不
  删除旧文件。已删除的行不在可见文件中，压实前后每个已发布版本（含删除
  前的版本）的可见结果逐一比对一致；压实之后再删除、删除之后再压实，结论
  都稳定。
- **结构演进不错位**：删除产生的历史版本仍按各版本自己的 schema 投影。
  之后新增列、改名（含主键列改名，列 id 不变）、删列，都不会让删除前历史
  版本读出的行列错位；演进后按新主键名删除、写回同样正常工作。主键列仍不
  允许删除。

## 快照与压实语义

- `read(version=N)` 只能看到 vN 元数据中列出的已提交文件，按 vN 的 schema
  投影；`version=None` 读最新版本。读取未提交的版本抛 `VersionNotFoundError`。
- `compact(partition)` 把该分区的多个数据文件合并为一个，**只产生新版本，
  不删除旧文件**，因此任何已发布版本的可见结果不变。压实提交前会显式校验
  压实前后最新版本的可见行完全一致，不一致即失败。
- 并发提交、压实与读取交错时：读取总是基于某个已发布版本的完整文件清单，
  结果可解释、可复现（见 `tests/test_snapshot_compact.py`）。

## 结构演进规则

- **新增列**：必须声明类型，可带缺省值（缺省为 `None`）。历史数据文件没有
  该列 id，读取时以缺省值补齐——缺省语义确定且稳定，不随后续写入改变。
- **删除列**：只从当前 schema 移除，历史数据文件中的列 id 保留但不再投影；
  历史版本仍按当时的 schema 读取，不会错位。主键列不可删除。
- **改名**：列 id 不变，历史数据不错位；主键名同步更新。改名后用旧名写入
  会被校验拒绝（`ValidationError`），不会静默吞掉。
- 每次演进产生一个新版本，记录新 schema；旧版本快照的 schema 不变，
  因此既有查询不会静默返回错误结果。

## HTTP API

```bash
uv run python main.py   # 或 uvicorn main:app
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tables/{name}` | 建表（columns + primary_keys） |
| POST | `/tables/{name}/commits` | 提交批次（batch_id/base_version/partition/mode/rows） |
| POST | `/tables/{name}/deletes` | 按主键删除批次（batch_id/base_version/partition/keys） |
| GET | `/tables/{name}/rows?version=&partition=` | 快照读取 |
| POST | `/tables/{name}/compact?partition=` | 分区压实 |
| POST | `/tables/{name}/schema/add-column` `/drop-column` `/rename-column` | 结构演进 |
| GET | `/tables/{name}/versions/{v}` | 查看版本元数据 |

所有领域错误以 HTTP 409 返回，`detail` 前缀为错误类型名
（`CommitConflictError` / `DeleteConflictError` / `DuplicateBatchError`
/ `DeleteNotSupportedError` / `ValidationError` / ...），失败原因可区分。

## 验证方法

```bash
uv run pytest            # 全部测试，日志实时打印版本、分区与判定依据
uv run pytest tests/test_concurrency.py -q   # 只跑并发场景
uv run pytest tests/test_delete.py -q        # 只跑行级删除场景
```

也可以手动起服务验证 HTTP 接口：

```bash
LAKEHOUSE_ROOT=./lakehouse_data uv run uvicorn main:app --port 8000
# 建主键表
curl -s -X POST localhost:8000/tables/demo -H 'content-type: application/json' \
  -d '{"columns":[{"name":"id","type":"int"},{"name":"val","type":"str"}],"primary_keys":["id"]}'
# 写两行
curl -s -X POST localhost:8000/tables/demo/commits -H 'content-type: application/json' \
  -d '{"batch_id":"b1","base_version":0,"partition":"p=1","mode":"upsert","rows":[{"id":1,"val":"a"},{"id":2,"val":"b"}]}'
# 删 id=1
curl -s -X POST localhost:8000/tables/demo/deletes -H 'content-type: application/json' \
  -d '{"batch_id":"d1","base_version":1,"partition":"p=1","keys":[{"id":1}]}'
curl -s localhost:8000/tables/demo/rows                      # 最新只剩 id=2
curl -s 'localhost:8000/tables/demo/rows?version=1'         # v1 历史仍有 id=1
```

Python 接口对照：

```python
from lakehouse.models import Schema, Column, WriteBatch, DeleteBatch
from lakehouse.storage import LocalStore
from lakehouse.table import TableService

t = TableService(LocalStore("./lakehouse_data")).create_table(
    "demo", Schema((Column(1, "id", "int"), Column(2, "val", "str")), ("id",)))
t.commit(WriteBatch("b1", 0, "p=1", ({"id": 1, "val": "a"}, {"id": 2, "val": "b"}), "upsert"))
t.commit_delete(DeleteBatch("d1", 1, "p=1", ({"id": 1},)))   # 删
t.read()                    # 最新：id=2
t.read(version=1)           # 历史：id=1、id=2 都在
```

测试覆盖：

- `test_commit.py`：幂等重放、重复批次（同内容/不同内容）、upsert 覆盖序、
  同分区冲突拒绝、不相交分区合并、append 主键约束、列校验；
- `test_concurrency.py`：8 线程同分区并发提交（版本号为 1..N 排列、无重复
  无丢失）、不相交分区零重试合并、提交与快照读交错 200 次零不一致；
- `test_schema_evolution.py`：新增列缺省语义、删列/改名后历史数据不错位、
  旧版本快照 schema 不变、非法演进被拒绝；
- `test_snapshot_compact.py`：快照只见已提交数据、压实前后所有已发布版本
  逐一比对一致、压实/提交/读取交错可复现；
- `test_delete.py`：删除后最新不可见但删除前历史可读、同主键写回复现、
  分区隔离与联合主键、删除幂等重放（无新版本/无文件痕迹）、同 batch_id
  内容不一致拒绝、删除不存在/混合/空批次的确定性 no-op、删除与写入双向
  撞车的拒绝（`DeleteConflictError` 与 `CommitConflictError` 可区分）、
  冲突重读重试结论稳定、5 删除+5 写入同分区并发（版本 1..N 排列、结论
  确定）、无主键表 `DeleteNotSupportedError`、删除键校验、删除后压实不
  复活且各历史版本不变、压实-删除-再压实稳定、删除后加列/改名/删列历史
  仍按各版本 schema 读出且可按新主键名继续删除/写回；
- `test_api.py`：HTTP 端到端冒烟、删除接口端到端（生效/历史/幂等/冲突/
  不相交合并/无主键拒绝）与错误可区分性。

一致性结论可复现：所有并发判定在表级锁内串行进行，测试断言最终行集合与
版本序列而非线程调度顺序。
