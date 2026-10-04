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
- **行级删除**：`delete` 按主键删除。批次的 `rows` 只含主键列，表示要删除
  的主键集合；提交时重写该分区文件（去掉命中主键的行）产生新版本，
  旧文件不删，因此删除前的历史版本仍读到当时的样子。删除不存在的 主键
  或对空分区删除是确定的无害结果（数据不变）。删除过的主键之后可以
  正常写回（upsert/append），作为一条新行重新出现。仅在声明了主键的表
  上可用，否则以 `ValidationError` 明确拒绝；删除批次只允许主键列，
  缺主键、带非主键列、类型不符都以 `ValidationError` 拒绝。

## 并发与幂等规则

- **幂等键**：每个写入批次携带 `batch_id`。重复提交**相同内容**返回原版本号，
  不产生重复数据或新版本；相同 `batch_id` 提交**不同内容**抛出
  `DuplicateBatchError`。
- **乐观并发**：提交携带 `base_version`（调用方读取数据时基于的版本）。
  若当前版本已前进，检查期间已提交的写版本（append/upsert/delete）触及的
  分区——即其新增文件（含重写分区产生的新文件）所在的分区：
  - 与本批次分区**不相交** → 按既定规则 rebase 合并到最新版本，提交成功；
  - **相交** → 抛出 `CommitConflictError`（消息含冲突版本与分区），
    绝不静默覆盖。客户端读取最新版本后重试即可。
  删除与写入适用完全相同的规则：删除撞同分区写提交会被拒绝，反之亦然；
  冲突重试基于最新版本重新判定，最终结论稳定、可复现。
- 压实与结构演进不参与写冲突判定（压实不改变可见数据，演进只改 schema）。
- 所有提交在表级锁内串行判定，因此并发结论可复现：同分区并发提交最终
  全部成功（经冲突重试）且版本号为 1..N 的排列。

## 快照与压实语义

- `read(version=N)` 只能看到 vN 元数据中列出的已提交文件，按 vN 的 schema
  投影；`version=None` 读最新版本。读取未提交的版本抛 `VersionNotFoundError`。
- `compact(partition)` 把该分区的多个数据文件合并为一个，**只产生新版本，
  不删除旧文件**，因此任何已发布版本的可见结果不变。压实提交前会显式校验
  压实前后最新版本的可见行完全一致，不一致即失败。删除之后压实同样安全：
  压实只合并当前版本文件清单里的文件，已删除的行不在其中，不会被带回来。
- 删除与结构演进并存：删除不重写历史数据文件，删列/改名只改 schema，
  删除前后的历史版本仍按各自版本的 schema 投影读取，不会错位。
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
| POST | `/tables/{name}/commits` | 提交批次（batch_id/base_version/partition/mode/rows）；mode 为 append/upsert/delete，delete 时 rows 只含主键列 |
| GET | `/tables/{name}/rows?version=&partition=` | 快照读取 |
| POST | `/tables/{name}/compact?partition=` | 分区压实 |
| POST | `/tables/{name}/schema/add-column` `/drop-column` `/rename-column` | 结构演进 |
| GET | `/tables/{name}/versions/{v}` | 查看版本元数据 |

删除示例（HTTP）：

```bash
curl -X POST localhost:8000/tables/t/commits -H 'content-type: application/json' -d '{
  "batch_id": "d1", "base_version": 1, "partition": "p=1",
  "mode": "delete", "rows": [{"id": 1}]
}'
```

所有领域错误以 HTTP 409 返回，`detail` 前缀为错误类型名
（`CommitConflictError` / `DuplicateBatchError` / `ValidationError` / ...），
失败原因可区分。

## 验证方法

```bash
uv run pytest            # 全部测试，日志实时打印版本、分区与判定依据
uv run pytest tests/test_concurrency.py -q   # 只跑并发场景
uv run pytest tests/test_delete.py -q        # 只跑删除场景
```

Python 接口删除示例：

```python
from lakehouse.models import WriteBatch
table.commit(WriteBatch("d1", base_version=1, partition="p=1",
                        rows=({"id": 1},), mode="delete"))
```

测试覆盖：

- `test_commit.py`：幂等重放、重复批次（同内容/不同内容）、upsert 覆盖序、
  同分区冲突拒绝、不相交分区合并、append 主键约束、列校验；
- `test_delete.py`：删除后最新快照消失而历史版本不变、删除主键写回、
  删除幂等重放与同 batch_id 不同内容拒绝、删除不存在主键/空分区的确定结果、
  无主键表与非法删除批次的明确拒绝、删除与同分区并发提交撞车拒绝与重试、
  不相交分区合并、多线程删除+写入的最终一致性、删除后压实不带回行且
  已发布版本逐一比对不变、删除后改名/删列历史版本不错位；
- `test_concurrency.py`：8 线程同分区并发提交（版本号为 1..N 排列、无重复
  无丢失）、不相交分区零重试合并、提交与快照读交错 200 次零不一致；
- `test_schema_evolution.py`：新增列缺省语义、删列/改名后历史数据不错位、
  旧版本快照 schema 不变、非法演进被拒绝；
- `test_snapshot_compact.py`：快照只见已提交数据、压实前后所有已发布版本
  逐一比对一致、压实/提交/读取交错可复现；
- `test_api.py`：HTTP 端到端冒烟（含删除）、错误可区分性。

一致性结论可复现：所有并发判定在表级锁内串行进行，测试断言最终行集合与
版本序列而非线程调度顺序。
