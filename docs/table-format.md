# 本地湖仓表格式约定

本服务用本地文件系统模拟一套小型湖仓表格式（Lakehouse Table Format），
在**同一份元数据**下给出并发写入、结构演进与历史查询的一致结果。

## 1. 目录布局

每张表是根目录下的一个子目录：

```
<LAKEHOUSE_ROOT>/<table>/
├── metadata/
│   ├── _table.json          # 表定义与当前指针（见下）
│   ├── lock                 # 提交互斥锁（flock，文件预先创建）
│   ├── v-0.json             # 初始空快照（携带初始 schema）
│   ├── v-1.json             # 每次提交一个不可变快照清单
│   └── v-<n>.json
└── data/
    └── <uuid>.json          # 不可变数据文件，行按 column_id 存储
```

`_table.json` 内容：

```json
{
  "name": "events",
  "partition_column": "p",
  "primary_key_columns": ["id"],
  "primary_key_ids": [0],
  "current_version": 3,
  "current_schema_id": 2,
  "batch_index": {"b1": {"version": 1, "operation": "append", "partition": "a"}}
}
```

快照清单 `v-<n>.json` 记录：版本号、父版本、操作类型
（`append` / `upsert` / `compaction` / `schema_change`）、涉及分区、
`added_files`、`removed_files`、批次 `batch_id`、压实基版本
`compaction_of_version`，以及**该版本发布时刻的完整 schema**。

## 2. 列身份：column id 而不是列名

- 列以全表稳定、单调、**永不复用**的整数 `column_id` 标识；
- 数据文件里的行按 `column_id` 存储（字符串键的 JSON 对象），
  因此改名不改数据位置，删列只是在 schema 中标记 `dropped`；
- 快照内嵌发布时刻的完整 schema，旧版本永远按旧结构解释。

数据文件示例：

```json
{
  "schema_id": 0,
  "partition": "a",
  "rows": [{"0": 1, "1": "a", "2": 10}]
}
```

## 3. 写入模型

- **append（分区增量）**：一个批次只允许落在一个分区（行内分区列决定，
  跨分区批次直接拒绝）。校验后写一个不可变数据文件，发布新版本。
- **upsert（按主键）**：表必须声明主键。采用 copy-on-write：读取该分区当前
  live 文件，同主键旧行被整行替换，未命中文件原样保留，受影响文件中的存活行
  与全部新行合并为重写文件，旧文件进入新版本的 `removed_files`。
- 空批次、类型不匹配、未知列、主键为 NULL、同批主键重复都属于
  `SchemaValidationError`（HTTP 422），绝不静默忽略。

## 4. 并发提交、幂等与冲突规则

提交临界区为 `flock` 表锁内的
"读当前版本 → 冲突判定 → 写快照 → 推进 `_table.json` 指针"，
对同表提交严格串行；元数据与快照都用"临时文件 + `os.replace`"原子落盘。

| 场景 | 规则 | 结果 |
| --- | --- | --- |
| 相同 `batch_id` 重放 | `batch_index` 命中即幂等 | 不产生新版本/新数据，返回/抛出既有版本（HTTP 409 `duplicate_batch`） |
| append vs append | 结果集天然为并集，既定合并规则 | 都接受；`expected_version` 过期也排队合并 |
| upsert vs 同分区提交 | 客户端基版本之后该分区被任何提交触碰 | `CommitConflictError`（HTTP 409 `commit_conflict`），拒绝并指明冲突版本，需基于新版本重试 |
| upsert vs 其他分区 | 分区不重叠 | 不冲突，正常接受 |
| compaction / schema_change | 要求 `expected_version` 等于当前版本 | 过期即冲突拒绝，不与并发提交静默交错 |
| `expected_version` > 当前版本 | 基版本尚不存在 | 冲突拒绝 |

冲突判定只依赖已发布快照，因此在任何机器/任何时刻重放结论一致、可复现。

## 5. 结构演进规则

演进占用一个普通版本（`schema_change`），不触碰数据文件：

- **新增列**：必须 `nullable` 或声明常量默认值。该默认值写入列定义且不可变：
  历史行读取时确定性地取默认值，否则取 NULL。旧版本快照不携带该列，
  旧版本查询结果永远不变。
- **改名**：只改列名，`column_id` 不变。历史版本按旧名读取，当前版本按新名读取；
  当前版本用旧名查询会得到 `ColumnNotFoundError`（HTTP 404），不会静默映射到其他列。
- **删列**：逻辑删除（`dropped=true`）。历史快照照常返回该列；当前版本不返回，
  显式投影该列明确报错。被删 id 不会被后续新列复用。
- 重名新增、改成已存在列名等有歧义的演进一律拒绝。

## 6. 快照与时间旅行

读取 `version=n` 时，从 v0 到 vn **顺序回放清单**构造 live 文件集：

```
live = {}
for v in 0..n:
    live += snapshot(v).added_files
    live -= snapshot(v).removed_files
```

- 只能看到截至该版本已提交的数据；未发布版本抛 `VersionNotFoundError`；
- 行按该版本内嵌的 schema 解释：新增列按默认值/NULL 补齐，改名列按当时名字返回，
  已删列不返回；
- 列投影（`columns`）包含当时不存在的列时抛 `ColumnNotFoundError`，
  绝不静默返回错误结果；
- 返回顺序按"文件名升序 + 文件内行序"稳定排列，同一版本重复读取可复现。

## 7. 压实（Compaction）语义

- 分区压实把一个分区当前 live 文件合并为一个重写文件；全表压实对每个分区各产出一个；
- 压实发布新版本：旧文件进入新版本 `removed_files`，**物理文件永不删除**
  （`FileStorage.delete_data_file` 被显式禁用），因此对任意 `n ≤ 压实版本`
  回放清单时，压实的 `removed_files` 不会进入版本 n 的 live 集——
  **压实不可能改变任何已发布版本的可见结果**；
- 重写行投影到压实时刻的 schema（新增列补默认值），列数据按 id 对齐，不会错位；
- 基版本过期、分区无 live 文件均拒绝压实。

## 8. 判定日志

`lakehouse.commit` logger 为每次接受/重复/冲突/拒绝输出单行 JSON，字段：
`table, operation, base_version, candidate_version, partition, batch_id,
outcome, reason, conflicting_version`。失败结论带具体冲突版本号与判定依据，
测试通过 caplog 断言这些字段，保证结论可复现、失败原因可区分。
