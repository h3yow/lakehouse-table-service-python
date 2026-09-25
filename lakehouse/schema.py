"""表结构（Schema）定义与结构演进。

核心约定：列以稳定的整数 column id 标识，改名不影响物理数据定位，
删列只做逻辑删除，历史数据文件不会错位。

新增列的缺省语义（确定性、稳定性）：
- 新增列若声明常量默认值 D，则该列在任何历史行上的取值都为 D；
- 未声明默认值时，历史行取值为 NULL（列必须 nullable）。
该默认值是列定义的一部分，一经发布不可改变，因此旧版本的读取结果
不会随后续结构演进而变化。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import ColumnNotFoundError, SchemaValidationError

# 支持的原始数据类型：保持最小集合，语义明确可解释
SUPPORTED_TYPES: frozenset[str] = frozenset({"int", "long", "double", "string", "bool"})


@dataclass(frozen=True)
class Column:
    """一列的不可变描述。

    column_id 全表稳定且永不复用；name 可改，id 不变。
    has_default 为 False 时新增列对历史行的取值为 NULL（稳定缺省语义）。
    """

    column_id: int
    name: str
    dtype: str
    nullable: bool = True
    has_default: bool = False
    default_value: object = None
    dropped: bool = False


@dataclass(frozen=True)
class Schema:
    """某一个 schema 版本下的完整列集合（含已逻辑删除的列）。"""

    schema_id: int
    columns: tuple[Column, ...] = field(default_factory=tuple)

    def live_columns(self) -> tuple[Column, ...]:
        """返回未删除的列（按 column_id 升序，顺序即列身份顺序）。"""
        return tuple(c for c in self.columns if not c.dropped)

    def column_by_name(self, name: str) -> Column:
        """按当前名称查找未删除列，找不到抛 ColumnNotFoundError。"""
        for col in self.columns:
            if not col.dropped and col.name == name:
                return col
        raise ColumnNotFoundError(f"列不存在: {name} (schema_id={self.schema_id})")

    def column_by_id(self, column_id: int) -> Column:
        """按 id 查找列（含已删除列，用于读取历史数据文件）。"""
        for col in self.columns:
            if col.column_id == column_id:
                return col
        raise ColumnNotFoundError(f"column_id 不存在: {column_id}")

    def is_live(self, column_id: int) -> bool:
        try:
            return not self.column_by_id(column_id).dropped
        except ColumnNotFoundError:
            return False

    def validate_row(self, row: dict[str, object]) -> dict[int, object]:
        """校验一行按列名给出的数据，返回 {column_id: value}。

        规则：
        - 出现未知列名（含已删列旧名）-> SchemaValidationError，绝不静默忽略；
        - 缺失的可空列填 None，缺失且声明默认值的列填默认值；
        - 缺失且既不可空又无默认值 -> SchemaValidationError；
        - 值类型与 dtype 不匹配 -> SchemaValidationError。
        """
        live = {c.name: c for c in self.live_columns()}
        unknown = set(row) - set(live)
        if unknown:
            raise SchemaValidationError(
                f"数据中存在表结构未定义的列: {sorted(unknown)}"
            )

        result: dict[int, object] = {}
        for name, col in live.items():
            if name in row:
                value = row[name]
                _check_type(col, value)
            elif col.has_default:
                value = col.default_value
            elif col.nullable:
                value = None
            else:
                raise SchemaValidationError(f"必填列缺失且无默认值: {name}")
            result[col.column_id] = value
        return result


def _check_type(col: Column, value: object) -> None:
    if value is None:
        if not col.nullable:
            raise SchemaValidationError(f"列 {col.name} 不可为 NULL")
        return
    t = col.dtype
    if t in ("int", "long"):
        ok = isinstance(value, int) and not isinstance(value, bool)
    elif t == "double":
        ok = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif t == "string":
        ok = isinstance(value, str)
    elif t == "bool":
        ok = isinstance(value, bool)
    else:  # pragma: no cover - 建表/演进时已限制类型
        raise SchemaValidationError(f"未知类型: {t}")
    if not ok:
        raise SchemaValidationError(
            f"列 {col.name} 期望类型 {t}，实际值 {value!r} 类型为 {type(value).__name__}"
        )


@dataclass(frozen=True)
class SchemaChange:
    """一次结构演进操作的描述。

    kind ∈ {"add_column", "rename_column", "drop_column"}。
    """

    kind: str
    name: str
    new_name: str | None = None
    dtype: str | None = None
    nullable: bool = True
    has_default: bool = False
    default_value: object = None

    @staticmethod
    def add_column(
        name: str,
        dtype: str,
        nullable: bool = True,
        has_default: bool = False,
        default_value: object = None,
    ) -> "SchemaChange":
        return SchemaChange(
            kind="add_column",
            name=name,
            dtype=dtype,
            nullable=nullable,
            has_default=has_default,
            default_value=default_value,
        )

    @staticmethod
    def rename_column(name: str, new_name: str) -> "SchemaChange":
        return SchemaChange(kind="rename_column", name=name, new_name=new_name)

    @staticmethod
    def drop_column(name: str) -> "SchemaChange":
        return SchemaChange(kind="drop_column", name=name)


@dataclass
class SchemaEvolution:
    """结构演进器：在既有 schema 上应用变更，产生新 schema_id。

    column_id 单调不复用；每次 apply 都基于上一个结果生成不可变 Schema。
    """

    current: Schema
    _next_column_id: int = 0

    @classmethod
    def begin(cls, columns: list[tuple[str, str]]) -> "SchemaEvolution":
        """从列定义列表构造 schema_id=0 的初始结构。"""
        cols: list[Column] = []
        for cid, (name, dtype) in enumerate(columns):
            if dtype not in SUPPORTED_TYPES:
                raise SchemaValidationError(f"不支持的列类型: {dtype}")
            cols.append(Column(column_id=cid, name=name, dtype=dtype))
        return cls(current=Schema(schema_id=0, columns=tuple(cols)), _next_column_id=len(cols))

    def apply(self, change: SchemaChange) -> Schema:
        """应用单个变更，返回新的不可变 Schema，并推进演进器状态。"""
        if change.kind == "add_column":
            new_schema = self._add(change)
        elif change.kind == "rename_column":
            new_schema = self._rename(change)
        elif change.kind == "drop_column":
            new_schema = self._drop(change)
        else:
            raise SchemaValidationError(f"未知结构变更类型: {change.kind}")
        self.current = new_schema
        return new_schema

    def _add(self, change: SchemaChange) -> Schema:
        assert change.dtype is not None
        if change.dtype not in SUPPORTED_TYPES:
            raise SchemaValidationError(f"不支持的列类型: {change.dtype}")
        try:
            existing = self.current.column_by_name(change.name)
        except ColumnNotFoundError:
            existing = None  # type: ignore[assignment]
        if existing is not None:
            raise SchemaValidationError(f"列已存在，不能重复新增: {change.name}")
        if not change.nullable and not change.has_default:
            raise SchemaValidationError(
                f"新增列 {change.name} 必须 nullable 或提供默认值，"
                "否则历史行无法获得确定取值"
            )
        if change.has_default:
            # 声明的常量默认值必须与列类型一致，否则历史行会获得非法值
            probe = Column(column_id=-1, name=change.name, dtype=change.dtype)
            _check_type(probe, change.default_value)
        new_col = Column(
            column_id=self._next_column_id,
            name=change.name,
            dtype=change.dtype,
            nullable=change.nullable,
            has_default=change.has_default,
            default_value=change.default_value,
        )
        cols = tuple(sorted(self.current.columns + (new_col,), key=lambda c: c.column_id))
        self._next_column_id += 1
        return Schema(schema_id=self.current.schema_id + 1, columns=cols)

    def _rename(self, change: SchemaChange) -> Schema:
        if not change.new_name:
            raise SchemaValidationError("改名必须提供 new_name")
        col = self.current.column_by_name(change.name)
        # 新名字不得与任何现存列冲突（避免歧义导致查询错位）
        for other in self.current.live_columns():
            if other.column_id != col.column_id and other.name == change.new_name:
                raise SchemaValidationError(
                    f"目标列名已被占用: {change.new_name}"
                )
        renamed = Column(
            column_id=col.column_id,
            name=change.new_name,
            dtype=col.dtype,
            nullable=col.nullable,
            has_default=col.has_default,
            default_value=col.default_value,
            dropped=col.dropped,
        )
        cols = tuple(renamed if c.column_id == col.column_id else c for c in self.current.columns)
        return Schema(schema_id=self.current.schema_id + 1, columns=cols)

    def _drop(self, change: SchemaChange) -> Schema:
        col = self.current.column_by_name(change.name)
        dropped = Column(
            column_id=col.column_id,
            name=col.name,
            dtype=col.dtype,
            nullable=col.nullable,
            has_default=col.has_default,
            default_value=col.default_value,
            dropped=True,
        )
        cols = tuple(dropped if c.column_id == col.column_id else c for c in self.current.columns)
        return Schema(schema_id=self.current.schema_id + 1, columns=cols)
