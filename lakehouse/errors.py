"""湖仓表服务的错误类型。

每种错误对应一类可区分的失败原因，调用方与测试可据此判定结果。
"""


class LakehouseError(Exception):
    """所有湖仓错误的基类。"""


class TableNotFoundError(LakehouseError):
    """表不存在。"""


class TableAlreadyExistsError(LakehouseError):
    """表已存在。"""


class VersionNotFoundError(LakehouseError):
    """指定版本不存在（尚未提交或已超出范围）。"""


class CommitConflictError(LakehouseError):
    """并发提交冲突：两个提交基于同一版本且触及相同分区，被拒绝。

    消息中需携带冲突的分区与版本信息，便于定位。
    """


class DuplicateBatchError(LakehouseError):
    """批次重复但内容不一致：同一 batch_id 已提交过不同内容，被拒绝。

    注意：同一 batch_id 提交完全相同的内容属于幂等重放，应返回原版本，
    不抛此错误；只有内容不一致时才抛出。
    """


class SchemaEvolutionError(LakehouseError):
    """非法的结构演进（如删除主键列、新增列缺少缺省值等）。"""


class ValidationError(LakehouseError):
    """写入数据不符合表结构（缺主键、类型不符等）。"""
