"""本地湖仓表服务异常定义。

不同失败原因使用不同异常类型，便于调用方与测试区分判定依据：

- TableExistsError / TableNotFoundError / ColumnNotFoundError: 资源与结构类
- SchemaValidationError: 写入数据与表结构不匹配
- CommitConflictError: 乐观并发提交冲突（需要重试或显式拒绝）
- DuplicateBatchError: 幂等批次被重复提交（返回既有版本，不产生新版本）
- VersionNotFoundError: 快照版本不存在
"""


class LakehouseError(Exception):
    """所有湖仓表错误的基类。"""


class TableExistsError(LakehouseError):
    pass


class TableNotFoundError(LakehouseError):
    pass


class ColumnNotFoundError(LakehouseError):
    pass


class SchemaValidationError(LakehouseError):
    pass


class CommitConflictError(LakehouseError):
    """提交冲突：自基版本之后出现了不兼容的已提交版本。"""


class DuplicateBatchError(LakehouseError):
    """批次幂等键已存在。已提交的版本号记录在 existing_version 上。"""

    def __init__(self, message: str, existing_version: int):
        super().__init__(message)
        self.existing_version = existing_version


class VersionNotFoundError(LakehouseError):
    pass
