"""执行层错误分类。

迁移期间最重要的约定：**引擎故障不能被折算成指标值**。
所有失败都必须以异常向外传递，由路由层显式决定是回退还是返回 503。
"""

from __future__ import annotations


class MeterhouseError(Exception):
    """所有可预期错误的基类。"""


class EngineError(MeterhouseError):
    """引擎执行失败。

    `engine` 标明故障来自哪个执行引擎（duckdb / clickhouse），用于故障隔离：
    一个引擎的故障不应影响另一个引擎上执行的接口。
    """

    def __init__(self, message: str, *, engine: str, interface: str | None = None) -> None:
        super().__init__(message)
        self.engine = engine
        self.interface = interface

    def as_dict(self) -> dict[str, str | None]:
        return {
            "type": type(self).__name__,
            "engine": self.engine,
            "interface": self.interface,
            "message": str(self),
        }


class EngineUnavailable(EngineError):
    """连不上引擎（连接被拒绝、DNS、熔断打开等）。"""


class EngineTimeout(EngineError):
    """查询超过执行时限。"""


class CapacityExceeded(EngineError):
    """并发额度、结果行数或扫描量超出配置上限。"""


class EngineQueryError(EngineError):
    """引擎返回了查询错误（SQL 语法、类型不匹配等）。"""


class ServiceUnavailable(MeterhouseError):
    """主引擎不可用且没有可用的回退引擎。

    API 层必须把它映射成 5xx，而不是 0 / null 指标。
    """

    def __init__(
        self,
        message: str,
        *,
        interface: str | None = None,
        causes: list[EngineError] | None = None,
    ) -> None:
        super().__init__(message)
        self.interface = interface
        self.causes = causes or []

    def as_dict(self) -> dict:
        return {
            "type": type(self).__name__,
            "interface": self.interface,
            "message": str(self),
            "causes": [c.as_dict() for c in self.causes],
        }


class BatchAlreadyLoaded(MeterhouseError):
    """同一批次被重复加载。

    原始事实会按行保留，重复加载会让事实翻倍并污染口径，因此直接拒绝，
    而不是"再插一遍"。
    """


class DefinitionError(MeterhouseError, ValueError):
    """指标定义或查询参数不合法（未知指标、非法维度等）。

    继承 `ValueError` 以保留旧的调用契约（旧代码 `except ValueError`）。
    """


class UnknownMetric(DefinitionError, KeyError):
    """未知指标。

    同时继承 `KeyError`，保留 `store.get()` 的历史契约。
    """

    def __str__(self) -> str:  # KeyError 会给消息加引号，这里保持可读。
        return self.args[0] if self.args else ""
