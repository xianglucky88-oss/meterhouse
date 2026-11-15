"""meterhouse —— 内部经营看板与指标语义层。

结构：
    * metrics/metrics.yaml 是指标口径契约，meterhouse.semantic 把它编译成 SQL；
    * 页面接口仍保留历史手写 SQL，/api/v1/parity 用于对比口径差异；
    * meterhouse.regression 是独立于生产实现的语义回归体系：
      参考口径由纯 Python oracle 计算，候选实现单独执行，
      基准变更必须显式审批（见 docs/metric-regression.md）。

本地执行用 DuckDB（秒级），生产是 ClickHouse。
SQL 方言差异由 sql/dialect.py 的适配层处理。
"""

__version__ = "1.1.4"
