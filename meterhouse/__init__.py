"""meterhouse —— 内部经营看板与指标语义层。

架构（v2）：
    * metrics/metrics.yaml 是唯一口径来源：来源去重规格、维度注册、
      结构化过滤、比率分子分母、可加性与语义版本；
    * meterhouse.compiler 把 QueryRequest 编译为参数化 SQL 计划，
      维度/粒度/算子/类型/可加性在编译期统一约束，非法请求显式拒绝；
    * meterhouse.engine 是唯一执行通道，负责空结果归一、零分母标记、
      同环比重算对齐与非可加上卷拦截；
    * meterhouse.api 的所有接口只调用引擎，不再持有手写聚合 SQL。

本地执行用 DuckDB（秒级），生产是 ClickHouse；SQL 方言差异应在
编译器的输出层适配，语义约束不随方言变化。
"""

__version__ = "1.2.0"
