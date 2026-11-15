"""meterhouse —— 内部经营看板与指标语义层。

所有接口都从 `metrics/metrics.yaml` 编译取数：一个指标只有一个定义，
一份定义编译到 DuckDB（本地）与 ClickHouse（生产）两种方言。

模块分工：
    semantic.py    指标定义 + SQL 编译器
    sql/dialect.py 方言适配（时间/聚合/去重/DDL）
    storage.py     事实表布局（分区、排序、skip index、投影）
    data.py        数据生成、建表、权威口径
    ingest.py      DuckDB → ClickHouse 明细同步与逐行校验
    engine/        执行引擎：连接、容量、熔断、异常翻译
    routing.py     逐接口切流、影子比对、回退
    errors.py      错误分类（引擎故障绝不折算成指标值）
    api.py         FastAPI

迁移设计见 docs/migration.md。
"""

__version__ = "1.2.0"
