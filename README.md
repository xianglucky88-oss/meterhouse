# meterhouse

内部经营看板。指标由 YAML 语义层定义，编译到 DuckDB（本地）与 ClickHouse
（生产）。支持逐接口切流、影子比对、容量/故障回退。

## 快速开始

```bash
pip install -r requirements.txt

python -m meterhouse seed      # 生成示例数据（DuckDB）
python -m meterhouse verify    # 口径断言
make api                       # 起服务（9400）
make demo                      # 固定演示序列
```

## 结构

```
meterhouse/
    semantic.py      指标定义加载 + SQL 编译器（两种方言）
    sql/dialect.py   方言适配：时间/聚合/去重/DDL
    storage.py       事实表布局（分区、排序、skip index、投影）
    data.py          数据生成、建表、权威口径
    ingest.py        DuckDB → ClickHouse 明细同步与逐行校验
    engine/          执行引擎：连接管理、容量、熔断、异常翻译
    routing.py       逐接口切流、影子比对、回退
    errors.py        错误分类（引擎故障绝不折算成指标值）
    api.py           FastAPI
    queries.py       已废弃的手写 SQL（现由语义层编译，避免 ImportError）
metrics/
    metrics.yaml     指标定义（唯一口径来源）
    routes.yaml      逐接口切流配置
docs/
    migration.md     迁移设计说明
```

## 接口

```
GET  /api/v1/dashboard/summary     概览
GET  /api/v1/dashboard/trend       趋势（日均活跃数 + 订单序列）
GET  /api/v1/reports/region        区域报表
GET  /api/v1/exports/daily         每日导出
GET  /api/v1/metrics               指标清单
GET  /api/v1/metrics/{name}        语义层查询（维度/时间窗口/粒度）
GET  /api/v1/parity                指标口径并排对比
GET  /api/v1/ground-truth          权威口径
GET  /api/v1/status                切流状态 + 引擎健康 + 最近差异
GET  /api/v1/routes                逐接口切流配置
POST /api/v1/routes/{interface}    修改切流（需 X-Admin-Token）
GET  /api/v1/ingest/batches        摄入台账
POST /api/v1/admin/sync            明细同步到 ClickHouse
POST /api/v1/admin/verify-parity   校验两引擎明细逐行一致
```

## 指标查询示例

```bash
# 按区域、月粒度、限定时间窗口
curl -s 'localhost:9400/api/v1/metrics/gmv_cents?dimensions=region&granularity=month&start=2026-10-01&end=2026-10-31'
```

响应里带 `engine` / `degraded` / `sql` / `lineage` / `definition_version`。

## 切流

```bash
python -m meterhouse cutover --list
python -m meterhouse cutover dashboard.summary --mode shadow   # 先观察差异
python -m meterhouse cutover dashboard.summary --mode primary  # 再切流（保留回退）
```

详见 [docs/migration.md](docs/migration.md)。

## 测试

```bash
python -m pytest -q                 # 不依赖 ClickHouse
CLICKHOUSE_HOST=127.0.0.1 python -m pytest -q   # 含端到端
```

## 配置

见 `.env.example`。生产必须设置 `CLICKHOUSE_HOST`、`MH_ADMIN_TOKEN`。
