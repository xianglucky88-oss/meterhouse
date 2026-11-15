"""API。

迁移后的取数路径只有一条：

    接口 → 语义层编译 → 路由层选引擎 → 执行 → 响应（含引擎与降级标记）

所有接口都从 `metrics/metrics.yaml` 取数，不再有手写 SQL。

逐接口切流
----------
每个接口独立配置（`metrics/routes.yaml`，可用 `MH_CUTOVER_FILE` 覆盖）：

    off      只走 DuckDB
    shadow   主走 ClickHouse，同时用 DuckDB 重算并记录差异（不改响应数字）
    primary  走 ClickHouse；引擎故障时按配置回退 DuckDB，并标记 degraded

管理接口
--------
    GET  /api/v1/status              切流状态、引擎健康、最近差异
    GET  /api/v1/routes              逐接口切流配置
    POST /api/v1/routes/{interface}  修改切流（需 X-Admin-Token）
    POST /api/v1/admin/sync          从 DuckDB 同步明细到 ClickHouse
    POST /api/v1/admin/verify-parity 校验两引擎明细逐行一致

故障语义
--------
引擎不可用且无可用回退时返回 503，**绝不**返回 0 / null 冒充指标值。
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from . import __version__
from .config import settings
from .data import connect, ground_truth
from .engine import CapacityPolicy, ClickHouseEngine, DuckDBEngine
from .errors import DefinitionError, EngineError, ServiceUnavailable
from .ingest import ClickHouseLoader
from .routing import CutoverStore, Router
from .semantic import MetricStore, TimeWindow

log = logging.getLogger("meterhouse.api")

app = FastAPI(title="meterhouse", version=__version__)

# 接口清单：决定切流配置的合法键集合。
INTERFACES = [
    "dashboard.summary",
    "dashboard.trend",
    "reports.region",
    "metrics.query",
    "exports.daily",
]

_store: MetricStore | None = None
_router: Router | None = None
_cutover: CutoverStore | None = None
_duck: DuckDBEngine | None = None
_ch: ClickHouseEngine | None = None


# ---------------------------------------------------------------------------
# 组装
# ---------------------------------------------------------------------------


def _build() -> None:
    global _store, _router, _cutover, _duck, _ch
    if _router is not None:
        return
    _store = MetricStore.load(
        settings.metrics_file, default_database=settings.ch_database
    )
    settings.ensure()
    _duck = DuckDBEngine(
        path=str(settings.db_path),
        policy=CapacityPolicy(
            max_concurrency=settings.duckdb_max_concurrency,
            acquire_timeout_s=settings.acquire_timeout_s,
            max_result_rows=settings.max_result_rows,
        ),
    )
    engines: dict[str, Any] = {"duckdb": _duck}
    if settings.clickhouse_enabled:
        _ch = ClickHouseEngine(
            host=settings.ch_host,
            port=settings.ch_port,
            username=settings.ch_user,
            password=settings.ch_password,
            database=settings.ch_database,
            connect_timeout_s=settings.ch_timeout_s,
            policy=CapacityPolicy(
                max_concurrency=settings.clickhouse_max_concurrency,
                acquire_timeout_s=settings.acquire_timeout_s,
                max_result_rows=settings.max_result_rows,
            ),
        )
        engines["clickhouse"] = _ch
    _cutover = CutoverStore(settings.cutover_file, INTERFACES)
    _router = Router(engines, _cutover)


def get_store() -> MetricStore:
    _build()
    assert _store is not None
    return _store


def get_router() -> Router:
    _build()
    assert _router is not None
    return _router


def get_loader() -> ClickHouseLoader:
    _build()
    if _ch is None:
        raise HTTPException(
            status_code=503, detail="ClickHouse is not configured (CLICKHOUSE_HOST unset)"
        )
    return ClickHouseLoader(_ch)


def _require_admin(x_admin_token: str | None = Header(default=None)) -> str:
    """写操作必须带令牌。

    未配置令牌时仅允许本机演示使用，并在响应里标明 `actor=local-unauthenticated`。
    """
    if settings.admin_token:
        if x_admin_token != settings.admin_token:
            raise HTTPException(status_code=403, detail="invalid admin token")
        return "admin-token"
    return "local-unauthenticated"


# ---------------------------------------------------------------------------
# 错误处理
# ---------------------------------------------------------------------------


@app.exception_handler(DefinitionError)
def _handle_definition(_request, exc: DefinitionError):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=400, content={"detail": str(exc), "type": "definition"})


@app.exception_handler(ServiceUnavailable)
def _handle_unavailable(_request, exc: ServiceUnavailable):
    from fastapi.responses import JSONResponse

    return JSONResponse(
        status_code=503,
        content={"detail": "metrics engine unavailable; refusing to return a placeholder value", "error": exc.as_dict()},
    )


@app.exception_handler(EngineError)
def _handle_engine(_request, exc: EngineError):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=503, content={"detail": str(exc), "error": exc.as_dict()})


# ---------------------------------------------------------------------------
# 响应装配
# ---------------------------------------------------------------------------


def _window(
    start: date | None, end: date | None, granularity: str | None, basis: str
) -> TimeWindow:
    return TimeWindow(start=start, end=end, granularity=granularity, basis=basis)


def _format_value(value: Any, value_type: str) -> Any:
    if value is None:
        return None
    if value_type == "int":
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    return float(value)


def _result_payload(result, compiled) -> dict[str, Any]:
    rows = []
    for row in result.rows:
        item = dict(zip(result.columns, row))
        if compiled.value_column in item:
            item[compiled.value_column] = _format_value(
                item[compiled.value_column], compiled.value_type
            )
        for col in [*compiled.dimension_columns, compiled.time_column_alias]:
            if col and col in item and hasattr(item[col], "isoformat"):
                item[col] = item[col].isoformat()
        rows.append(item)
    return {
        "metric": compiled.metric,
        "dimensions": compiled.dimension_columns,
        "rows": rows,
        "engine": result.engine,
        "degraded": result.degraded,
        "fallback_from": result.fallback_from,
        "elapsed_ms": result.elapsed_ms,
        "row_count": result.row_count,
        "sql": compiled.sql,
        "lineage": compiled.lineage,
        "definition_version": compiled.definition_version,
    }


def _metric_query(
    interface: str,
    metric: str,
    *,
    dimensions: list[str] | None = None,
    window: TimeWindow | None = None,
    max_rows: int | None = None,
):
    store = get_store()
    router = get_router()

    def build(engine_name: str):
        return store.compile_query(
            metric, engine=engine_name, dimensions=dimensions, time_window=window
        )

    result = router.execute(interface, build, max_rows=max_rows)
    return result, result.compiled


def _metric_value(interface: str, metric: str, *, window: TimeWindow | None = None):
    result, compiled = _metric_query(interface, metric, window=window)
    if not result.rows:
        value = compiled.empty_value
    else:
        row = dict(zip(result.columns, result.rows[0]))
        value = _format_value(row.get(compiled.value_column), compiled.value_type)
    return value, result, compiled


# ---------------------------------------------------------------------------
# 元信息
# ---------------------------------------------------------------------------


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/api/v1/metrics")
def list_metrics() -> dict[str, Any]:
    store = get_store()
    return {
        "definition_version": store.metrics[store.names()[0]].definition_version
        if store.names()
        else None,
        "metrics": [
            {
                "name": m.name,
                "label": m.label,
                "kind": m.kind,
                "additivity": m.additivity,
                "dimensions": m.dimensions,
                "time_column": m.time_column,
                "value_type": m.value_type,
                "definition_version": m.definition_version,
            }
            for m in (store.get(n) for n in store.names())
        ],
    }


@app.get("/api/v1/metrics/{name}")
def query_metric(
    name: str,
    dimensions: str | None = None,
    start: date | None = None,
    end: date | None = None,
    granularity: str | None = None,
    basis: str = "business",
) -> dict[str, Any]:
    dims = [d.strip() for d in dimensions.split(",") if d.strip()] if dimensions else []
    window = _window(start, end, granularity, basis)
    result, compiled = _metric_query("metrics.query", name, dimensions=dims, window=window)
    payload = _result_payload(result, compiled)
    payload["rows"] = payload["rows"]
    return payload


# ---------------------------------------------------------------------------
# 页面接口
# ---------------------------------------------------------------------------


@app.get("/api/v1/dashboard/summary")
def summary(
    start: date | None = None, end: date | None = None
) -> dict[str, Any]:
    window = _window(start, end, None, "business")
    active, a_res, a_comp = _metric_value("dashboard.summary", "active_suppliers", window=window)
    gmv, g_res, _ = _metric_value("dashboard.summary", "gmv_cents", window=window)
    orders, o_res, _ = _metric_value("dashboard.summary", "orders_total", window=window)
    engines = {r.engine for r in (a_res, g_res, o_res)}
    return {
        "active_suppliers": active,
        "gmv_cents": gmv,
        "orders_total": orders,
        "engine": a_res.engine,
        "degraded": any(r.degraded for r in (a_res, g_res, o_res)),
        "fallback_from": a_res.fallback_from,
        "elapsed_ms": round(a_res.elapsed_ms + g_res.elapsed_ms + o_res.elapsed_ms, 2),
        "definition_version": a_comp.definition_version,
        "traceable": {
            "source": a_comp.lineage["source_table"],
            "dedup_by": a_comp.lineage["dedup_by"],
            "version_column": a_comp.lineage["version_column"],
        },
    }


@app.get("/api/v1/dashboard/trend")
def trend(
    start: date | None = None,
    end: date | None = None,
    granularity: str = "day",
    basis: str = "business",
) -> dict[str, Any]:
    window = _window(start, end, granularity, basis)
    # 趋势页用两个明确不同的指标：窗口日均活跃数（标量）与订单时间序列。
    balance = _window(start, end, None, basis)
    avg_active, avg_res, _ = _metric_value(
        "dashboard.trend", "active_suppliers_daily_avg", window=balance
    )
    series_result, series_compiled = _metric_query(
        "dashboard.trend", "orders_total", window=window
    )
    return {
        "active_suppliers_daily_avg": avg_active,
        "granularity": granularity,
        "time_basis": basis,
        "orders_series": _result_payload(series_result, series_compiled),
        "engine": series_result.engine,
        "degraded": series_result.degraded or avg_res.degraded,
    }


@app.get("/api/v1/reports/region")
def region_report(
    start: date | None = None, end: date | None = None
) -> dict[str, Any]:
    window = _window(start, end, None, "business")
    active_result, active_compiled = _metric_query(
        "reports.region", "active_suppliers", dimensions=["region"], window=window
    )
    suppliers_result, suppliers_compiled = _metric_query(
        "reports.region", "suppliers_total", dimensions=["region"], window=window
    )
    gmv_result, gmv_compiled = _metric_query(
        "reports.region", "gmv_cents", dimensions=["region"], window=window
    )
    return {
        "by_region": _result_payload(active_result, active_compiled)["rows"],
        "suppliers_by_region": _result_payload(suppliers_result, suppliers_compiled)["rows"],
        "gmv_by_region": _result_payload(gmv_result, gmv_compiled)["rows"],
        "engine": active_result.engine,
        "degraded": any(
            r.degraded for r in (active_result, suppliers_result, gmv_result)
        ),
        "definition_version": active_compiled.definition_version,
    }


@app.get("/api/v1/exports/daily")
def daily_export(
    start: date | None = None, end: date | None = None
) -> dict[str, Any]:
    window = _window(start, end, "day", "business")
    orders_result, orders_compiled = _metric_query("exports.daily", "orders_total", window=window)
    gmv_result, gmv_compiled = _metric_query("exports.daily", "gmv_cents", window=window)
    return {
        "orders_by_day": _result_payload(orders_result, orders_compiled)["rows"],
        "gmv_by_day": _result_payload(gmv_result, gmv_compiled)["rows"],
        "engine": orders_result.engine,
        "degraded": orders_result.degraded or gmv_result.degraded,
        "definition_version": orders_compiled.definition_version,
    }


# ---------------------------------------------------------------------------
# 口径对比（保留原语义，但值全部来自语义层）
# ---------------------------------------------------------------------------


def _duck_connection():
    """只读打开 DuckDB 供权威口径查询使用（调用方负责关闭）。"""
    import duckdb

    return duckdb.connect(str(settings.db_path))


@app.get("/api/v1/parity")
def parity(start: date | None = None, end: date | None = None) -> dict[str, Any]:
    window = _window(start, end, None, "business")
    windowed, _, _ = _metric_value("metrics.query", "active_suppliers", window=window)
    daily_avg, _, _ = _metric_value("metrics.query", "active_suppliers_daily_avg")
    con = _duck_connection()
    try:
        gt = ground_truth(con, settings.biz_tz_offset_hours)
    finally:
        con.close()
    values = {
        "active_suppliers_windowed": windowed,
        "active_suppliers_daily_avg": daily_avg,
    }
    spread = (
        (max(values.values()) - min(values.values())) / max(values.values())
        if max(values.values())
        else 0
    )
    return {
        "metrics": values,
        "ground_truth": gt,
        "spread_pct": round(spread * 100, 2),
        "note": "窗口内去重数与日均值是两个不同指标，不再混用同一个名字。",
    }


@app.get("/api/v1/ground-truth")
def gt() -> dict[str, Any]:
    con = _duck_connection()
    try:
        return ground_truth(con, settings.biz_tz_offset_hours)
    finally:
        con.close()


@app.get("/api/v1/slow")
def slow() -> dict[str, Any]:
    """原来的慢查询现场。已改成走数据布局的版本，仅用于演示布局效果。"""
    import time

    start = time.perf_counter()
    router = get_router()
    store = get_store()

    def build(engine_name: str):
        return store.compile_query(
            "gmv_cents", engine=engine_name, dimensions=["region"], time_window=TimeWindow()
        )

    result = router.execute("reports.region", build)
    return {
        "rows": result.as_dicts(),
        "engine": result.engine,
        "elapsed_ms": round((time.perf_counter() - start) * 1000, 2),
    }


# ---------------------------------------------------------------------------
# 切流与运维
# ---------------------------------------------------------------------------


@app.get("/api/v1/routes")
def routes() -> dict[str, Any]:
    return {"interfaces": get_router().status()["routes"]}


class RouteUpdate(BaseModel):
    mode: str | None = None
    target: str | None = None
    control: str | None = None
    fallback: bool | None = None
    reason: str = ""


@app.post("/api/v1/routes/{interface}")
def update_route(
    interface: str,
    body: RouteUpdate,
    actor: str = Depends(_require_admin),
) -> dict[str, Any]:
    cutover = _cutover_instance()
    try:
        updated = cutover.update(
            interface,
            mode=body.mode,
            target=body.target,
            control=body.control,
            fallback=body.fallback,
            actor=actor,
            reason=body.reason,
        )
    except DefinitionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if updated.mode == "primary" and updated.target not in get_router()._engines:
        raise HTTPException(
            status_code=400,
            detail=f"target engine {updated.target!r} is not configured; cannot cut over",
        )
    return {"interface": interface, "route": updated.__dict__, "actor": actor}


def _cutover_instance() -> CutoverStore:
    _build()
    assert _cutover is not None
    return _cutover


@app.get("/api/v1/status")
def status() -> dict[str, Any]:
    router = get_router()
    payload = router.status()
    payload["clickhouse_configured"] = settings.clickhouse_enabled
    return payload


@app.get("/api/v1/ingest/batches")
def ingest_batches(limit: int = Query(default=50, le=500)) -> dict[str, Any]:
    """摄入台账：每次同步落在哪张表、多少行、版本区间。"""
    loader = get_loader()
    client = loader.engine._get_client()
    rows = client.query(
        f"SELECT batch_id, source, table_name, rows_loaded, min_version, max_version, loaded_at "
        f"FROM {loader.database}.ingest_batches ORDER BY loaded_at DESC LIMIT {int(limit)}"
    ).result_rows
    return {
        "batches": [
            dict(zip(
                ["batch_id", "source", "table_name", "rows_loaded", "min_version", "max_version", "loaded_at"],
                [*(r[:6]), r[6].isoformat() if hasattr(r[6], "isoformat") else str(r[6])],
            ))
            for r in rows
        ]
    }


@app.post("/api/v1/admin/sync")
def admin_sync(
    truncate: bool = True, actor: str = Depends(_require_admin)
) -> dict[str, Any]:
    loader = get_loader()
    con = connect(settings.db_path)
    try:
        loader.ensure_schema()
        out = loader.sync_all(con, truncate=truncate)
        return {"actor": actor, **out}
    finally:
        con.close()


@app.post("/api/v1/admin/verify-parity")
def admin_verify(actor: str = Depends(_require_admin)) -> dict[str, Any]:
    loader = get_loader()
    con = connect(settings.db_path)
    try:
        return {"actor": actor, **loader.verify(con)}
    finally:
        con.close()


@app.post("/api/v1/admin/seed")
def admin_seed(
    seed_value: int | None = None, actor: str = Depends(_require_admin)
) -> dict[str, Any]:
    """重建本地示例数据。不动 ClickHouse；需再调 /admin/sync 同步。"""
    from .data import seed as do_seed

    settings.ensure()
    con = connect(settings.db_path)
    try:
        out = do_seed(con, seed_value or settings.seed)
    finally:
        con.close()
    return {"actor": actor, "seeded": out, "note": "ClickHouse 未更新，请调 /admin/sync"}


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>meterhouse</title>
<style>body{font-family:system-ui,-apple-system,sans-serif;max-width:880px;margin:40px auto;padding:0 16px}
code{background:#f4f4f4;padding:2px 5px;border-radius:3px}
table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #eee;padding:6px 4px;text-align:left}</style>
</head><body><h1>meterhouse · 经营看板</h1>
<p>所有接口均从 <code>metrics/metrics.yaml</code> 编译取数，支持逐接口切流到列式引擎。</p>
<ul>
<li><code>/api/v1/dashboard/summary</code></li>
<li><code>/api/v1/dashboard/trend</code></li>
<li><code>/api/v1/reports/region</code></li>
<li><code>/api/v1/metrics/{name}?dimensions=region&amp;granularity=month</code></li>
<li><code>/api/v1/status</code> 切流状态与引擎健康</li>
</ul>
</body></html>"""
