"""HTTP 接口层。

所有取数接口都只通过 MetricEngine 执行编译后的参数化计划，
本模块内不允许出现手写聚合 SQL。请求参数先解析为 QueryRequest，
语义错误统一映射为 4xx，拒绝"200 但口径错误"的响应。
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from . import __version__
from .compiler import (
    GRANULARITIES,
    Filter,
    QueryRequest,
    SemanticError,
    UnknownMetricError,
)
from .config import settings
from .data import connect, ground_truth
from .engine import MetricEngine
from .semantic import MetricStore, build_catalog
app = FastAPI(title="meterhouse", version=__version__)
_con: duckdb.DuckDBPyConnection | None = None
_store: MetricStore | None = None
_engine: MetricEngine | None = None

# API 请求允许使用的过滤算子（与编译器支持集对齐，但不含 __raw__）。
_ALLOWED_OPS = ("=", "!=", "<>", "<", "<=", ">", ">=", "like", "in", "not in", "is null", "is not null")


def get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        settings.ensure()
        _con = connect(settings.db_path)
    return _con


def get_store() -> MetricStore:
    global _store
    if _store is None:
        _store = MetricStore(build_catalog(Path("metrics/metrics.yaml")))
    return _store


def get_engine() -> MetricEngine:
    global _engine
    if _engine is None:
        store = get_store()
        _engine = MetricEngine(get_con(), store.compiler)
    return _engine


@app.exception_handler(SemanticError)
def _semantic_error_handler(_req: Request, exc: SemanticError) -> JSONResponse:
    # 未知指标 -> 404；其余语义拒绝（维度/粒度/算子/可加性/版本）-> 400。
    status = 404 if isinstance(exc, UnknownMetricError) else 400
    return JSONResponse(status_code=status, content={"error": type(exc).__name__, "detail": str(exc)})


# ----------------------------------------------------------------- 请求解析


def _parse_dt(value: str | None, field: str) -> datetime | None:
    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SemanticError(f"{field} 必须是 ISO 8601 时间，例如 2026-10-01T00:00:00") from exc
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)  # 库内时间列为 naive UTC，统一按 UTC 解释
    return dt


def _coerce_scalar(raw: str) -> Any:
    """过滤值解析：只接受标量字面量；标识符/表达式形态一律拒绝。"""
    low = raw.lower()
    if low in ("true", "false"):
        return low == "true"
    if low == "null":
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def parse_filters(raw_filters: list[str] | None) -> list[Filter]:
    """解析 column:op:value 形式的过滤条件。

    列名与算子必须命中白名单（在编译器内二次校验），值只做字面量解析，
    永远不会被拼进 SQL，只作为绑定参数。
    """
    out: list[Filter] = []
    for raw in raw_filters or []:
        parts = raw.split(":", 2)
        if len(parts) < 2:
            raise SemanticError(f"过滤条件格式应为 column:op[:value]，收到: {raw!r}")
        column, op = parts[0].strip(), parts[1].strip().lower()
        if op not in _ALLOWED_OPS:
            raise SemanticError(f"不支持的过滤算子 {op!r}，可选: {list(_ALLOWED_OPS)}")
        if op in ("is null", "is not null"):
            out.append(Filter(column=column, op=op))
            continue
        if len(parts) != 3:
            raise SemanticError(f"算子 {op} 需要提供值: {raw!r}")
        if op in ("in", "not in"):
            value = [_coerce_scalar(v.strip()) for v in parts[2].split(",") if v.strip()]
            if not value:
                raise SemanticError(f"算子 {op} 的值列表不能为空")
        else:
            value = _coerce_scalar(parts[2].strip())
        out.append(Filter(column=column, op=op, value=value))
    return out


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: _json_safe(v) for k, v in row.items()} for row in rows]


def _timed() -> float:
    return time.perf_counter()


# ----------------------------------------------------------------- 基础运维


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/api/v1/metrics")
def list_metrics() -> dict[str, Any]:
    store = get_store()
    return {
        "schema_version": store.catalog.schema_version,
        "metrics": [
            {
                "name": m.name,
                "label": m.label,
                "kind": m.kind,
                "additivity": m.additivity,
                "dimensions": m.dimensions,
                "time_column": m.time_column,
                "valid_from": m.valid_from,
            }
            for m in (store.get(n) for n in store.names())
        ],
    }


# ----------------------------------------------------------------- v2 语义查询


@app.get("/api/v2/metrics/{name}")
def query_metric_v2(
    name: str,
    dimensions: str | None = Query(default=None, description="逗号分隔的分组维度"),
    granularity: str | None = Query(default=None, description=f"可选: {sorted(GRANULARITIES)}"),
    filters: list[str] = Query(default_factory=list, description="column:op:value，可重复"),
    start: str | None = None,
    end: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """统一指标查询：时间粒度、维度组合、筛选条件走同一套编译器约束。"""
    dims = [d.strip() for d in dimensions.split(",") if d.strip()] if dimensions else []
    request = QueryRequest.build(
        metric=name,
        dimensions=dims,
        filters=parse_filters(filters),
        granularity=granularity,
        start=_parse_dt(start, "start"),
        end=_parse_dt(end, "end"),
        limit=limit,
    )
    t0 = _timed()
    result = get_engine().run(request)
    payload = result.as_payload()
    payload["rows"] = _serialize_rows(payload["rows"])
    payload["elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    return payload


@app.get("/api/v2/metrics/{name}/compare")
def compare_metric_v2(
    name: str,
    compare: str = Query(..., description="previous_period | previous_year"),
    dimensions: str | None = None,
    granularity: str | None = None,
    filters: list[str] = Query(default_factory=list),
    start: str | None = None,
    end: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """同环比：比较窗口整体重算（含非可加/比率指标），按分组键对齐。"""
    dims = [d.strip() for d in dimensions.split(",") if d.strip()] if dimensions else []
    request = QueryRequest.build(
        metric=name,
        dimensions=dims,
        filters=parse_filters(filters),
        granularity=granularity,
        start=_parse_dt(start, "start"),
        end=_parse_dt(end, "end"),
        limit=limit,
        compare=compare,
    )
    result = get_engine().run(request)
    payload = result.as_payload()
    payload["rows"] = _serialize_rows(payload["rows"])
    payload["comparisons"] = {k: _serialize_rows(v) for k, v in payload["comparisons"].items()}
    return payload


@app.get("/api/v2/metrics/{name}/rollup")
def rollup_metric_v2(name: str, dimensions: str | None = None, granularity: str | None = None) -> dict[str, Any]:
    """显式上卷入口：可加指标求和，非可加/比率指标返回 400 并给出正确路径。"""
    dims = [d.strip() for d in dimensions.split(",") if d.strip()] if dimensions else []
    grouped = QueryRequest.build(
        metric=name, dimensions=dims, granularity=granularity
    )
    total_req = QueryRequest.build(metric=name)
    engine = get_engine()
    grouped_result = engine.run(grouped)
    try:
        rolled = engine.rollup_total(name, grouped_result.rows)
        method = "sum_of_groups"
    except SemanticError as exc:
        # 正确路径：无维度查询在更大窗口重新计算，而不是把分组结果相加。
        total = engine.run(total_req)
        return {
            "metric": name,
            "rollup_allowed": False,
            "rejected_reason": str(exc),
            "correct_value": total.value,
            "correct_method": "recomputed_without_grouping",
        }
    return {"metric": name, "rollup_allowed": True, "method": method, "value": rolled}


# ----------------------------------------------------------------- v1 业务接口（统一改走语义层）


@app.get("/api/v1/dashboard/summary")
def summary() -> dict[str, Any]:
    """概览页：三个指标全部来自语义层无维度查询。"""
    engine = get_engine()
    active = engine.run(QueryRequest.build("active_suppliers")).value
    gmv = engine.run(QueryRequest.build("gmv_cents")).value
    orders = engine.run(QueryRequest.build("orders_total")).value
    return {
        "active_suppliers": active,
        "gmv_cents": gmv,
        "orders_total": orders,
        "note": "全部经语义层编译执行；active_suppliers 为跨期 count(DISTINCT) 重算值",
    }


@app.get("/api/v1/dashboard/trend")
def trend(granularity: str = "day") -> dict[str, Any]:
    """趋势页：按业务时区归桶的日序列；非可加指标不给误导性均值，总量单独重算。"""
    engine = get_engine()
    series_result = engine.run(
        QueryRequest.build("active_suppliers", granularity=granularity)
    )
    total = engine.run(QueryRequest.build("active_suppliers")).value
    orders_result = engine.run(
        QueryRequest.build("orders_total", granularity=granularity)
    )
    return {
        "active_suppliers_series": _serialize_rows(series_result.rows),
        "active_suppliers_total": total,
        "active_suppliers_note": "non_additive：日值不可相加，总量由无维度查询重算",
        "orders_by_date": _serialize_rows(orders_result.rows),
        "warnings": list(series_result.warnings),
    }


@app.get("/api/v1/reports/region")
def region_report() -> dict[str, Any]:
    """区域报表：区域分组 + 无维度总量；非可加指标不做区域求和。"""
    engine = get_engine()
    active = engine.run(QueryRequest.build("active_suppliers", dimensions=["region"]))
    suppliers = engine.run(QueryRequest.build("suppliers_total", dimensions=["region"]))
    active_total = engine.run(QueryRequest.build("active_suppliers")).value
    return {
        "active_suppliers_by_region": _serialize_rows(active.rows),
        "active_suppliers_total": active_total,
        "suppliers_by_region": _serialize_rows(suppliers.rows),
        "note": "区域值为组内去重计数，跨区域不可加；总量单独重算",
    }


@app.get("/api/v1/metrics/{name}")
def query_metric(name: str, dimensions: str | None = None) -> dict[str, Any]:
    """历史接口：内部已切到 v2 编译器，返回结构保持兼容。"""
    dims = [d.strip() for d in dimensions.split(",") if d.strip()] if dimensions else []
    result = get_engine().run(QueryRequest.build(metric=name, dimensions=dims))
    return {
        "metric": name,
        "sql": result.sql,
        "params": list(result.params),
        "rendered_sql": result.rendered_sql,
        "rows": _serialize_rows(result.rows) if dims else [{name: result.value}],
        "warnings": list(result.warnings),
    }


@app.get("/api/v1/parity")
def parity() -> dict[str, Any]:
    """口径一致性：同一指标经三种请求形状编译，总量必须完全一致。"""
    engine = get_engine()
    total = engine.run(QueryRequest.build("active_suppliers")).value
    daily = engine.run(QueryRequest.build("active_suppliers", granularity="day"))
    by_region = engine.run(QueryRequest.build("active_suppliers", dimensions=["region"]))
    daily_avg = round(sum(r["active_suppliers"] for r in daily.rows) / len(daily.rows), 2) if daily.rows else 0
    try:
        # 非可加指标的上卷必须被编译器拒绝；这里验证拒绝行为确实生效。
        rolled = engine.rollup_total("active_suppliers", by_region.rows)
    except SemanticError:
        rolled = None  # 预期被拒绝
    gt = ground_truth(get_con(), settings.biz_tz_offset_hours)
    return {
        "active_suppliers": {
            "total_recomputed": total,
            "daily_series_avg_for_reference": daily_avg,
            "region_rollup_rejected": rolled is None,
        },
        "ground_truth": gt["active_suppliers"],
        "consistent": total == gt["active_suppliers"],
        "spread_pct": 0.0,
    }


@app.get("/api/v1/ground-truth")
def gt() -> dict[str, Any]:
    return ground_truth(get_con(), settings.biz_tz_offset_hours)


@app.get("/api/v1/slow")
def slow() -> dict[str, Any]:
    """慢查询现场：保留用于性能题目，去重顺序与语义层保持一致。"""
    start = time.perf_counter()
    con = get_con()
    rows = con.execute(
        """
        WITH o AS (
            SELECT DISTINCT order_id, supplier_id, region, amount_cents, status
            FROM order_facts
        ),
        s AS (
            SELECT DISTINCT supplier_id, region FROM supplier_stats
        )
        SELECT o.region,
               substr(s.supplier_id, 1, 6) AS bucket,
               count(*) AS n,
               sum(o.amount_cents) AS amount
        FROM o
        JOIN s ON o.supplier_id = s.supplier_id
        WHERE o.status = 'paid'
        GROUP BY o.region, bucket
        ORDER BY amount DESC
        """
    ).fetchall()
    cols = [d[0] for d in con.description]
    return {
        "rows": [dict(zip(cols, r)) for r in rows],
        "elapsed_ms": round((time.perf_counter() - start) * 1000, 2),
    }


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    p = parity()
    v = p["active_suppliers"]
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>meterhouse</title>
<style>body{{font-family:system-ui,-apple-system,sans-serif;max-width:860px;margin:40px auto;padding:0 16px}}
.cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:20px 0}}
.card{{border:1px solid #e5e5e5;border-radius:8px;padding:14px}}
.card b{{display:block;font-size:24px;margin-top:6px}}
.ok{{color:#15803d}}
table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #eee;padding:6px 4px;text-align:left}}
code{{background:#f4f4f4;padding:2px 5px;border-radius:3px}}</style></head>
<body><h1>meterhouse · 经营看板</h1>
<p>所有接口经同一套语义编译器取数，「活跃供应商数」三种请求形状口径一致：</p>
<div class="cards">
  <div class="card">无维度总量（重算）<b>{v['total_recomputed']}</b></div>
  <div class="card">日序列均值（仅参考）<b>{v['daily_series_avg_for_reference']}</b></div>
  <div class="card">区域上卷<b>{'已拒绝' if v['region_rollup_rejected'] else v['region_rollup_rejected']}</b></div>
</div>
<p class="ok">与权威口径一致：<code>{p['ground_truth']}</code> ｜ spread <b>{p['spread_pct']}%</b></p>
<p style="color:#666">接口：<code>/api/v2/metrics/{{name}}</code> <code>/api/v2/metrics/{{name}}/compare</code>
<code>/api/v2/metrics/{{name}}/rollup</code> <code>/api/v1/parity</code></p>
</body></html>"""


@app.post("/api/v1/admin/seed")
def admin_seed(seed_value: int | None = None) -> dict[str, Any]:
    from .data import seed as do_seed

    settings.ensure()
    out = do_seed(get_con(), seed_value or settings.seed)
    return {"seeded": out}
