"""API。

接口按页面组织，每个接口各自写 SQL —— 这是当前状态，也是核心问题。
`USE_SEMANTIC_LAYER=1` 时部分接口改走语义层，但语义层只覆盖了 2 个指标，
口径仍然对不齐。
"""

from __future__ import annotations

import time
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from . import __version__
from .config import settings
from .data import connect, ground_truth
from .queries import QUERIES
from .semantic import MetricStore

app = FastAPI(title="meterhouse", version=__version__)
_con: duckdb.DuckDBPyConnection | None = None
_store: MetricStore | None = None


def get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        settings.ensure()
        _con = connect(settings.db_path)
    return _con


def get_store() -> MetricStore:
    global _store
    if _store is None:
        _store = MetricStore.load(__import__("pathlib").Path("metrics/metrics.yaml"))
    return _store


def _q(sql: str) -> list[dict[str, Any]]:
    start = time.perf_counter()
    rows = get_con().execute(sql).fetchall()
    cols = [d[0] for d in get_con().description] if get_con().description else []
    elapsed = (time.perf_counter() - start) * 1000
    out = [dict(zip(cols, r)) for r in rows]
    for r in out:
        r["_elapsed_ms"] = round(elapsed, 2)
    return out


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/api/v1/dashboard/summary")
def summary() -> dict[str, Any]:
    """概览页：活跃供应商数用口径 A。"""
    active = _q(QUERIES["active_suppliers_a"])[0]["active_suppliers"]
    gmv = _q(QUERIES["gmv_cents"])[0]["gmv_cents"]
    orders = _q(QUERIES["orders_total"])[0]["orders_total"]
    return {
        "active_suppliers": active,
        "gmv_cents": gmv,
        "orders_total": orders,
        "note": "active_suppliers 走口径 A（跨期去重）",
    }


@app.get("/api/v1/dashboard/trend")
def trend() -> dict[str, Any]:
    """趋势页：活跃供应商数用口径 B（按 UTC 日期去重后求平均）。"""
    active = _q(QUERIES["active_suppliers_b"])[0]["active_suppliers"]
    series = _q(QUERIES["orders_by_date"])
    return {
        "active_suppliers": round(float(active or 0), 2),
        "orders_by_date": series,
        "note": "active_suppliers 走口径 B（UTC 日期）",
    }


@app.get("/api/v1/reports/region")
def region_report() -> dict[str, Any]:
    """区域报表：活跃供应商数用口径 C（按 (supplier_id, date) 去重，漏了 is_active）。"""
    rows = _q(QUERIES["active_suppliers_c"])
    suppliers = _q(QUERIES["supplier_count_by_region"])
    return {
        "by_region": rows,
        "suppliers_by_region": suppliers,
        "note": "active_suppliers 走口径 C（按日期拼键去重）",
    }


@app.get("/api/v1/metrics")
def list_metrics() -> dict[str, Any]:
    """列出语义层里的指标（不含手写 SQL 的那些）。"""
    store = get_store()
    return {
        "metrics": [
            {
                "name": m.name,
                "label": m.label,
                "kind": m.kind,
                "additivity": m.additivity,
                "dimensions": m.dimensions,
                "time_column": m.time_column,
            }
            for m in (store.get(n) for n in store.names())
        ]
    }


@app.get("/api/v1/metrics/{name}")
def query_metric(name: str, dimensions: str | None = None) -> dict[str, Any]:
    """通过语义层查询指标。

    当前只支持无维度或多维度，不支持时间粒度、同环比、时间过滤。
    """
    store = get_store()
    try:
        if dimensions:
            dims = [d.strip() for d in dimensions.split(",") if d.strip()]
            sql = store.compile_grouped(name, dims)
        else:
            sql = store.compile(name)
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"metric": name, "sql": sql, "rows": _q(sql)}


@app.get("/api/v1/parity")
def parity() -> dict[str, Any]:
    """三份口径并排对比。这是质检与出题最常用的接口。"""
    a = _q(QUERIES["active_suppliers_a"])[0]["active_suppliers"]
    b = _q(QUERIES["active_suppliers_b"])[0]["active_suppliers"]
    c_rows = _q(QUERIES["active_suppliers_c"])
    c_sum = sum(r["active_suppliers"] for r in c_rows)
    gt = ground_truth(get_con(), settings.biz_tz_offset_hours)
    values = {"A": a, "B": round(float(b or 0), 2), "C": c_sum}
    spread = (max(values.values()) - min(values.values())) / max(values.values()) if max(values.values()) else 0
    return {
        "active_suppliers": values,
        "ground_truth": gt["active_suppliers"],
        "spread_pct": round(spread * 100, 2),
        "rows_by_region_C": c_rows,
    }


@app.get("/api/v1/ground-truth")
def gt() -> dict[str, Any]:
    return ground_truth(get_con(), settings.biz_tz_offset_hours)


@app.get("/api/v1/slow")
def slow() -> dict[str, Any]:
    """慢查询现场：报表接口的原始 SQL，用于性能优化题目。"""
    start = time.perf_counter()
    rows = _q(
        """
        SELECT o.region,
               substr(s.supplier_id, 1, 6) AS bucket,
               count(*) AS n,
               sum(o.amount_cents) AS amount
        FROM order_facts o
        JOIN supplier_stats s ON o.supplier_id = s.supplier_id
        WHERE o.status = 'paid'
        GROUP BY o.region, bucket
        ORDER BY amount DESC
        """
    )
    return {"rows": rows, "elapsed_ms": round((time.perf_counter() - start) * 1000, 2)}


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    p = parity()
    v = p["active_suppliers"]
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>meterhouse</title>
<style>body{{font-family:system-ui,-apple-system,sans-serif;max-width:820px;margin:40px auto;padding:0 16px}}
.cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:20px 0}}
.card{{border:1px solid #e5e5e5;border-radius:8px;padding:14px}}
.card b{{display:block;font-size:24px;margin-top:6px}}
.warn{{color:#b45309}}
table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #eee;padding:6px 4px;text-align:left}}
code{{background:#f4f4f4;padding:2px 5px;border-radius:3px}}</style></head>
<body><h1>meterhouse · 经营看板</h1>
<p>同一个「活跃供应商数」，三个接口给出三个数字：</p>
<div class="cards">
  <div class="card">概览页（口径 A）<b>{v['A']}</b></div>
  <div class="card">趋势页（口径 B）<b>{v['B']}</b></div>
  <div class="card">区域报表（口径 C）<b>{v['C']}</b></div>
</div>
<p class="warn">口径差异 <b>{p['spread_pct']}%</b> ｜ 权威口径 <code>{p['ground_truth']}</code></p>
<p style="color:#666">接口：<code>/api/v1/parity</code> <code>/api/v1/metrics</code> <code>/api/v1/slow</code></p>
</body></html>"""


@app.post("/api/v1/admin/seed")
def admin_seed(seed_value: int | None = None) -> dict[str, Any]:
    from .data import seed as do_seed

    settings.ensure()
    con = get_con()
    out = do_seed(con, seed_value or settings.seed)
    return {"seeded": out}
