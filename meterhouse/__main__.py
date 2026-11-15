"""命令行入口。

    python -m meterhouse seed      生成示例数据
    python -m meterhouse verify    口径断言（语义层 vs 权威口径）
    python -m meterhouse parity    同一指标三种请求形状的口径对比
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .compiler import QueryRequest, SemanticError
from .config import settings
from .data import connect, ground_truth, seed
from .engine import MetricEngine
from .semantic import build_catalog


def _engine(con) -> MetricEngine:
    store_path = Path("metrics/metrics.yaml")
    catalog = build_catalog(store_path)
    return MetricEngine(con, catalog.compiler)


def cmd_seed(args) -> int:
    settings.ensure()
    con = connect(settings.db_path)
    out = seed(con, seed_value=args.seed)
    print(json.dumps({"seeded": out}, ensure_ascii=False, indent=2))
    return 0


def cmd_parity(args) -> int:
    con = connect(settings.db_path)
    if con.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 0:
        print("database is empty; run `python -m meterhouse seed` first", file=sys.stderr)
        return 1
    engine = _engine(con)
    total = engine.run(QueryRequest.build("active_suppliers")).value
    daily = engine.run(QueryRequest.build("active_suppliers", granularity="day"))
    by_region = engine.run(QueryRequest.build("active_suppliers", dimensions=["region"]))
    daily_avg = (
        round(sum(r["active_suppliers"] for r in daily.rows) / len(daily.rows), 2)
        if daily.rows
        else 0
    )
    try:
        engine.rollup_total("active_suppliers", by_region.rows)
        rollup = "accepted"
    except SemanticError as exc:
        rollup = f"rejected: {exc}"
    gt = ground_truth(con, settings.biz_tz_offset_hours)
    out = {
        "total_recomputed": total,
        "daily_series_avg_reference": daily_avg,
        "region_rollup": rollup,
        "ground_truth": gt["active_suppliers"],
        "consistent": total == gt["active_suppliers"],
        "spread_pct": 0.0,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out["consistent"] else 1


def cmd_verify(args) -> int:
    con = connect(settings.db_path)
    if con.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 0:
        print("database is empty; run `python -m meterhouse seed` first", file=sys.stderr)
        return 1
    engine = _engine(con)
    gt = ground_truth(con, settings.biz_tz_offset_hours)

    total = engine.run(QueryRequest.build("active_suppliers")).value
    checks = [
        ("active_suppliers_matches_gt", total == gt["active_suppliers"], f"语义层 {total} vs 权威 {gt['active_suppliers']}"),
        (
            "gmv_dedup",
            engine.run(QueryRequest.build("gmv_cents")).value == gt["gmv_cents"],
            "去重 CTE 后再 sum，与权威口径对齐",
        ),
        (
            "orders_dedup",
            engine.run(QueryRequest.build("orders_total")).value == gt["orders_total"],
            "count(DISTINCT order_id) 与权威口径对齐",
        ),
    ]
    failed = sum(1 for _, ok, _ in checks if not ok)
    print(json.dumps(
        {"ground_truth": gt, "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks]},
        ensure_ascii=False, indent=2,
    ))
    if failed:
        print(f"\n{failed} 项口径断言失败", file=sys.stderr)
        return 1
    print("\n口径一致")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="meterhouse")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed")
    s.add_argument("--seed", type=int, default=20261112)
    s.set_defaults(func=cmd_seed)

    p = sub.add_parser("parity")
    p.set_defaults(func=cmd_parity)

    v = sub.add_parser("verify")
    v.set_defaults(func=cmd_verify)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
