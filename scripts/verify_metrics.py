#!/usr/bin/env python3
"""verify_metrics.py —— 语义层口径一致性断言（硬门禁）。

所有断言都通过"编译器 + 引擎"取数，再对照 ground_truth：
    1. 去重/过滤顺序：gmv、orders 与权威口径一致；
    2. 业务时区归桶：本地日 vs UTC 日的订单归属差异被正确表达；
    3. 比率指标：paid_rate / avg_order_cents 与手工重算一致，零分母有标记；
    4. 非可加上卷被显式拒绝，正确路径是无维度重算；
    5. 同环比在平移窗口整体重算，结果与直接查询平移窗口一致；
    6. 未知维度/未知指标/非法算子被拒绝；
    7. 生成 SQL 全部参数化，请求值不以字面量内联。

用法:
    python3 scripts/verify_metrics.py [--db PATH] [--json]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from meterhouse.compiler import (  # noqa: E402
    Filter,
    QueryRequest,
    SemanticError,
)
from meterhouse.data import connect, ground_truth, seed  # noqa: E402
from meterhouse.engine import MetricEngine  # noqa: E402
from meterhouse.semantic import build_catalog  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("DB_PATH", "data/meterhouse.duckdb"))
    ap.add_argument("--reseed", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    path = args.db
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = connect(path)
    if args.reseed or con.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 0:
        seed(con)

    catalog = build_catalog(Path("metrics/metrics.yaml"))
    engine = MetricEngine(con, catalog.compiler)
    gt = ground_truth(con, catalog.biz_offset_hours)

    checks: list[dict[str, object]] = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    # 1. 基础口径：去重 -> 过滤 -> 聚合
    gmv = engine.run(QueryRequest.build("gmv_cents")).value
    add("gmv_dedup", gmv == gt["gmv_cents"], f"语义层 {gmv:,} / 权威 {gt['gmv_cents']:,}")

    orders = engine.run(QueryRequest.build("orders_total")).value
    add("orders_dedup", orders == gt["orders_total"], f"语义层 {orders:,} / 权威 {gt['orders_total']:,}")

    active = engine.run(QueryRequest.build("active_suppliers")).value
    add(
        "active_suppliers_matches_gt",
        active == gt["active_suppliers"],
        f"语义层 {active} / 权威 {gt['active_suppliers']}",
    )

    # 2. 业务时区：按本地日过滤的订单数与 ground_truth 对齐
    local_day = datetime(2026, 10, 30, 16, 0, 0)  # UTC，对应北京时间 10-31 00:00
    next_day = local_day + timedelta(days=1)
    local_count = engine.run(
        QueryRequest.build("orders_total", start=local_day, end=next_day)
    ).value
    add(
        "date_attribution_local",
        local_count == gt["orders_by_local_date"]
        and gt["orders_by_utc_date"] != gt["orders_by_local_date"],
        f"10-31 本地口径 {local_count}（UTC 口径 {gt['orders_by_utc_date']}）",
    )

    # 3. 比率指标：与手工重算的分子分母一致
    paid = engine.run(
        QueryRequest.build("orders_total", filters=[Filter("status", "=", "paid")])
    ).value
    paid_rate = engine.run(QueryRequest.build("paid_rate")).value
    expected_rate = paid / orders if orders else 0
    add(
        "paid_rate_recomputed",
        abs(paid_rate - expected_rate) < 1e-12,
        f"语义层 {paid_rate:.6f} / 重算 {expected_rate:.6f}",
    )
    avg = engine.run(QueryRequest.build("avg_order_cents")).value
    expected_avg = gt["gmv_cents"] / paid if paid else 0
    add(
        "avg_order_denominator_dedup",
        abs(avg - expected_avg) < 1e-9,
        f"语义层 {avg:,.4f} / 重算 {expected_avg:,.4f}",
    )

    # 4. 非可加/比率上卷必须被拒绝；无维度重算给出正确总量
    region_rows = engine.run(QueryRequest.build("active_suppliers", dimensions=["region"])).rows
    try:
        engine.rollup_total("active_suppliers", region_rows)
        add("non_additive_rollup_rejected", False, "non_additive 指标上卷未被拒绝")
    except SemanticError as exc:
        add("non_additive_rollup_rejected", True, str(exc))

    gmv_rows = engine.run(QueryRequest.build("gmv_cents", dimensions=["region"])).rows
    rolled_gmv = engine.rollup_total("gmv_cents", gmv_rows)
    add(
        "additive_rollup_allowed",
        rolled_gmv == gmv,
        f"可加指标区域求和 {rolled_gmv:,} == 无维度重算 {gmv:,}",
    )

    try:
        engine.rollup_total("paid_rate", [{"paid_rate": 0.5}])
        add("ratio_rollup_rejected", False, "比率指标上卷未被拒绝")
    except SemanticError as exc:
        add("ratio_rollup_rejected", True, str(exc))

    # 5. 同环比：平移窗口重算 == 直接查询平移窗口
    start = datetime(2026, 10, 1)
    end = datetime(2026, 10, 8)
    cmp_res = engine.run(
        QueryRequest.build("gmv_cents", start=start, end=end, compare="previous_period")
    )
    direct = engine.run(
        QueryRequest.build("gmv_cents", start=start - timedelta(days=7), end=end - timedelta(days=7))
    ).value
    cmp_value = cmp_res.comparisons["previous_period"][0]["gmv_cents"]
    add(
        "compare_window_recomputed",
        cmp_value == direct and cmp_res.value == engine.run(
            QueryRequest.build("gmv_cents", start=start, end=end)
        ).value,
        f"同环比重算 {cmp_value} == 平移窗口直查 {direct}",
    )

    # 6. 语义拒绝路径
    def expect_rejected(label: str, builder) -> None:
        try:
            engine.run(builder())
            add(label, False, "非法请求未被拒绝")
        except SemanticError as exc:
            add(label, True, f"{type(exc).__name__}: {exc}")

    expect_rejected("unknown_dimension_rejected", lambda: QueryRequest.build("gmv_cents", dimensions=["evil_dim"]))
    expect_rejected("unknown_metric_rejected", lambda: QueryRequest.build("does_not_exist"))
    expect_rejected("bad_operator_rejected", lambda: QueryRequest.build(
        "orders_total", filters=[Filter("status", "drop table", "x")]))
    expect_rejected("bad_granularity_rejected", lambda: QueryRequest.build(
        "orders_total", granularity="century"))
    expect_rejected("compare_without_window_rejected", lambda: QueryRequest.build(
        "gmv_cents", compare="previous_period"))

    # 7. 参数化安全：请求值只能出现在 params，不能内联进 SQL
    plan = catalog.compiler.compile(
        QueryRequest.build("orders_total", filters=[Filter("status", "=", "paid' OR '1'='1")])
    )
    add(
        "parameterized_sql",
        "paid' OR '1'='1" not in plan.sql and "?" in plan.sql,
        f"过滤值仅作为绑定参数：{plan.sql}",
    )

    # 8. 语义版本
    add(
        "schema_version_v2",
        catalog.schema_version == 2,
        f"当前 schema 版本 {catalog.schema_version}，指标 valid_from 最小值 "
        f"{min(m.valid_from for m in catalog.metrics.values())}",
    )

    if args.json:
        print(json.dumps({"ground_truth": gt, "checks": checks}, ensure_ascii=False, indent=2))
    else:
        print("== 权威口径 ==")
        for k, v in gt.items():
            print(f"  {k:26} {v:>18,}")
        print("\n== 语义层口径断言 ==")
        for c in checks:
            print(f"  [{'PASS' if c['ok'] else 'FAIL'}] {c['name']}: {c['detail']}")
        failed = sum(1 for c in checks if not c["ok"])
        print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")

    return 0 if all(c["ok"] for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
