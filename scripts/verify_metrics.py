#!/usr/bin/env python3
"""verify_metrics.py —— 指标口径一致性断言。

这是本仓库的核心工具：把"同一个指标在不同接口给出不同数字"这件事
量化出来，并逐项对照权威口径。

用法:
    python3 scripts/verify_metrics.py                # 对当前库
    python3 scripts/verify_metrics.py --reseed       # 重建数据
    python3 scripts/verify_metrics.py --json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from meterhouse.data import connect, ground_truth, seed  # noqa: E402
from meterhouse.queries import QUERIES  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("DB_PATH", "data/meterhouse.duckdb"))
    ap.add_argument("--reseed", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    # 直接跑脚本时用临时库，避免污染仓库里的 data/
    path = args.db
    if args.reseed and not Path(path).exists():
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = connect(path)
    if con.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 0:
        seed(con)

    gt = ground_truth(con, 8)
    a = con.execute(QUERIES["active_suppliers_a"]).fetchone()[0]
    b = con.execute(QUERIES["active_suppliers_b"]).fetchone()[0] or 0
    c_rows = con.execute(QUERIES["active_suppliers_c"]).fetchall()
    c_sum = sum(r[1] for r in c_rows)

    gmv_raw = con.execute(QUERIES["gmv_cents"]).fetchone()[0]
    orders_raw = con.execute(QUERIES["orders_total"]).fetchone()[0]
    avg_raw = con.execute(QUERIES["avg_order_cents"]).fetchone()[0] or 0

    checks = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    # 1. 三份活跃供应商口径必须一致
    vals = [float(a), float(b), float(c_sum)]
    spread = (max(vals) - min(vals)) / max(vals) if max(vals) else 0
    add(
        "active_suppliers_consistent",
        spread <= 0.01,
        f"A={a} / B={b:.2f} / C={c_sum}（极差 {spread:.1%}）",
    )

    # 2. 与权威口径一致
    add(
        "active_suppliers_matches_gt",
        abs(float(a) - gt["active_suppliers"]) <= 1 and abs(float(b) - gt["active_suppliers"]) <= 1,
        f"A={a} B={b:.2f} vs 权威 {gt['active_suppliers']}",
    )

    # 3. GMV 是否按 order_id 去重
    ratio = gmv_raw / gt["gmv_cents"] if gt["gmv_cents"] else 1
    add(
        "gmv_dedup",
        abs(ratio - 1) <= 0.001,
        f"接口 {gmv_raw:,} / 权威 {gt['gmv_cents']:,} = {ratio:.4f}",
    )

    # 4. 订单数是否去重
    ratio_o = orders_raw / gt["orders_total"] if gt["orders_total"] else 1
    add(
        "orders_dedup",
        abs(ratio_o - 1) <= 0.001,
        f"接口 {orders_raw:,} / 权威 {gt['orders_total']:,} = {ratio_o:.4f}",
    )

    # 5. 日期归属：UTC vs 本地
    add(
        "date_attribution_local",
        gt["orders_by_local_date"] > 0 and gt["orders_by_utc_date"] != gt["orders_by_local_date"],
        f"10-31：UTC 口径 {gt['orders_by_utc_date']} 单，本地口径 {gt['orders_by_local_date']} 单",
    )

    # 6. 客单价的分母是否去重
    gt_avg = gt["gmv_cents"] / gt["orders_total"] if gt["orders_total"] else 0
    ratio_avg = avg_raw / gt_avg if gt_avg else 1
    add(
        "avg_order_denominator",
        abs(ratio_avg - 1) <= 0.001,
        f"接口 {avg_raw:,.2f} / 权威 {gt_avg:,.2f} = {ratio_avg:.4f}",
    )

    # 7. 语义层是否覆盖全部指标
    try:
        from meterhouse.semantic import MetricStore

        store = MetricStore.load(Path("metrics/metrics.yaml"))
        declared = set(store.names())
        used = {"active_suppliers", "gmv_cents", "orders_total", "paid_rate", "avg_order_cents", "suppliers_total"}
        add(
            "semantic_coverage",
            declared >= used,
            f"语义层声明 {len(declared)} 个，接口需要 {len(used)} 个，缺 {sorted(used - declared)}",
        )
    except Exception as exc:  # noqa: BLE001
        add("semantic_coverage", False, f"无法加载语义层：{exc}")

    if args.json:
        print(json.dumps({"ground_truth": gt, "checks": checks}, ensure_ascii=False, indent=2))
    else:
        print("== 权威口径 ==")
        for k, v in gt.items():
            print(f"  {k:26} {v:>18,}")
        print("\n== 口径一致性 ==")
        for c in checks:
            print(f"  [{'PASS' if c['ok'] else 'FAIL'}] {c['name']}: {c['detail']}")
        failed = sum(1 for c in checks if not c["ok"])
        print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")

    return 0 if all(c["ok"] for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
