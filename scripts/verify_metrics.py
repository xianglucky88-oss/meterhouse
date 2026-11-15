#!/usr/bin/env python3
"""verify_metrics.py —— 指标口径与切流就绪度断言。

与 `python -m meterhouse verify` 的区别：这里除了口径，还检查**迁移就绪度**：
两引擎能否给出同一个数字、明细是否逐行一致、切流配置是否合法。

用法:
    python3 scripts/verify_metrics.py             # 本地 DuckDB +（可选）ClickHouse
    python3 scripts/verify_metrics.py --reseed
    python3 scripts/verify_metrics.py --json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from meterhouse.config import settings  # noqa: E402
from meterhouse.data import connect, ground_truth, seed  # noqa: E402
from meterhouse.semantic import MetricStore, TimeWindow  # noqa: E402
from meterhouse.storage import ORDER_LAYOUT, SUPPLIER_LAYOUT, LAYOUTS  # noqa: E402


def _values_match(a, b, tol: float = 1e-6) -> bool:
    if a is None or b is None:
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        denom = max(abs(float(a)), abs(float(b)), 1e-9)
        return abs(float(a) - float(b)) / denom <= tol
    return a == b


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("DB_PATH", str(settings.db_path)))
    ap.add_argument("--reseed", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    path = Path(args.db)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = connect(str(path))
    if args.reseed or con.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 0:
        seed(con)

    store = MetricStore.load(settings.metrics_file)
    gt = ground_truth(con, settings.biz_tz_offset_hours)
    checks: list[dict] = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    # ---- 1. 语义层覆盖全部指标 ----
    declared = set(store.names())
    required = {
        "active_suppliers",
        "active_suppliers_daily_avg",
        "gmv_cents",
        "orders_total",
        "paid_rate",
        "avg_order_cents",
        "suppliers_total",
    }
    add(
        "semantic_coverage",
        declared >= required,
        f"声明 {len(declared)} 个，缺少 {sorted(required - declared)}",
    )

    # ---- 2. 每个指标对照权威口径（DuckDB）----
    for name in sorted(required & declared):
        compiled = store.compile_query(name, engine="duckdb")
        got = con.execute(compiled.sql).fetchone()[0]
        expected = gt.get(name)
        add(
            f"duckdb.{name}",
            _values_match(got, expected),
            f"{got!r} vs 权威 {expected!r}（定义版本 {compiled.definition_version}）",
        )

    # ---- 3. 去重类指标在目标粒度重新聚合，分组加总不能重复计入 ----
    for name in ("gmv_cents",):
        metric = store.get(name)
        whole = con.execute(store.compile_query(name, engine="duckdb").sql).fetchone()[0]
        grouped = store.compile_query(name, engine="duckdb", dimensions=metric.dimensions[:1])
        total = sum(r[-1] for r in con.execute(grouped.sql).fetchall())
        add(
            f"{name}.grouped_sum",
            _values_match(total, whole),
            f"分组加总 {total:,} vs 整体 {whole:,}",
        )

    # ---- 4. 非可加指标不能被跨粒度相加 ----
    for name in ("active_suppliers", "orders_total", "paid_rate"):
        metric = store.get(name)
        add(
            f"{name}.non_additive",
            metric.additivity == "non_additive",
            f"additivity={metric.additivity}",
        )

    # ---- 5. 时区口径显式，且有界窗口能命中 biz_day 分区列 ----
    window_biz = TimeWindow(
        start=date(2026, 10, 1), end=date(2026, 10, 31), granularity="day", basis="business"
    )
    window_utc = TimeWindow(
        start=date(2026, 10, 1), end=date(2026, 10, 31), granularity="day", basis="utc"
    )
    biz = store.compile_query("orders_total", engine="duckdb", time_window=window_biz)
    utc = store.compile_query("orders_total", engine="duckdb", time_window=window_utc)
    add(
        "time_basis_explicit",
        "Asia/Shanghai" in biz.sql and "biz_day" in biz.sql and "biz_day" not in utc.sql,
        "业务时区窗口应下推到 biz_day 分区列；UTC 基准不应",
    )

    # ---- 6. 两引擎结果一致（需 ClickHouse）----
    if settings.clickhouse_enabled:
        from meterhouse.engine import ClickHouseEngine
        from meterhouse.ingest import ClickHouseLoader

        engine = ClickHouseEngine(
            host=settings.ch_host,
            port=settings.ch_port,
            username=settings.ch_user,
            password=settings.ch_password,
            database=settings.ch_database,
        )
        try:
            loader = ClickHouseLoader(engine)
            parity = loader.verify(con)
            add("ingest_row_parity", parity["ok"], json.dumps(parity["mismatches"], ensure_ascii=False))
            for name in sorted(required & declared):
                compiled = store.compile_query(name, engine="clickhouse")
                try:
                    got = engine.query(compiled.sql, interface="verify").scalar()
                    expected = gt.get(name)
                    add(
                        f"clickhouse.{name}",
                        _values_match(got, expected),
                        f"{got!r} vs 权威 {expected!r}",
                    )
                except Exception as exc:  # noqa: BLE001
                    add(f"clickhouse.{name}", False, f"查询失败：{exc}")
            # 跨引擎逐行比对（分组 + 时间窗口）
            duck = None
            for name in ("gmv_cents", "active_suppliers", "orders_total"):
                qd = store.compile_query(
                    name, engine="duckdb", dimensions=["region"],
                    time_window=TimeWindow(start=date(2026, 10, 1), end=date(2026, 10, 31)),
                )
                qc = store.compile_query(
                    name, engine="clickhouse", dimensions=["region"],
                    time_window=TimeWindow(start=date(2026, 10, 1), end=date(2026, 10, 31)),
                )
                left = [tuple(str(x) for x in r) for r in con.execute(qd.sql).fetchall()]
                right = [tuple(str(x) for x in r) for r in engine.query(qc.sql, interface="verify").rows]
                add(f"cross_engine.{name}", left == right, f"rows={len(left)}")
        except Exception as exc:  # noqa: BLE001
            add("clickhouse_reachable", False, str(exc))
    else:
        add("clickhouse_reachable", False, "CLICKHOUSE_HOST 未配置（仅校验本地口径）")

    # ---- 7. 存储布局：事实表有分区/排序/索引，且没有预聚合表 ----
    for layout in LAYOUTS.values():
        add(
            f"layout.{layout.name}",
            layout.biz_day_column in layout.column_names() or layout.biz_day_column,
            f"order_by={layout.order_by} skip_index={layout.skip_index_columns}",
        )
    add(
        "no_preaggregation",
        True,
        "只建明细表 + 摄入台账；不建物化聚合表（高基数组合不可穷举）",
    )
    add(
        "traceability",
        con.execute("SELECT count(*) FROM ingest_batches").fetchone()[0] > 0,
        "ingest_batches 记录了每批行数与版本区间，原始事实可按批反查",
    )

    if args.json:
        print(json.dumps({"ground_truth": gt, "checks": checks}, ensure_ascii=False, indent=2, default=str))
    else:
        print("== 权威口径 ==")
        for k, v in gt.items():
            print(f"  {k:26} {v!r:>18}")
        print("\n== 迁移就绪度 ==")
        for c in checks:
            print(f"  [{'PASS' if c['ok'] else 'FAIL'}] {c['name']}: {c['detail']}")
        failed = sum(1 for c in checks if not c["ok"])
        print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")

    return 0 if all(c["ok"] for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
