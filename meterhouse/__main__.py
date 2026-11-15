"""命令行入口。

    python -m meterhouse seed                 生成示例数据
    python -m meterhouse verify               口径断言（全部走语义层）
    python -m meterhouse parity               打印两引擎口径对比
    python -m meterhouse compile [METRIC]     打印编译后的 SQL（两种方言）
    python -m meterhouse sync                 明细同步到 ClickHouse
    python -m meterhouse check-parity         校验两引擎明细逐行一致
    python -m meterhouse cutover ...          逐接口切流
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime

from .config import settings
from .data import connect, ground_truth, seed
from .semantic import MetricStore, TimeWindow


def _engine(name: str):
    from .engine import CapacityPolicy, ClickHouseEngine, DuckDBEngine

    if name == "duckdb":
        return DuckDBEngine(path=str(settings.db_path))
    if not settings.clickhouse_enabled:
        raise SystemExit("CLICKHOUSE_HOST is not configured")
    return ClickHouseEngine(
        host=settings.ch_host,
        port=settings.ch_port,
        username=settings.ch_user,
        password=settings.ch_password,
        database=settings.ch_database,
        connect_timeout_s=settings.ch_timeout_s,
        policy=CapacityPolicy(max_result_rows=settings.max_result_rows),
    )


def _store() -> MetricStore:
    return MetricStore.load(settings.metrics_file, default_database=settings.ch_database)


def cmd_seed(args) -> int:
    settings.ensure()
    con = connect(str(settings.db_path))
    out = seed(con, seed_value=args.seed)
    con.close()
    print(json.dumps({"seeded": out}, ensure_ascii=False, indent=2))
    return 0


def cmd_verify(args) -> int:
    """逐指标对照权威口径。

    全部经由语义层编译执行（与线上接口同一条路径），既能验证口径，
    也能验证编译器没有引入偏差。
    """
    store = _store()
    con = connect(str(settings.db_path))
    try:
        if con.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 0:
            print("database is empty; run `python -m meterhouse seed` first", file=sys.stderr)
            return 1
        gt = ground_truth(con, settings.biz_tz_offset_hours)
        checks = []
        for name in store.names():
            if name not in gt:
                continue
            compiled = store.compile_query(name, engine="duckdb")
            got = con.execute(compiled.sql).fetchone()[0]
            expected = gt[name]
            ok = _close(got, expected)
            checks.append(
                {
                    "metric": name,
                    "ok": ok,
                    "value": got,
                    "ground_truth": expected,
                    "definition_version": compiled.definition_version,
                }
            )
        # 分组加总必须等于整体（去重指标在目标粒度重算，不能相加，因此只对
        # additive 指标做这项检查）。
        for name in store.names():
            metric = store.get(name)
            if metric.additivity != "additive":
                continue
            whole = con.execute(store.compile_query(name, engine="duckdb").sql).fetchone()[0]
            grouped = store.compile_query(
                name, engine="duckdb", dimensions=metric.dimensions[:1]
            )
            rows = con.execute(grouped.sql).fetchall()
            total = sum(r[-1] for r in rows)
            checks.append(
                {
                    "metric": f"{name}.grouped_sum",
                    "ok": _close(total, whole),
                    "value": total,
                    "ground_truth": whole,
                    "definition_version": metric.definition_version,
                }
            )
    finally:
        con.close()

    if args.json:
        print(json.dumps({"checks": checks}, ensure_ascii=False, indent=2, default=str))
    else:
        print("== 语义层口径 ==")
        for c in checks:
            mark = "PASS" if c["ok"] else "FAIL"
            print(
                f"  [{mark}] {c['metric']:34} value={c['value']!r:>18} "
                f"truth={c['ground_truth']!r}"
            )
        failed = sum(1 for c in checks if not c["ok"])
        print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")
    return 0 if all(c["ok"] for c in checks) else 1


def _close(a, b, tol: float = 1e-6) -> bool:
    if a is None or b is None:
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        denom = max(abs(float(a)), abs(float(b)), 1e-9)
        return abs(float(a) - float(b)) / denom <= tol
    return a == b


def cmd_parity(args) -> int:
    """两引擎并排对比（引擎可用时）。"""
    store = _store()
    engines = {"duckdb": _engine("duckdb")}
    if settings.clickhouse_enabled:
        engines["clickhouse"] = _engine("clickhouse")
    out: dict[str, dict[str, object]] = {}
    for name in store.names():
        row: dict[str, object] = {}
        for engine_name, engine in engines.items():
            compiled = store.compile_query(name, engine=engine_name)
            try:
                row[engine_name] = engine.query(compiled.sql, interface="parity").scalar()
            except Exception as exc:  # noqa: BLE001
                row[engine_name] = f"ERROR: {exc}"
        out[name] = row
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_compile(args) -> int:
    store = _store()
    names = [args.metric] if args.metric else store.names()
    window = TimeWindow(
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
        granularity=args.granularity,
    )
    out = {}
    for name in names:
        out[name] = {
            engine_name: store.compile_query(
                name,
                engine=engine_name,
                dimensions=[d for d in (args.dimensions or "").split(",") if d],
                time_window=window,
            ).sql
            for engine_name in ("duckdb", "clickhouse")
        }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_sync(args) -> int:
    from .ingest import ClickHouseLoader

    loader = ClickHouseLoader(_engine("clickhouse"))
    con = connect(str(settings.db_path))
    try:
        loader.ensure_schema()
        out = loader.sync_all(con, truncate=not args.append)
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        con.close()
    return 0


def cmd_check_parity(args) -> int:
    from .ingest import ClickHouseLoader

    loader = ClickHouseLoader(_engine("clickhouse"))
    con = connect(str(settings.db_path))
    try:
        report = loader.verify(con)
    finally:
        con.close()
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report["ok"] else 1


def cmd_cutover(args) -> int:
    from .api import INTERFACES
    from .routing import CutoverStore

    cutover = CutoverStore(settings.cutover_file, INTERFACES)
    if args.list or not args.interface:
        print(json.dumps(cutover.snapshot(), ensure_ascii=False, indent=2))
        return 0
    if (
        args.mode == "primary"
        and (args.target or cutover.route(args.interface).target) == "clickhouse"
        and not settings.clickhouse_enabled
    ):
        raise SystemExit(
            "refusing to cut over: CLICKHOUSE_HOST is not configured "
            "(nothing was written)"
        )
    updated = cutover.update(
        args.interface,
        mode=args.mode,
        target=args.target,
        control=args.control,
        fallback=args.fallback,
        actor=args.actor,
        reason=args.reason,
    )
    print(json.dumps(updated.__dict__, ensure_ascii=False, indent=2))
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
    v.add_argument("--json", action="store_true")
    v.set_defaults(func=cmd_verify)

    c = sub.add_parser("compile")
    c.add_argument("metric", nargs="?")
    c.add_argument("--dimensions", default="")
    c.add_argument("--granularity")
    c.add_argument("--start")
    c.add_argument("--end")
    c.set_defaults(func=cmd_compile)

    sy = sub.add_parser("sync")
    sy.add_argument("--append", action="store_true")
    sy.set_defaults(func=cmd_sync)

    cp = sub.add_parser("check-parity")
    cp.set_defaults(func=cmd_check_parity)

    cu = sub.add_parser("cutover")
    cu.add_argument("interface", nargs="?")
    cu.add_argument("--mode", choices=["off", "shadow", "primary"])
    cu.add_argument("--target", choices=["duckdb", "clickhouse"])
    cu.add_argument("--control", choices=["duckdb", "clickhouse"])
    cu.add_argument("--fallback", dest="fallback", action="store_true", default=None)
    cu.add_argument("--no-fallback", dest="fallback", action="store_false")
    cu.add_argument("--list", action="store_true")
    cu.add_argument("--actor", default="cli")
    cu.add_argument("--reason", default="")
    cu.set_defaults(func=cmd_cutover)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
