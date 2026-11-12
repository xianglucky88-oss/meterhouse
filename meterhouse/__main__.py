"""命令行入口。

    python -m meterhouse seed      生成示例数据
    python -m meterhouse verify    口径断言
    python -m meterhouse parity    打印三份口径对比
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import settings
from .data import connect, ground_truth, seed
from .queries import QUERIES


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
    a = con.execute(QUERIES["active_suppliers_a"]).fetchone()[0]
    b = con.execute(QUERIES["active_suppliers_b"]).fetchone()[0]
    c_rows = con.execute(QUERIES["active_suppliers_c"]).fetchall()
    c = sum(r[1] for r in c_rows)
    gt = ground_truth(con, settings.biz_tz_offset_hours)
    out = {
        "A_dashboard_summary": a,
        "B_dashboard_trend": round(float(b or 0), 2),
        "C_region_report": c,
        "ground_truth": gt["active_suppliers"],
        "spread_pct": round(
            (max(a, b, c) - min(a, b, c)) / max(a, b, c) * 100, 2
        ),
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_verify(args) -> int:
    from .data import ground_truth

    con = connect(settings.db_path)
    if con.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 0:
        print("database is empty; run `python -m meterhouse seed` first", file=sys.stderr)
        return 1
    gt = ground_truth(con, settings.biz_tz_offset_hours)
    a = con.execute(QUERIES["active_suppliers_a"]).fetchone()[0]
    b = con.execute(QUERIES["active_suppliers_b"]).fetchone()[0]
    c_rows = con.execute(QUERIES["active_suppliers_c"]).fetchall()
    c = sum(r[1] for r in c_rows)
    vals = [float(a), float(b or 0), float(c)]
    spread = (max(vals) - min(vals)) / max(vals)
    print(json.dumps({"ground_truth": gt, "values": {"A": a, "B": b, "C": c}, "spread": spread}, ensure_ascii=False, indent=2))
    if spread > 0.01:
        print(f"\n各口径极差 {spread:.1%}（阈值 1%）", file=sys.stderr)
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
