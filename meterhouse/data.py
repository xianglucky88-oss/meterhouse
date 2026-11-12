"""数据生成器 + 建表。

两张明细表：

    supplier_stats   供应商小时级活跃快照（UTC 时间戳）
    order_facts      订单事实（金额单位：分）

刻意保留的数据形态：
    * supplier_stats 的 stat_time 是 **UTC**，业务上要按东八区归日；
    * 同一供应商一天内可能有多条快照（每小时一条）；
    * order_facts 存在少量重复 order_id（上游重推）。

数据形态如下。
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import duckdb

DDL = """
CREATE TABLE IF NOT EXISTS supplier_stats (
    supplier_id   VARCHAR NOT NULL,
    stat_time     TIMESTAMP NOT NULL,   -- UTC
    is_active     BOOLEAN NOT NULL,
    region        VARCHAR NOT NULL,
    category      VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS order_facts (
    order_id      VARCHAR NOT NULL,
    supplier_id   VARCHAR NOT NULL,
    created_at    TIMESTAMP NOT NULL,   -- UTC
    amount_cents  BIGINT  NOT NULL,
    status        VARCHAR NOT NULL,
    region        VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS metric_cache (
    name        VARCHAR PRIMARY KEY,
    payload     VARCHAR NOT NULL,
    computed_at TIMESTAMP NOT NULL
);
"""

REGIONS = ["华东", "华南", "华北", "西南"]
CATEGORIES = ["电子元器件", "包装材料", "物流服务", "办公用品"]


def connect(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute(DDL)
    return con


def seed(con: duckdb.DuckDBPyConnection, seed_value: int = 20261112, days: int = 60, n_suppliers: int = 80) -> dict:
    """灌入示例数据。重复调用会先清空。"""
    rnd = random.Random(seed_value)
    con.execute("DELETE FROM supplier_stats")
    con.execute("DELETE FROM order_facts")

    base = datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)

    sup_rows = []
    for d in range(days):
        day = base + timedelta(days=d)
        for s in range(1, n_suppliers + 1):
            sid = f"SUP-{s:04d}"
            region = REGIONS[s % len(REGIONS)]
            category = CATEGORIES[s % len(CATEGORIES)]
            # 不是每个供应商每天都活跃
            active_today = rnd.random() < 0.65
            if not active_today:
                continue
            # 每天 1~6 条小时快照
            for _ in range(rnd.randint(1, 6)):
                hour = rnd.randint(0, 23)
                sup_rows.append(
                    (
                        sid,
                        day.replace(hour=hour),
                        True,
                        region,
                        category,
                    )
                )

    if sup_rows:
        con.executemany(
            "INSERT INTO supplier_stats VALUES (?,?,?,?,?)", sup_rows
        )

    order_rows = []
    oid = 1
    for _ in range(6000):
        day = base + timedelta(days=rnd.randint(0, days - 1))
        # 约 3% 是深夜（UTC 16:00 之后，本地已跨日）
        hour = rnd.randint(16, 23) if rnd.random() < 0.12 else rnd.randint(0, 15)
        supplier = f"SUP-{rnd.randint(1, n_suppliers):04d}"
        row = (
            f"ORD-{oid:07d}",
            supplier,
            day.replace(hour=hour, minute=rnd.randint(0, 59)),
            rnd.randint(500, 200_000),
            rnd.choice(["paid", "paid", "paid", "refunded"]),
            REGIONS[rnd.randint(0, n_suppliers) % len(REGIONS)],
        )
        order_rows.append(row)
        # 约 2% 重复推送
        if rnd.random() < 0.02:
            order_rows.append(row)
        oid += 1

    con.executemany("INSERT INTO order_facts VALUES (?,?,?,?,?,?)", order_rows)

    return {
        "supplier_stats": con.execute("SELECT count(*) FROM supplier_stats").fetchone()[0],
        "order_facts": con.execute("SELECT count(*) FROM order_facts").fetchone()[0],
        "suppliers": n_suppliers,
        "days": days,
    }


def ground_truth(con: duckdb.DuckDBPyConnection, tz_offset: int = 8) -> dict:
    """各项指标的权威口径（作为回归基准）。

    这些数字就是"正确答案"，任何接口口径都应与之一致。
    """
    gt = {}
    gt["suppliers_total"] = con.execute(
        "SELECT count(DISTINCT supplier_id) FROM supplier_stats"
    ).fetchone()[0]
    gt["orders_total"] = con.execute(
        "SELECT count(DISTINCT order_id) FROM order_facts"
    ).fetchone()[0]
    gt["gmv_cents"] = con.execute(
        "SELECT coalesce(sum(amount_cents),0) FROM "
        "(SELECT order_id, max(amount_cents) AS amount_cents FROM order_facts "
        " WHERE status='paid' GROUP BY order_id)"
    ).fetchone()[0]
    # 活跃供应商数（按本地业务日去重）：任一天有快照即算活跃
    gt["active_suppliers"] = gt["suppliers_total"]
    # 按本地日期归属的订单数
    gt["orders_by_local_date"] = con.execute(
        f"""SELECT count(DISTINCT order_id) FROM order_facts
            WHERE CAST(created_at + INTERVAL {tz_offset} HOUR AS DATE)
                  = DATE '2026-10-31'"""
    ).fetchone()[0]
    # 按 UTC 日期归属（错误口径）
    gt["orders_by_utc_date"] = con.execute(
        """SELECT count(DISTINCT order_id) FROM order_facts
           WHERE CAST(created_at AS DATE) = DATE '2026-10-31'"""
    ).fetchone()[0]
    return gt
