"""数据生成器 + 本地建表。

两张明细表（语义层的事实来源）：

    supplier_stats   供应商小时级活跃快照（UTC 时间戳）
    order_facts      订单事实（金额单位：分）

刻意保留的数据形态（迁移时必须原样带过去，否则口径会变）：
    * stat_time / created_at 是 **UTC**，业务上要按东八区归日；
    * 同一供应商一天内可能有多条快照（每小时一条）；
    * order_facts 存在少量重复 order_id（上游重推）。

每行额外带两个可追溯列：
    ingest_batch_id  该行来自哪一次摄入（与 ingest_batches 台账对应）
    row_version      同主键内的先后序号；去重取最新一行
"""

from __future__ import annotations

import dataclasses
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import duckdb

from .storage import LAYOUTS, ORDER_LAYOUT, SUPPLIER_LAYOUT
from .sql.dialect import dialect_for

REGIONS = ["华东", "华南", "华北", "西南"]
CATEGORIES = ["电子元器件", "包装材料", "物流服务", "办公用品"]

# 旧的 DDL 只保留作为历史兼容（新代码走 storage.LAYOUTS）。
LEGACY_DDL = """
CREATE TABLE IF NOT EXISTS metric_cache (
    name        VARCHAR PRIMARY KEY,
    payload     VARCHAR NOT NULL,
    computed_at TIMESTAMP NOT NULL
);
"""


def connect(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute(LEGACY_DDL)
    _ensure_layout(con)
    return con


def _ensure_layout(con: duckdb.DuckDBPyConnection) -> None:
    """建表；若表已存在但缺新列（老库），就地补齐并回填版本号。"""
    dialect = dialect_for("duckdb")
    existing = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    for layout in LAYOUTS.values():
        if layout.name in existing:
            _migrate_existing(con, layout)
            continue
        for stmt in dialect.create_fact_table(layout):
            con.execute(stmt)
    con.execute(
        """CREATE TABLE IF NOT EXISTS ingest_batches (
            batch_id    VARCHAR NOT NULL,
            source      VARCHAR NOT NULL,
            table_name  VARCHAR NOT NULL,
            rows_loaded BIGINT NOT NULL,
            min_version BIGINT,
            max_version BIGINT,
            loaded_at   TIMESTAMP NOT NULL
        )"""
    )


def _migrate_existing(con: duckdb.DuckDBPyConnection, layout) -> None:
    """老库就地升级。

    DuckDB 不支持给已有表追加生成列，因此缺 `biz_day` 时只能**重建表**：
    新建影子表 → 计算新列后整表拷入 → 原子替换。原表的每一行都原样保留，
    只是补齐了 `ingest_batch_id` / `row_version` / `biz_day`。
    """
    cols = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [layout.name],
        ).fetchall()
    }
    needs_rebuild = "biz_day" not in cols
    if not needs_rebuild:
        # 只需补两列（可原地 ALTER），回填可追溯信息。
        if "ingest_batch_id" not in cols:
            con.execute(f"ALTER TABLE {layout.name} ADD COLUMN ingest_batch_id VARCHAR")
            con.execute(f"UPDATE {layout.name} SET ingest_batch_id = 'legacy'")
        if "row_version" not in cols:
            con.execute(f"ALTER TABLE {layout.name} ADD COLUMN row_version BIGINT")
            con.execute(f"UPDATE {layout.name} SET row_version = rowid")
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{layout.name}_day ON {layout.name} (biz_day)"
        )
        return

    dialect = dialect_for("duckdb")
    # DuckDB 有索引时不允许 RENAME TABLE，且不支持追加生成列。顺序改为：
    #   旧表 → 改名到备份名（此时旧表可能仍带索引，先删索引）
    #   以正式名新建最终布局，拷入数据，再删备份。
    backup = f"_{layout.name}_pre_migration"
    con.execute(f"DROP TABLE IF EXISTS {backup}")
    _drop_indexes(con, layout.name)
    con.execute(f"ALTER TABLE {layout.name} RENAME TO {backup}")

    final_layout = dataclasses.replace(layout, name=layout.name)
    for stmt in dialect.create_fact_table(final_layout):
        con.execute(stmt)

    # 旧表可能缺 ingest_batch_id / row_version；按列存在性构造投影。
    present = cols
    target_cols = [c for c in layout.column_names() if c in present]
    added: dict[str, str] = {}
    if "ingest_batch_id" not in present:
        added["ingest_batch_id"] = "'legacy'"
    if "row_version" not in present:
        added["row_version"] = "rowid"
    insert_cols = [*target_cols, *added]
    select_exprs = [*target_cols, *(f"{expr} AS {name}" for name, expr in added.items())]
    con.execute(
        f"INSERT INTO {layout.name} ({', '.join(insert_cols)}) "
        f"SELECT {', '.join(select_exprs)} FROM {backup}"
    )
    con.execute(f"DROP TABLE {backup}")


def _drop_indexes(con: duckdb.DuckDBPyConnection, table: str) -> None:
    rows = con.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = ?", [table]
    ).fetchall()
    for (index_name,) in rows:
        con.execute(f"DROP INDEX IF EXISTS {index_name}")


# ---------------------------------------------------------------------------
# 生成
# ---------------------------------------------------------------------------


def generate_rows(
    seed_value: int = 20261112, days: int = 60, n_suppliers: int = 80
) -> tuple[list[tuple], list[tuple]]:
    """纯函数：生成 (supplier_rows, order_rows)。

    两个引擎灌的是同一份输出，保证跨引擎逐行一致。
    返回的行顺序即 `row_version`（同主键内自增），批次号在这里固定。
    """
    rnd = random.Random(seed_value)
    base = datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    batch = "seed-" + str(seed_value)

    supplier_rows: list[tuple] = []
    versions: dict[tuple, int] = {}
    for d in range(days):
        day = base + timedelta(days=d)
        for s in range(1, n_suppliers + 1):
            sid = f"SUP-{s:04d}"
            region = REGIONS[s % len(REGIONS)]
            category = CATEGORIES[s % len(CATEGORIES)]
            if rnd.random() >= 0.65:
                continue
            for _ in range(rnd.randint(1, 6)):
                hour = rnd.randint(0, 23)
                ts = day.replace(hour=hour)
                key = (sid, ts)
                versions[key] = versions.get(key, 0) + 1
                supplier_rows.append(
                    (sid, ts, True, region, category, batch, versions[key])
                )

    # 快照天然按时间生成，落库前按 (主键, 版本) 稳定排序，便于两引擎逐行一致。
    supplier_rows.sort(key=lambda r: (r[0], r[1], r[6]))

    order_rows: list[tuple] = []
    order_versions: dict[str, int] = {}
    oid = 1
    for _ in range(6000):
        day = base + timedelta(days=rnd.randint(0, days - 1))
        # 约 12% 落在 UTC 16:00 之后（本地已跨日）
        hour = rnd.randint(16, 23) if rnd.random() < 0.12 else rnd.randint(0, 15)
        supplier = f"SUP-{rnd.randint(1, n_suppliers):04d}"
        order_id = f"ORD-{oid:07d}"
        row = (
            order_id,
            supplier,
            day.replace(hour=hour, minute=rnd.randint(0, 59)),
            rnd.randint(500, 200_000),
            rnd.choice(["paid", "paid", "paid", "refunded"]),
            REGIONS[rnd.randint(0, n_suppliers) % len(REGIONS)],
            batch,
        )
        repeats = 2 if rnd.random() < 0.02 else 1
        for _ in range(repeats):
            order_versions[order_id] = order_versions.get(order_id, 0) + 1
            order_rows.append(row + (order_versions[order_id],))
        oid += 1

    order_rows.sort(key=lambda r: (r[2], r[0], r[7]))
    return supplier_rows, order_rows


def seed(
    con: duckdb.DuckDBPyConnection,
    seed_value: int = 20261112,
    days: int = 60,
    n_suppliers: int = 80,
) -> dict[str, Any]:
    """灌入示例数据。重复调用会先清空。"""
    con.execute("DELETE FROM supplier_stats")
    con.execute("DELETE FROM order_facts")
    con.execute("DELETE FROM ingest_batches")
    supplier_rows, order_rows = generate_rows(seed_value, days, n_suppliers)
    _insert_rows(con, SUPPLIER_LAYOUT.name, supplier_rows, "seed")
    _insert_rows(con, ORDER_LAYOUT.name, order_rows, "seed")
    return {
        "supplier_stats": con.execute("SELECT count(*) FROM supplier_stats").fetchone()[0],
        "order_facts": con.execute("SELECT count(*) FROM order_facts").fetchone()[0],
        "suppliers": n_suppliers,
        "days": days,
    }


def _insert_rows(
    con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple], source: str
) -> None:
    layout = LAYOUTS[table]
    if not rows:
        return
    placeholders = ", ".join("?" for _ in layout.column_names())
    con.executemany(
        f"INSERT INTO {table} ({', '.join(layout.column_names())}) VALUES ({placeholders})",
        rows,
    )
    versions = [r[-1] for r in rows]
    con.execute(
        "INSERT INTO ingest_batches VALUES (?,?,?,?,?,?,?)",
        [
            rows[0][-2],
            source,
            table,
            len(rows),
            min(versions),
            max(versions),
            datetime.now(timezone.utc).replace(tzinfo=None),
        ],
    )


# ---------------------------------------------------------------------------
# 权威口径
# ---------------------------------------------------------------------------


def ground_truth(con: duckdb.DuckDBPyConnection, tz_offset: int = 8) -> dict:
    """各项指标的权威口径（作为回归基准）。

    去重语义：同一 order_id 取最新版本那一行（row_version 最大），
    与语义层的 `dedup_by=order_id` 完全一致。
    """
    gt: dict[str, Any] = {}
    gt["suppliers_total"] = con.execute(
        "SELECT count(DISTINCT supplier_id) FROM supplier_stats"
    ).fetchone()[0]
    gt["orders_total"] = con.execute(
        "SELECT count(DISTINCT order_id) FROM order_facts"
    ).fetchone()[0]
    gt["gmv_cents"] = con.execute(
        """SELECT coalesce(sum(amount_cents), 0) FROM (
             SELECT amount_cents, status FROM order_facts
             QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY row_version DESC) = 1
           ) WHERE status = 'paid'"""
    ).fetchone()[0]
    gt["paid_orders"] = con.execute(
        """SELECT count(*) FROM (
             SELECT order_id, status FROM order_facts
             QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY row_version DESC) = 1
           ) WHERE status = 'paid'"""
    ).fetchone()[0]
    gt["active_suppliers"] = gt["suppliers_total"]
    gt["active_suppliers_daily_avg"] = round(
        con.execute(
            """SELECT avg(cnt) FROM (
                 SELECT biz_day AS d, count(DISTINCT supplier_id) AS cnt
                 FROM supplier_stats WHERE is_active GROUP BY 1)"""
        ).fetchone()[0] or 0,
        4,
    )
    gt["orders_by_local_date"] = con.execute(
        f"""SELECT count(DISTINCT order_id) FROM order_facts
            WHERE CAST(created_at + INTERVAL {tz_offset} HOUR AS DATE)
                  = DATE '2026-10-31'"""
    ).fetchone()[0]
    gt["orders_by_utc_date"] = con.execute(
        """SELECT count(DISTINCT order_id) FROM order_facts
           WHERE CAST(created_at AS DATE) = DATE '2026-10-31'"""
    ).fetchone()[0]
    gt["paid_rate"] = round(
        (gt["paid_orders"] / gt["orders_total"]) if gt["orders_total"] else 0, 6
    )
    gt["avg_order_cents"] = round(
        (gt["gmv_cents"] / gt["paid_orders"]) if gt["paid_orders"] else 0, 2
    )
    return gt


def iter_fact_rows(con: duckdb.DuckDBPyConnection, table: str) -> Iterator[tuple]:
    """按布局列顺序导出明细，用于灌到另一个引擎。"""
    layout = LAYOUTS[table]
    cols = ", ".join(layout.column_names())
    cursor = con.execute(f"SELECT {cols} FROM {table} ORDER BY row_version")
    while True:
        batch = cursor.fetchmany(10_000)
        if not batch:
            break
        yield from batch


def batch_id(prefix: str = "load") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"
