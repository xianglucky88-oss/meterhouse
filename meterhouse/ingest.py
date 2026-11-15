"""列式分析引擎的加载与校验。

职责
----
1. 按 `storage.LAYOUTS` 在 ClickHouse 建表（分区 / 排序键 / skip index / 投影）。
2. 从 DuckDB 明细按批次灌入 ClickHouse，**原始事实逐行保留**并登记台账。
3. 灌入后做行数/校验和比对，差异不静默吞掉。

批次一致性
----------
同一 `batch_id` 在两边一致；`ingest_batches` 记录每批的行数与版本区间。
查询侧不需要读台账，但出了口径问题时可以按批次反查"当时灌了哪些行"。
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from .data import iter_fact_rows
from .engine import ClickHouseEngine, Engine
from .errors import BatchAlreadyLoaded, EngineError
from .storage import INGEST_LEDGER, LAYOUTS, FactTableLayout
from .sql.dialect import dialect_for

log = logging.getLogger("meterhouse.ingest")

CH_TYPES = {
    "string": "String",
    "int64": "UInt64",
    "bool": "Bool",
    "datetime": "DateTime('UTC')",
}


def _ingest_ledger_ddl(table: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {table} (
        batch_id String,
        source LowCardinality(String),
        table_name LowCardinality(String),
        rows_loaded UInt64,
        min_version UInt64,
        max_version UInt64,
        loaded_at DateTime('UTC')
    ) ENGINE = MergeTree ORDER BY (loaded_at, batch_id)"""


def clickhouse_insert_type(layout: FactTableLayout, name: str) -> str:
    for col_name, logical in layout.columns:
        if col_name == name:
            # LowCardinality 只用于维度列；版本/时间列用原生类型。
            if logical == "string" and col_name in layout.skip_index_columns:
                return "LowCardinality(String)"
            return CH_TYPES[logical]
    raise KeyError(name)


class ClickHouseLoader:
    """把 DuckDB 明细同步到 ClickHouse。"""

    def __init__(self, engine: ClickHouseEngine, database: str | None = None) -> None:
        self.engine = engine
        self.database = database or engine.database

    # ------------------------------------------------------------------ DDL
    def ensure_schema(self) -> list[str]:
        self.engine.ensure_database()
        dialect = dialect_for("clickhouse")
        statements: list[str] = []
        for layout in LAYOUTS.values():
            statements.extend(dialect.create_fact_table(layout, self.database))
        statements.append(_ingest_ledger_ddl(f"{self.database}.{INGEST_LEDGER}"))
        for stmt in statements:
            self._execute_ddl(stmt)
        return statements

    def _execute_ddl(self, sql: str) -> None:
        client = self.engine._get_client()
        try:
            client.command(sql)
        except Exception as exc:  # noqa: BLE001
            raise EngineError(str(exc), engine="clickhouse") from exc

    def _batch_already_loaded(self, batch_id: str, table: str) -> bool:
        count = self.engine._get_client().command(
            f"SELECT count() FROM {self.database}.{INGEST_LEDGER} "
            "WHERE batch_id = {batch:String} AND table_name = {table:String}",
            parameters={"batch": batch_id, "table": table},
        )
        return int(count) > 0

    # ------------------------------------------------------------------ 加载
    def load_table(
        self,
        source_con,
        table: str,
        *,
        batch_id: str | None = None,
        truncate: bool = False,
    ) -> dict[str, Any]:
        layout = LAYOUTS[table]
        if truncate:
            self._execute_ddl(f"TRUNCATE TABLE IF EXISTS {self.database}.{table}")
        # append 模式必须按批次幂等：重复投递同一批不能把事实翻倍。
        if batch_id and not truncate and self._batch_already_loaded(batch_id, table):
            raise BatchAlreadyLoaded(
                f"batch {batch_id} already loaded into {table}; "
                f"refusing to append it twice (raw facts would double)"
            )
        batch_id = batch_id or self._next_batch(table)

        rows = list(iter_fact_rows(source_con, table))
        if not rows:
            return {"table": table, "rows": 0, "batch_id": batch_id}

        client = self.engine._get_client()
        columns = layout.column_names()
        column_types = [clickhouse_insert_type(layout, c) for c in columns]
        try:
            client.insert(
                f"{self.database}.{table}",
                rows,
                column_names=columns,
                column_type_names=column_types,
            )
        except Exception as exc:  # noqa: BLE001
            raise EngineError(f"insert into {table} failed: {exc}", engine="clickhouse") from exc

        versions = [r[columns.index(layout.version_column)] for r in rows]
        self._execute_ddl(
            f"INSERT INTO {self.database}.{INGEST_LEDGER} "
            f"(batch_id, source, table_name, rows_loaded, min_version, max_version, loaded_at) VALUES "
            f"('{batch_id}', 'duckdb', '{table}', {len(rows)}, {min(versions)}, {max(versions)}, now())"
        )
        return {
            "table": table,
            "rows": len(rows),
            "batch_id": batch_id,
            "min_version": min(versions),
            "max_version": max(versions),
        }

    def sync_all(self, source_con, *, truncate: bool = True) -> dict[str, Any]:
        results = []
        for table in LAYOUTS:
            results.append(self.load_table(source_con, table, truncate=truncate))
        return {"loaded": results, "counts": self.row_counts()}

    # ------------------------------------------------------------------ 校验
    def row_counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        client = self.engine._get_client()
        for table in LAYOUTS:
            out[table] = int(
                client.command(f"SELECT count() FROM {self.database}.{table}")
            )
        return out

    def checksums(self, source_con) -> dict[str, dict[str, Any]]:
        """两端的行数与业务列校验和，用于确认"逐行一致"。"""
        client = self.engine._get_client()
        report: dict[str, dict[str, Any]] = {}
        for table, layout in LAYOUTS.items():
            cols = layout.column_names()
            numeric = [c for c, t in layout.columns if t == "int64"]
            sql = (
                f"SELECT count() AS n, "
                + ", ".join(f"sum({c}) AS {c}" for c in numeric)
                + f" FROM {self.database}.{table}"
            )
            ch_row = client.query(sql).result_rows[0]
            duck_row = source_con.execute(
                f"SELECT count(), "
                + ", ".join(f"sum({c})" for c in numeric)
                + f" FROM {table}"
            ).fetchone()
            report[table] = {
                "duckdb": dict(zip(["n", *numeric], duck_row)),
                "clickhouse": dict(zip(["n", *numeric], ch_row)),
            }
        return report

    def verify(self, source_con) -> dict[str, Any]:
        checks = self.checksums(source_con)
        mismatches = {
            table: data
            for table, data in checks.items()
            if data["duckdb"] != data["clickhouse"]
        }
        return {"ok": not mismatches, "tables": checks, "mismatches": mismatches}

    # ------------------------------------------------------------------ 内部
    def _next_batch(self, table: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return f"ch-{table}-{stamp}"
