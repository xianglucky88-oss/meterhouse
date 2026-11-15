"""SQL 方言适配层。

指标语义层只产出**逻辑表达式**，不直接产出目标库 SQL。方言负责：

* 时间：UTC 存储 → 业务时区的归日 / 落粒度；
* 聚合：条件聚合、去重计数；
* 去重：事实表按主键取最新版本；
* DDL：分区键、排序键、skip index、投影（两者差异最大）。

引擎的能力差异（高并发读取所依赖的部分）都收敛在这里，编译器不需要知道
"ClickHouse 用 LIMIT 1 BY、DuckDB 用 row_number()"这类细节。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

SUPPORTED_GRANULARITIES = ("day", "week", "month", "quarter", "year")

# 逻辑列类型 → 方言物理列类型
LOGICAL_TYPES = ("string", "int64", "bool", "datetime")


def _lit(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


@dataclass
class FactTableLayout:
    """事实表的存储布局声明（唯一定义处，两种引擎分别翻译）。

    高并发读取依赖的三个决定：

    * `biz_day`：把 UTC 时间落到业务日，作为**分区键**与**排序键首列**，
      让"按日期/区间过滤"能直接裁剪分区与 granule，而不是全表扫；
    * `skip_indexes`：给高基数维度（区域、供应商）加 bloom/跳数索引；
    * `projections`：为每个高频维度组合预建排序，避免运行期大排序。

    这里刻意**不做**任何预聚合（不建物化聚合表）：高基数维度的组合
    不可能穷举，预聚合只会把基数问题藏起来。查询始终从去重后的明细算起。
    """

    name: str
    columns: list[tuple[str, str]]  # (name, logical type)
    time_column: str
    biz_tz: str
    key_columns: list[str]
    biz_day_column: str = "biz_day"
    partition_month: bool = True
    order_by: list[str] = field(default_factory=list)
    skip_index_columns: list[str] = field(default_factory=list)
    projection_dimensions: list[list[str]] = field(default_factory=list)
    version_column: str = "row_version"

    def column_names(self) -> list[str]:
        return [n for n, _ in self.columns]

    def payload_columns(self) -> list[str]:
        return [
            n
            for n, _ in self.columns
            if n not in (self.biz_day_column,)
        ]


class Dialect(ABC):
    name: str

    # ------------------------------------------------------------------ 字面量
    @abstractmethod
    def instant_literal(self, dt: datetime) -> str:
        """UTC 瞬时字面量。"""

    @abstractmethod
    def date_literal(self, d: date) -> str:
        """日期字面量。"""

    # ------------------------------------------------------------------ 时间
    @abstractmethod
    def local_time_expr(self, col: str, tz: str) -> str:
        """UTC 时间列 → 业务时区墙钟时间。"""

    @abstractmethod
    def bucket_expr(self, col: str, tz: str | None, granularity: str) -> str:
        """落脚到粒度。tz=None 表示按存储时区（UTC）归桶。"""

    @abstractmethod
    def local_day_expr(self, col: str, tz: str) -> str:
        """按业务时区取日期。"""

    # ------------------------------------------------------------------ 聚合
    @abstractmethod
    def sum_cond(self, col: str, condition: str | None) -> str: ...

    @abstractmethod
    def count_star_cond(self, condition: str | None) -> str: ...

    @abstractmethod
    def count_distinct(self, col: str, condition: str | None) -> str: ...

    @abstractmethod
    def avg_cond(self, col: str, condition: str | None) -> str: ...

    @abstractmethod
    def max_cond(self, col: str, condition: str | None) -> str: ...

    @abstractmethod
    def divide_null_safe(self, numerator: str, denominator: str) -> str: ...

    @abstractmethod
    def string_concat(self, *parts: str) -> str: ...

    # ------------------------------------------------------------- 去重子查询
    @abstractmethod
    def dedup(
        self,
        table: str,
        columns: list[str],
        key_columns: list[str],
        version_expr: str,
        inner_where: str | None,
    ) -> str:
        """返回已按 key 去重的派生表 SQL（可嵌套在 FROM 后）。"""

    # ------------------------------------------------------------------ DDL
    @abstractmethod
    def create_fact_table(self, layout: FactTableLayout, database: str | None = None) -> list[str]:
        """建表语句列表（含投影/索引）。"""

    @abstractmethod
    def physical_type(self, logical: str) -> str: ...

    def quote_ident(self, name: str) -> str:
        return name

    def qualified(self, table: str, database: str | None) -> str:
        return f"{database}.{table}" if database else table


class DuckDBDialect(Dialect):
    name = "duckdb"

    def instant_literal(self, dt: datetime) -> str:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"

    def date_literal(self, d: date) -> str:
        return f"DATE '{d.isoformat()}'"

    def local_time_expr(self, col: str, tz: str) -> str:
        # TIMESTAMP 视为 UTC 墙钟：先标 UTC 瞬时，再转业务时区墙钟。
        return f"(({col} AT TIME ZONE 'UTC') AT TIME ZONE {_lit(tz)})"

    def local_day_expr(self, col: str, tz: str) -> str:
        return f"CAST({self.local_time_expr(col, tz)} AS DATE)"

    def bucket_expr(self, col: str, tz: str | None, granularity: str) -> str:
        expr = self.local_time_expr(col, tz) if tz else col
        if granularity == "day":
            return f"CAST({expr} AS DATE)"
        if granularity not in SUPPORTED_GRANULARITIES:
            raise ValueError(f"unsupported granularity: {granularity}")
        return f"CAST(date_trunc('{granularity}', {expr}) AS DATE)"

    def sum_cond(self, col: str, condition: str | None) -> str:
        base = f"sum({col}) FILTER (WHERE {condition})" if condition else f"sum({col})"
        return f"coalesce({base}, 0)"

    def count_star_cond(self, condition: str | None) -> str:
        return f"count(*) FILTER (WHERE {condition})" if condition else "count(*)"

    def count_distinct(self, col: str, condition: str | None) -> str:
        base = f"count(DISTINCT {col})"
        return f"{base} FILTER (WHERE {condition})" if condition else base

    def avg_cond(self, col: str, condition: str | None) -> str:
        return f"avg({col}) FILTER (WHERE {condition})" if condition else f"avg({col})"

    def max_cond(self, col: str, condition: str | None) -> str:
        return f"max({col}) FILTER (WHERE {condition})" if condition else f"max({col})"

    def divide_null_safe(self, numerator: str, denominator: str) -> str:
        return f"{numerator} / nullif({denominator}, 0)"

    def string_concat(self, *parts: str) -> str:
        return " || ".join(parts)

    def dedup(
        self,
        table: str,
        columns: list[str],
        key_columns: list[str],
        version_expr: str,
        inner_where: str | None,
    ) -> str:
        cols = ", ".join(columns)
        keys = ", ".join(key_columns)
        where = f" WHERE {inner_where}" if inner_where else ""
        return (
            f"(SELECT {cols} FROM ("
            f"SELECT {cols}, row_number() OVER (PARTITION BY {keys} ORDER BY {version_expr} DESC) AS _rn "
            f"FROM {table}{where}) WHERE _rn = 1)"
        )

    def physical_type(self, logical: str) -> str:
        return {
            "string": "VARCHAR",
            "int64": "BIGINT",
            "bool": "BOOLEAN",
            "datetime": "TIMESTAMP",
        }[logical]

    def create_fact_table(self, layout: FactTableLayout, database: str | None = None) -> list[str]:
        defs = [f"{n} {self.physical_type(t)}" for n, t in layout.columns]
        defs.append(
            f"{layout.biz_day_column} DATE GENERATED ALWAYS AS ("
            f"CAST((({layout.time_column} AT TIME ZONE 'UTC') AT TIME ZONE {_lit(layout.biz_tz)}) AS DATE))"
        )
        # DuckDB 没有便宜的 skip index/投影，但可以把排序键落成一个"覆盖排序"的索引。
        return [
            f"CREATE TABLE IF NOT EXISTS {self.qualified(layout.name, database)} ({', '.join(defs)})",
            f"CREATE INDEX IF NOT EXISTS idx_{layout.name}_day ON "
            f"{self.qualified(layout.name, database)} ({layout.biz_day_column})",
        ]


class ClickHouseDialect(Dialect):
    name = "clickhouse"

    def instant_literal(self, dt: datetime) -> str:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return f"toDateTime('{dt.strftime('%Y-%m-%d %H:%M:%S')}', 'UTC')"

    def date_literal(self, d: date) -> str:
        return f"toDate('{d.isoformat()}')"

    def local_time_expr(self, col: str, tz: str) -> str:
        return f"toTimeZone({col}, {_lit(tz)})"

    def local_day_expr(self, col: str, tz: str) -> str:
        return f"toDate(toTimeZone({col}, {_lit(tz)}))"

    def bucket_expr(self, col: str, tz: str | None, granularity: str) -> str:
        expr = f"toTimeZone({col}, {_lit(tz)})" if tz else col
        if granularity == "day":
            return f"toDate({expr})"
        if granularity == "week":
            return f"toStartOfWeek(toDate({expr}), 1)"
        if granularity == "month":
            return f"toStartOfMonth(toDate({expr}))"
        if granularity == "quarter":
            return f"toStartOfQuarter(toDate({expr}))"
        if granularity == "year":
            return f"toStartOfYear(toDate({expr}))"
        raise ValueError(f"unsupported granularity: {granularity}")

    def sum_cond(self, col: str, condition: str | None) -> str:
        base = f"sumIf({col}, {condition})" if condition else f"sum({col})"
        return f"coalesce({base}, 0)"

    def count_star_cond(self, condition: str | None) -> str:
        return f"countIf({condition})" if condition else "count()"

    def count_distinct(self, col: str, condition: str | None) -> str:
        return f"uniqExactIf({col}, {condition})" if condition else f"uniqExact({col})"

    def avg_cond(self, col: str, condition: str | None) -> str:
        return f"avgIf({col}, {condition})" if condition else f"avg({col})"

    def max_cond(self, col: str, condition: str | None) -> str:
        return f"maxIf({col}, {condition})" if condition else f"max({col})"

    def divide_null_safe(self, numerator: str, denominator: str) -> str:
        return f"{numerator} / nullIf({denominator}, 0)"

    def string_concat(self, *parts: str) -> str:
        return f"concat({', '.join(parts)})"

    def dedup(
        self,
        table: str,
        columns: list[str],
        key_columns: list[str],
        version_expr: str,
        inner_where: str | None,
    ) -> str:
        cols = ", ".join(columns)
        keys = ", ".join(key_columns)
        where = f" WHERE {inner_where}" if inner_where else ""
        # LIMIT 1 BY 是 ClickHouse 原生的按键去重，避免窗口函数全量排序。
        return (
            f"(SELECT {cols} FROM {table}{where} "
            f"ORDER BY {keys}, {version_expr} DESC LIMIT 1 BY {keys})"
        )

    def physical_type(self, logical: str) -> str:
        return {
            "string": "LowCardinality(String)",
            "int64": "Int64",
            "bool": "Bool",
            "datetime": "DateTime('UTC')",
        }[logical]

    def create_fact_table(self, layout: FactTableLayout, database: str | None = None) -> list[str]:
        table = self.qualified(layout.name, database)
        defs = [f"`{n}` {self.physical_type(t)}" for n, t in layout.columns]
        defs.append(
            f"`{layout.biz_day_column}` Date MATERIALIZED "
            f"toDate(toTimeZone(`{layout.time_column}`, {_lit(layout.biz_tz)}))"
        )
        for col in layout.skip_index_columns:
            defs.append(f"INDEX idx_{col} `{col}` TYPE bloom_filter(0.01) GRANULARITY 4")
        # 投影：为高频维度组合预建排序。不做聚合，只改物理顺序。
        for dims in layout.projection_dimensions:
            proj_name = "proj_" + "_".join(dims) + "_day"
            cols = ", ".join(f"`{c}`" for c in dims)
            defs.append(
                f"PROJECTION {proj_name} (SELECT * ORDER BY ({cols}, `{layout.biz_day_column}`))"
            )
        order_by = layout.order_by or [layout.biz_day_column, *layout.key_columns]
        partition = (
            f"\nPARTITION BY toYYYYMM(`{layout.biz_day_column}`)" if layout.partition_month else ""
        )
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {table} (\n    "
            + ",\n    ".join(defs)
            + f"\n) ENGINE = MergeTree{partition}\nORDER BY ("
            + ", ".join(f"`{c}`" for c in order_by)
            + ")\nSETTINGS index_granularity = 8192"
        )
        return [ddl]


DIALECTS: dict[str, Dialect] = {"duckdb": DuckDBDialect(), "clickhouse": ClickHouseDialect()}


def dialect_for(engine: str) -> Dialect:
    try:
        return DIALECTS[engine]
    except KeyError as exc:
        raise ValueError(f"unknown dialect: {engine}") from exc
