"""指标语义层与 SQL 编译器。

指标用**结构化原语**声明（聚合类型 + 列 + 过滤器 + 去重键），而不是手写
SQL 字符串。这样同一份定义可以编译到 DuckDB 与 ClickHouse 两种方言，
切流前后口径由构造保证一致。

关键约定
--------
* 事实去重发生在**查询期**：`dedup_by` 指定业务主键，`version_column`
  指定取哪一行。原始事实行完整保留，可追溯到具体摄入批次。
* 去重类指标（`non_additive`）永远在目标粒度**重新聚合**，绝不把更细
  粒度的结果相加。语义层不维护任何预聚合表。
* 时间窗口与粒度显式声明基准（`business` / `utc`），默认业务时区。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

from .errors import DefinitionError, UnknownMetric
from .sql.dialect import SUPPORTED_GRANULARITIES, Dialect, dialect_for

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQL_KEYWORDS = {
    "and", "or", "not", "null", "true", "false", "is", "in", "like", "ilike",
    "between", "case", "when", "then", "else", "end", "distinct", "filter",
    "where", "interval", "day", "month", "year", "coalesce", "sum", "count",
    "avg", "max", "min",
}


def _ident(name: str) -> str:
    if not IDENTIFIER.match(name):
        raise DefinitionError(f"invalid identifier: {name!r}")
    return name


def predicate_columns(predicate: str | None) -> set[str]:
    """从布尔谓词里提取列名（用于去重子查询的列裁剪与校验）。"""
    if not predicate:
        return set()
    bare = re.sub(r"'[^']*'", " ", predicate)
    bare = re.sub(r"\b\d+(?:\.\d+)?\b", " ", bare)
    return {
        tok
        for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", bare)
        if tok.lower() not in _SQL_KEYWORDS
    }


@dataclass(frozen=True)
class Agg:
    """一个可编译的聚合原语。"""

    agg: str  # sum | count | count_distinct | avg | max
    column: str | None = None
    filter: str | None = None

    def columns(self) -> set[str]:
        cols = predicate_columns(self.filter)
        if self.column:
            cols.add(self.column)
        return cols

    def with_extra_filter(self, extra: str | None) -> "Agg":
        if not extra:
            return self
        filt = f"({self.filter}) AND ({extra})" if self.filter else extra
        return Agg(agg=self.agg, column=self.column, filter=filt)

    def compile(self, dialect: Dialect) -> str:
        if self.agg == "sum":
            return dialect.sum_cond(_ident(self.column), self.filter)
        if self.agg == "count":
            return dialect.count_star_cond(self.filter)
        if self.agg == "count_distinct":
            return dialect.count_distinct(_ident(self.column), self.filter)
        if self.agg == "avg":
            return dialect.avg_cond(_ident(self.column), self.filter)
        if self.agg == "max":
            return dialect.max_cond(_ident(self.column), self.filter)
        raise DefinitionError(f"unsupported aggregation: {self.agg}")

    @classmethod
    def from_raw(cls, raw: Any) -> "Agg":
        if not isinstance(raw, dict):
            raise DefinitionError(f"aggregation must be a mapping, got {raw!r}")
        if raw["agg"] not in ("sum", "count", "count_distinct", "avg", "max"):
            raise DefinitionError(f"unsupported aggregation: {raw['agg']}")
        return cls(agg=raw["agg"], column=raw.get("column"), filter=raw.get("filter"))


@dataclass
class Metric:
    """一个指标定义。"""

    name: str
    label: str
    description: str
    # simple（单聚合）、distinct（去重计数）、ratio（分子/分母）
    kind: str
    source: str
    filters: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    time_column: str | None = None
    additivity: str = "additive"
    agg: Agg | None = None
    numerator: Agg | None = None
    denominator: Agg | None = None
    time_basis: str = "business"
    empty_value: float | int | None = 0
    value_type: str = "float"
    definition_version: str = "1"
    legacy_expr: str | None = None
    bucket_granularity: str = "day"

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Metric":
        kind = raw.get("kind", "simple")
        filters = list(raw.get("filters", []))
        if raw.get("filter"):
            filters.insert(0, raw["filter"])

        agg = Agg.from_raw(raw["agg"]) if raw.get("agg") else None
        numerator = Agg.from_raw(raw["numerator"]) if raw.get("numerator") else None
        denominator = Agg.from_raw(raw["denominator"]) if raw.get("denominator") else None

        if kind in ("simple", "distinct"):
            if agg is None and raw.get("distinct_by"):
                agg = Agg(agg="count_distinct", column=raw["distinct_by"])
            if agg is None:
                raise DefinitionError(
                    f"metric {raw['name']}: kind={kind} needs `agg` or `distinct_by`"
                )
        if kind == "ratio" and (numerator is None or denominator is None):
            raise DefinitionError(f"metric {raw['name']}: ratio needs numerator and denominator")
        if kind == "avg_of_distinct":
            if agg is None or agg.agg != "count_distinct":
                raise DefinitionError(
                    f"metric {raw['name']}: avg_of_distinct needs a count_distinct agg"
                )
        if kind not in ("simple", "distinct", "ratio", "avg_of_distinct"):
            raise DefinitionError(f"metric {raw['name']}: unsupported kind {kind}")

        default_empty = 0 if kind in ("simple", "distinct") else None
        return cls(
            name=raw["name"],
            label=raw.get("label", raw["name"]),
            description=raw.get("description", ""),
            kind=kind,
            source=raw["source"],
            filters=filters,
            dimensions=list(raw.get("dimensions", [])),
            time_column=raw.get("time_column"),
            additivity=raw.get("additivity", "additive"),
            agg=agg,
            numerator=numerator,
            denominator=denominator,
            time_basis=raw.get("time_basis", "business"),
            empty_value=raw.get("empty_value", default_empty),
            value_type=raw.get(
                "value_type", "int" if kind in ("simple", "distinct") else "float"
            ),
            definition_version=str(raw.get("definition_version", "1")),
            legacy_expr=raw.get("expr"),
            bucket_granularity=raw.get("bucket_granularity", "day"),
        )

    def referenced_columns(self) -> set[str]:
        cols: set[str] = set()
        for f in self.filters:
            cols |= predicate_columns(f)
        for agg in (self.agg, self.numerator, self.denominator):
            if agg is not None:
                cols |= agg.columns()
        if self.time_column:
            cols.add(self.time_column)
        return cols


@dataclass
class SourceSpec:
    """指标的取数来源（与 `FactTableLayout` 一一对应）。

    `table` 不含库名；库名前缀由编译器按目标引擎的默认库补齐，
    这样同一份定义既能在 DuckDB（无库概念）也能在 ClickHouse 多库下执行。
    """

    table: str
    time_column: str
    dedup_by: list[str] = field(default_factory=list)
    version_column: str = "row_version"
    biz_day_column: str = "biz_day"
    biz_tz: str = "Asia/Shanghai"
    database: str | None = None

    @property
    def qualified_table(self) -> str:
        return f"{self.database}.{self.table}" if self.database else self.table

    @classmethod
    def from_dict(cls, raw: dict[str, Any], default_tz: str) -> "SourceSpec":
        return cls(
            table=raw["table"],
            time_column=raw["time_column"],
            dedup_by=list(raw.get("dedup_by", [])),
            version_column=raw.get("version_column", "row_version"),
            biz_day_column=raw.get("biz_day_column", "biz_day"),
            biz_tz=raw.get("biz_tz", default_tz),
            database=raw.get("database"),
        )


@dataclass
class TimeWindow:
    """查询时间窗口（业务日期闭区间）与粒度。"""

    start: date | None = None
    end: date | None = None
    granularity: str | None = None
    basis: str = "business"

    def validate(self) -> None:
        if self.granularity and self.granularity not in SUPPORTED_GRANULARITIES:
            raise DefinitionError(f"unsupported granularity: {self.granularity}")
        if self.start and self.end and self.end < self.start:
            raise DefinitionError("end date must not be before start date")
        if self.basis not in ("business", "utc"):
            raise DefinitionError(f"unsupported time basis: {self.basis}")


@dataclass
class CompiledQuery:
    engine: str
    metric: str
    sql: str
    value_column: str
    dimension_columns: list[str]
    time_column_alias: str | None
    value_type: str
    empty_value: float | int | None
    description: str
    definition_version: str
    lineage: dict[str, Any] = field(default_factory=dict)

    def result_columns(self) -> list[str]:
        cols = [*self.dimension_columns]
        if self.time_column_alias:
            cols.append(self.time_column_alias)
        cols.append(self.value_column)
        return cols


class MetricStore:
    """指标定义集合 + 编译器。"""

    def __init__(
        self,
        metrics: dict[str, Metric] | None = None,
        sources: dict[str, SourceSpec] | None = None,
        default_tz: str = "Asia/Shanghai",
        default_database: str | None = None,
    ) -> None:
        self.metrics = metrics or {}
        self.sources = sources or {}
        self.default_tz = default_tz
        self.default_database = default_database

    # ------------------------------------------------------------------ 加载
    @classmethod
    def load(cls, path: Path, *, default_database: str | None = None) -> "MetricStore":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        defaults = raw.get("defaults", {})
        default_tz = defaults.get("biz_tz", "Asia/Shanghai")
        sources = {
            name: SourceSpec.from_dict(spec, spec.get("biz_tz", default_tz))
            for name, spec in (raw.get("sources") or {}).items()
        }
        store = cls(
            sources=sources, default_tz=default_tz, default_database=default_database
        )
        for m in raw.get("metrics", []):
            metric = Metric.from_dict(m)
            if metric.name in store.metrics:
                raise DefinitionError(f"duplicate metric: {metric.name}")
            store.metrics[metric.name] = metric
        return store

    def get(self, name: str) -> Metric:
        if name not in self.metrics:
            raise UnknownMetric(f"unknown metric: {name}")
        return self.metrics[name]

    def names(self) -> list[str]:
        return sorted(self.metrics)

    def source_for(self, metric: Metric) -> SourceSpec:
        if metric.source in self.sources:
            return self.sources[metric.source]
        if not metric.time_column:
            raise DefinitionError(f"metric {metric.name}: missing time_column")
        return SourceSpec(
            table=metric.source,
            time_column=metric.time_column,
            biz_tz=self.default_tz,
            database=self.default_database,
        )

    # -------------------------------------------------------------- 旧接口兼容
    def compile(self, name: str, **kwargs: Any) -> str:
        """无维度、无时间过滤的单指标 SQL（保留给旧调用方）。"""
        return self.compile_query(name, engine="duckdb").sql

    def compile_grouped(self, name: str, dimensions: list[str]) -> str:
        return self.compile_query(name, dimensions=dimensions, engine="duckdb").sql

    # ------------------------------------------------------------------ 编译
    def compile_query(
        self,
        name: str,
        *,
        engine: str,
        dimensions: list[str] | None = None,
        time_window: TimeWindow | None = None,
    ) -> CompiledQuery:
        metric = self.get(name)
        dialect = dialect_for(engine)
        dims = list(dimensions or [])
        window = time_window or TimeWindow()
        window.validate()

        for d in dims:
            _ident(d)
            if d not in metric.dimensions:
                raise DefinitionError(f"metric {name} has no dimension {d!r}")
        if len(set(dims)) != len(dims):
            raise DefinitionError("duplicate dimensions")
        if window.granularity and not metric.time_column:
            raise DefinitionError(f"metric {name} has no time column")

        source = self.source_for(metric)
        if source.database is None and self.default_database and engine == "clickhouse":
            source = replace(source, database=self.default_database)
        # 指标级过滤与具体聚合分支的过滤必须一致地作用到去重前的行。
        metric_cond = " AND ".join(f"({f})" for f in metric.filters) or None

        # ---- FROM（可选去重）----
        from_sql = self._build_from(dialect, metric, source, dims, window)

        if metric.kind == "avg_of_distinct":
            return self._compile_avg_of_distinct(
                metric, dialect, source, from_sql, dims, window, metric_cond
            )

        # ---- 选择列 / 分组键 ----
        group_by = [*dims]
        select_parts = [*dims]
        time_alias: str | None = None
        if window.granularity:
            tz = source.biz_tz if window.basis == "business" else None
            bucket = dialect.bucket_expr(source.time_column, tz, window.granularity)
            time_alias = "time_bucket"
            select_parts.append(f"{bucket} AS {time_alias}")
            group_by.append(bucket)

        value_expr = self._value_expr(dialect, metric, metric_cond)
        select_parts.append(f"{value_expr} AS {metric.name}")

        sql = f"SELECT {', '.join(select_parts)} FROM {from_sql}"
        if group_by:
            sql += f" GROUP BY {', '.join(group_by)}"
            order_cols = [*dims] + ([time_alias] if time_alias else [])
            if order_cols:
                sql += " ORDER BY " + ", ".join(order_cols)

        return CompiledQuery(
            engine=engine,
            metric=name,
            sql=sql,
            value_column=metric.name,
            dimension_columns=dims,
            time_column_alias=time_alias,
            value_type=metric.value_type,
            empty_value=metric.empty_value,
            description=metric.description.strip(),
            definition_version=metric.definition_version,
            lineage={
                "source_table": source.table,
                "dedup_by": source.dedup_by,
                "version_column": source.version_column if source.dedup_by else None,
                "time_column": source.time_column,
                "time_basis": window.basis,
                "window": {
                    "start": window.start.isoformat() if window.start else None,
                    "end": window.end.isoformat() if window.end else None,
                    "granularity": window.granularity,
                },
                "additivity": metric.additivity,
                "filters": metric.filters,
                "definition_version": metric.definition_version,
            },
        )

    # ------------------------------------------------------------------ 内部
    def _build_from(
        self,
        dialect: Dialect,
        metric: Metric,
        source: SourceSpec,
        dims: list[str],
        window: TimeWindow,
    ) -> str:
        """构造 FROM 子句；需要去重时把时间谓词下推到去重之前。

        先按时间窗口过滤再按键取最新版本，是明细事实的正确窗口语义：
        一行事实属于它自己的发生时间，窗口外的重复推送不应参与窗口内去重。
        """
        needed = metric.referenced_columns() | set(dims) | {source.time_column}
        predicate = self._time_predicate(dialect, source, window)

        if source.dedup_by:
            cols = sorted(needed | set(source.dedup_by) | {source.version_column})
            return dialect.dedup(
                source.qualified_table,
                cols,
                source.dedup_by,
                source.version_column,
                inner_where=predicate,
            )
        if predicate:
            return f"(SELECT * FROM {source.qualified_table} WHERE {predicate})"
        return source.qualified_table

    def _value_expr(self, dialect: Dialect, metric: Metric, metric_cond: str | None) -> str:
        if metric.kind == "ratio":
            assert metric.numerator and metric.denominator
            num = metric.numerator.with_extra_filter(metric_cond).compile(dialect)
            den = metric.denominator.with_extra_filter(metric_cond).compile(dialect)
            return dialect.divide_null_safe(f"1.0 * ({num})", f"({den})")
        if metric.agg is None:
            raise DefinitionError(f"metric {metric.name}: missing aggregation")
        return metric.agg.with_extra_filter(metric_cond).compile(dialect)

    def _compile_avg_of_distinct(
        self,
        metric: Metric,
        dialect: Dialect,
        source: SourceSpec,
        from_sql: str,
        dims: list[str],
        window: TimeWindow,
        metric_cond: str | None,
    ) -> CompiledQuery:
        """先按固定桶（`bucket_granularity`）去重，再对桶求平均。

        这是唯一允许"先细粒度再上卷"的形态，而且只允许**平均**，不允许相加。
        请求粒度更粗时必须重新选桶，否则"日去重数"相加会重复计入跨日活跃的
        供应商（这正是原始口径 B/C 的错误）。
        """
        if metric.agg is None:
            raise DefinitionError(f"metric {metric.name}: missing aggregation")
        inner_grain = metric.bucket_granularity
        # 外层粒度取请求粒度，但不得细于内层桶；否则内层桶会掩盖细粒度。
        outer_grain = window.granularity or inner_grain
        if _grain_rank(outer_grain) < _grain_rank(inner_grain):
            raise DefinitionError(
                f"metric {metric.name}: granularity {outer_grain} is finer than "
                f"bucket_granularity {inner_grain}"
            )
        tz = source.biz_tz if window.basis == "business" else None
        inner_bucket_expr = dialect.bucket_expr(source.time_column, tz, inner_grain)
        outer_bucket_expr = dialect.bucket_expr(source.time_column, tz, outer_grain)

        distinct_n = metric.agg.with_extra_filter(metric_cond).compile(dialect)
        inner_cols = [*dims, f"{inner_bucket_expr} AS _inner_bucket", f"{distinct_n} AS _distinct_n"]
        inner = (
            f"(SELECT {', '.join(inner_cols)} FROM {from_sql} "
            f"GROUP BY {', '.join([*dims, inner_bucket_expr])})"
        )
        # 内层每桶的去重数不能直接相加：外层必须再按请求粒度重新去重 —
        # 但内层已经把明细折叠成 (维度, 桶, 去重数)，丢失了 supplier_id。
        # 因此日均值指标的请求粒度只能是 day 或单独求平均。
        if window.granularity and window.granularity != inner_grain:
            raise DefinitionError(
                f"metric {metric.name}: granularity {window.granularity} would require "
                f"re-deduplication across {inner_grain} buckets; only day or ungrouped "
                f"averages are supported"
            )
        select = [*dims]
        group_by = [*dims]
        time_alias: str | None = None
        if window.granularity:
            time_alias = "time_bucket"
            select.append(f"_inner_bucket AS {time_alias}")
            group_by.append("_inner_bucket")
        select.append(f"{dialect.avg_cond('_distinct_n', None)} AS {metric.name}")
        sql = f"SELECT {', '.join(select)} FROM {inner}"
        if group_by:
            sql += f" GROUP BY {', '.join(group_by)}"
            sql += " ORDER BY " + ", ".join([*dims] + ([time_alias] if time_alias else []))

        return CompiledQuery(
            engine=dialect.name,
            metric=metric.name,
            sql=sql,
            value_column=metric.name,
            dimension_columns=dims,
            time_column_alias=time_alias,
            value_type=metric.value_type,
            empty_value=metric.empty_value,
            description=metric.description.strip(),
            definition_version=metric.definition_version,
            lineage={
                "source_table": source.table,
                "dedup_by": source.dedup_by,
                "version_column": source.version_column if source.dedup_by else None,
                "time_column": source.time_column,
                "time_basis": window.basis,
                "window": {
                    "start": window.start.isoformat() if window.start else None,
                    "end": window.end.isoformat() if window.end else None,
                    "granularity": window.granularity,
                },
                "additivity": metric.additivity,
                "filters": metric.filters,
                "definition_version": metric.definition_version,
                "average_over": inner_grain,
            },
        )

    def _time_predicate(
        self, dialect: Dialect, source: SourceSpec, window: TimeWindow
    ) -> str | None:
        if not window.start and not window.end:
            return None
        tz = ZoneInfo(source.biz_tz) if window.basis == "business" else timezone.utc
        parts: list[str] = []
        if window.start:
            start_instant = datetime.combine(window.start, time.min, tzinfo=tz).astimezone(
                timezone.utc
            )
            parts.append(
                f"{source.time_column} >= {dialect.instant_literal(start_instant)}"
            )
        if window.end:
            end_instant = datetime.combine(
                window.end + timedelta(days=1), time.min, tzinfo=tz
            ).astimezone(timezone.utc)
            parts.append(f"{source.time_column} < {dialect.instant_literal(end_instant)}")
        # 业务日范围谓词：让分区/排序键能真正裁剪，而不是只靠时间列 min-max。
        if window.basis == "business" and window.start and window.end:
            parts.append(
                f"{source.biz_day_column} >= {dialect.date_literal(window.start)}"
            )
            parts.append(
                f"{source.biz_day_column} <= {dialect.date_literal(window.end)}"
            )
        return " AND ".join(parts)


_GRAIN_RANK = {"day": 1, "week": 2, "month": 3, "quarter": 4, "year": 5}


def _grain_rank(granularity: str) -> int:
    try:
        return _GRAIN_RANK[granularity]
    except KeyError as exc:
        raise DefinitionError(f"unsupported granularity: {granularity}") from exc
