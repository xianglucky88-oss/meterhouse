"""指标语义层：定义模型与 SQL 编译器。

这里只负责把 YAML 里的口径契约编译成 SQL，不做取数或业务判断。
口径的“正确答案”由 ``meterhouse.regression.oracle`` 用独立的 Python 实现
计算，回归门禁只比较候选执行结果与参考结果，不信任编译器自身。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml

SUPPORTED_GRANULARITIES = ("day", "week", "month")
GRANULARITY_STEP = {"day": "INTERVAL 1 DAY", "week": "INTERVAL 7 DAY", "month": "INTERVAL 1 MONTH"}
GRANULARITY_TRUNC = {"day": None, "week": "week", "month": "month"}

ALLOWED_METRIC_KEYS = {
    "name", "label", "description", "kind", "source", "measure", "numerator", "denominator",
    "filters", "dimensions", "time_column", "additivity", "empty_value", "definition_version",
}
ALLOWED_MEASURE_KEYS = {"agg", "column", "filters", "dedup"}
ALLOWED_DEDUP_KEYS = {"keys", "policy", "column", "tiebreak"}
ALLOWED_FILTER_KEYS = {"column", "op", "value"}


def _reject_unknown(raw: dict[str, Any], allowed: set[str], where: str) -> None:
    """未知字段一律报错。

    否则把 dedup 写错层级（指标级而非 measure 内）会被静默忽略，
    定义哈希也不变，候选与参考一起算错却无人察觉。
    """
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"{where}: unknown field(s) {unknown}; allowed: {sorted(allowed)}")


@dataclass(frozen=True)
class Filter:
    column: str
    op: str
    value: Any = None

    @classmethod
    def from_raw(cls, raw: Any) -> "Filter":
        if isinstance(raw, Filter):
            return raw
        if isinstance(raw, str):
            raise ValueError(
                f"unstructured filter {raw!r} is no longer accepted; "
                "use {column, op, value}"
            )
        if not isinstance(raw, dict):
            raise ValueError(f"filter must be a mapping, got {raw!r}")
        _reject_unknown(raw, ALLOWED_FILTER_KEYS, "filter")
        return cls(column=raw["column"], op=raw.get("op", "eq"), value=raw.get("value"))

    def as_dict(self) -> dict[str, Any]:
        return {"column": self.column, "op": self.op, "value": self.value}


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (list, tuple)):
        if not value:
            raise ValueError("empty IN list is not supported")
        return "(" + ", ".join(sql_literal(v) for v in value) + ")"
    raise TypeError(f"unsupported literal: {value!r}")


def filter_to_sql(flt: Filter) -> str:
    col = flt.column
    if flt.op == "is_null":
        return f"{col} IS NULL"
    if flt.op == "not_null":
        return f"{col} IS NOT NULL"
    if flt.op == "in":
        return f"{col} IN {sql_literal(flt.value)}"
    if flt.op == "not_in":
        return f"{col} NOT IN {sql_literal(flt.value)}"
    op_map = {"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
    if flt.op not in op_map:
        raise TypeError(f"unsupported filter op: {flt.op}")
    return f"{col} {op_map[flt.op]} {sql_literal(flt.value)}"


@dataclass
class Dedup:
    """事实去重规则：重复推送的行按 keys 保留一条。"""

    keys: list[str]
    policy: str = "max_by"
    column: str | None = None
    tiebreak: list[str] = field(default_factory=list)

    @classmethod
    def from_raw(cls, raw: dict[str, Any] | None) -> "Dedup | None":
        if not raw:
            return None
        _reject_unknown(raw, ALLOWED_DEDUP_KEYS, "dedup")
        return cls(
            keys=list(raw["keys"]),
            policy=raw.get("policy", "max_by"),
            column=raw.get("column"),
            tiebreak=list(raw.get("tiebreak", [])),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "keys": self.keys,
            "policy": self.policy,
            "column": self.column,
            "tiebreak": self.tiebreak,
        }

    def rank_order(self) -> str:
        if self.policy != "max_by":
            raise ValueError(f"unsupported dedup policy: {self.policy}")
        if not self.column:
            raise ValueError("dedup policy max_by requires column")
        terms = [f"{self.column} DESC NULLS LAST"]
        terms += [f"{c} DESC NULLS LAST" for c in self.tiebreak]
        return ", ".join(terms)

    def validate(self) -> None:
        if not self.keys:
            raise ValueError("dedup keys must not be empty")
        self.rank_order()


@dataclass
class Measure:
    """一个聚合分支：agg(col)，可带自己的筛选与去重。"""

    agg: str
    column: str | None = None
    filters: list[Filter] = field(default_factory=list)
    dedup: Dedup | None = None

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "Measure":
        _reject_unknown(raw, ALLOWED_MEASURE_KEYS, "measure")
        return cls(
            agg=raw["agg"],
            column=raw.get("column"),
            filters=[Filter.from_raw(f) for f in raw.get("filters", [])],
            dedup=Dedup.from_raw(raw.get("dedup")),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "agg": self.agg,
            "column": self.column,
            "filters": [f.as_dict() for f in self.filters],
            "dedup": self.dedup.as_dict() if self.dedup else None,
        }

    def sql_expr(self) -> str:
        if self.agg == "count_distinct":
            if not self.column:
                raise ValueError("count_distinct requires column")
            return f"count(DISTINCT {self.column})"
        if self.agg == "count":
            return "count(*)"
        if self.agg in ("sum", "min", "max", "avg"):
            if not self.column:
                raise ValueError(f"{self.agg} requires column")
            return f"{self.agg}({self.column})"
        raise ValueError(f"unsupported aggregate: {self.agg}")

    def validate(self) -> None:
        self.sql_expr()
        if self.dedup:
            self.dedup.validate()
        for flt in self.filters:
            filter_to_sql(flt)


@dataclass
class Metric:
    """一个指标的完整口径契约。

    语义字段参与 ``semantic_hash``；label/description 只用于展示。
    """

    name: str
    label: str
    description: str
    kind: str
    source: str
    measure: Measure | None = None
    numerator: Measure | None = None
    denominator: Measure | None = None
    filters: list[Filter] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    time_column: str | None = None
    additivity: str = "additive"
    empty_value: Any = 0
    definition_version: int = 1

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Metric":
        _reject_unknown(raw, ALLOWED_METRIC_KEYS, f"metric {raw.get('name')}")
        if "expr" in raw and "measure" not in raw:
            raise ValueError(
                f"metric {raw.get('name')}: raw expr is no longer accepted; "
                "declare a structured measure instead"
            )
        return cls(
            name=raw["name"],
            label=raw.get("label", raw["name"]),
            description=raw.get("description", ""),
            kind=raw.get("kind", "simple"),
            source=raw["source"],
            measure=Measure.from_raw(raw["measure"]) if raw.get("measure") else None,
            numerator=Measure.from_raw(raw["numerator"]) if raw.get("numerator") else None,
            denominator=Measure.from_raw(raw["denominator"]) if raw.get("denominator") else None,
            filters=[Filter.from_raw(f) for f in raw.get("filters", [])],
            dimensions=list(raw.get("dimensions", [])),
            time_column=raw.get("time_column"),
            additivity=raw.get("additivity", "additive"),
            empty_value=raw.get("empty_value", 0),
            definition_version=int(raw.get("definition_version", 1)),
        )

    def semantic_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "source": self.source,
            "measure": self.measure.as_dict() if self.measure else None,
            "numerator": self.numerator.as_dict() if self.numerator else None,
            "denominator": self.denominator.as_dict() if self.denominator else None,
            "filters": [f.as_dict() for f in self.filters],
            "dimensions": self.dimensions,
            "time_column": self.time_column,
            "additivity": self.additivity,
            "empty_value": self.empty_value,
            "definition_version": self.definition_version,
        }

    def semantic_hash(self) -> str:
        payload = json.dumps(self.semantic_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def validate(self) -> None:
        if self.kind == "ratio":
            if self.numerator is None or self.denominator is None:
                raise ValueError(f"ratio metric {self.name} requires numerator and denominator")
            self.numerator.validate()
            self.denominator.validate()
        elif self.measure is None:
            raise ValueError(f"metric {self.name} requires measure")
        else:
            self.measure.validate()
        for flt in self.filters:
            filter_to_sql(flt)
        if self.additivity not in ("additive", "semi_additive", "non_additive"):
            raise ValueError(f"metric {self.name}: bad additivity {self.additivity!r}")
        if self.kind not in ("simple", "distinct", "ratio"):
            raise ValueError(f"metric {self.name}: bad kind {self.kind!r}")
        if self.time_column and "date" in self.dimensions:
            raise ValueError(f"metric {self.name}: reserved dimension name 'date'")

    @property
    def expr(self) -> str | None:
        return self.measure.sql_expr() if self.measure else None


@dataclass
class MetricStore:
    """指标定义集合，负责编译。"""

    metrics: dict[str, Metric] = field(default_factory=dict)
    defaults: dict[str, Any] = field(default_factory=dict)
    version: int = 1

    @classmethod
    def load(cls, path: Path | str) -> "MetricStore":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        store = cls(defaults=dict(raw.get("defaults", {})), version=int(raw.get("version", 1)))
        for m in raw.get("metrics", []):
            metric = Metric.from_dict(m)
            metric.validate()
            if metric.name in store.metrics:
                raise ValueError(f"duplicate metric definition: {metric.name}")
            store.metrics[metric.name] = metric
        return store

    def get(self, name: str) -> Metric:
        if name not in self.metrics:
            raise KeyError(f"unknown metric: {name}")
        return self.metrics[name]

    def names(self) -> list[str]:
        return sorted(self.metrics)

    @property
    def tz_offset_hours(self) -> int:
        return int(self.defaults.get("timezone_offset_hours", 0))

    def fingerprints(self) -> dict[str, dict[str, Any]]:
        return {
            name: {
                "definition_version": m.definition_version,
                "semantic_hash": m.semantic_hash(),
            }
            for name, m in sorted(self.metrics.items())
        }

    # -------------------------------------------------------------- 编译入口

    def compile(
        self,
        name: str,
        dimensions: Iterable[str] | None = None,
        granularity: str | None = None,
        filters: Iterable[Filter | dict[str, Any]] | None = None,
        time_range: dict[str, str] | None = None,
        fill_missing: bool = False,
    ) -> str:
        """编译单指标查询。

        dimensions 必须已声明；granularity 为 day/week/month，按业务时区归桶；
        filters 与指标自身 filters 取交集；time_range 左闭右开；
        fill_missing 只支持无维度时间序列，用于暴露“空缺桶”。
        """
        metric = self.get(name)
        dims = list(dimensions or [])
        scenario_filters = [Filter.from_raw(f) for f in (filters or [])]
        self._validate_query(name, dims, granularity, time_range, fill_missing)

        shifted = self._shifted_time(metric)
        date_expr = f"CAST({shifted} AS DATE)" if metric.time_column else None
        where_parts = [filter_to_sql(f) for f in metric.filters + scenario_filters]
        if time_range and date_expr:
            where_parts.append(f"{date_expr} >= DATE '{time_range['start']}'")
            where_parts.append(f"{date_expr} < DATE '{time_range['end']}'")
        base_cte = f"base AS (SELECT * FROM {metric.source}"
        if where_parts:
            base_cte += " WHERE " + " AND ".join(where_parts)
        base_cte += ")"

        group_exprs = self._group_expressions(metric, dims, granularity, shifted)
        group_aliases = self._group_aliases(dims, granularity)

        if metric.kind == "ratio":
            sql = self._compile_ratio(metric, base_cte, group_exprs, group_aliases)
        else:
            sql = self._compile_simple(metric, base_cte, group_exprs, group_aliases)
        if fill_missing:
            sql = self._fill_series(metric, sql, granularity, time_range)  # type: ignore[arg-type]
        return sql

    def compile_grouped(self, name: str, dimensions: list[str]) -> str:
        """向后兼容入口：仅按维度分组。"""
        return self.compile(name, dimensions=dimensions)

    # ---------------------------------------------------------------- 内部

    def _validate_query(
        self,
        name: str,
        dims: list[str],
        granularity: str | None,
        time_range: dict[str, str] | None,
        fill_missing: bool,
    ) -> None:
        metric = self.get(name)
        bad = [d for d in dims if d not in metric.dimensions]
        if bad:
            raise ValueError(f"metric {name} has no dimensions {bad}")
        if len(set(dims)) != len(dims):
            raise ValueError(f"duplicate dimensions: {dims}")
        if granularity is not None:
            if granularity not in SUPPORTED_GRANULARITIES:
                raise ValueError(f"unsupported granularity: {granularity}")
            if not metric.time_column:
                raise ValueError(f"metric {name} has no time_column for granularity")
        if time_range is not None:
            if set(time_range) != {"start", "end"}:
                raise ValueError("time_range requires exactly start and end (end exclusive)")
            if time_range["start"] >= time_range["end"]:
                raise ValueError("time_range start must be before end")
            if not metric.time_column:
                raise ValueError(f"metric {name} has no time_column for time_range")
        if fill_missing:
            if granularity is None or time_range is None:
                raise ValueError("fill_missing requires granularity and time_range")
            if dims:
                raise ValueError("fill_missing supports only dimension-free series")

    def _shifted_time(self, metric: Metric) -> str | None:
        if not metric.time_column:
            return None
        offset = self.tz_offset_hours
        return f"({metric.time_column} + INTERVAL {offset} HOUR)" if offset else metric.time_column

    @staticmethod
    def _group_expressions(metric: Metric, dims: list[str], granularity: str | None, shifted: str | None) -> list[str]:
        exprs = list(dims)
        if granularity:
            assert shifted
            trunc = GRANULARITY_TRUNC[granularity]
            if trunc is None:
                exprs.append(f"CAST({shifted} AS DATE) AS date")
            else:
                exprs.append(f"CAST(date_trunc('{trunc}', {shifted}) AS DATE) AS date")
        return exprs

    @staticmethod
    def _group_aliases(dims: list[str], granularity: str | None) -> list[str]:
        aliases = list(dims)
        if granularity:
            aliases.append("date")
        return aliases

    @staticmethod
    def _measure_source_cte(prefix: str, base_cte: str, measure: Measure) -> tuple[list[str], str]:
        ctes = [base_cte]
        filters = [filter_to_sql(f) for f in measure.filters]
        source = "base"
        if filters:
            source = f"(SELECT * FROM base WHERE {' AND '.join(filters)})"
        if measure.dedup:
            dedup = measure.dedup
            partition = ", ".join(dedup.keys)
            ctes.append(
                f"{prefix}_dedup AS (SELECT * FROM ("
                f"SELECT *, row_number() OVER (PARTITION BY {partition} ORDER BY {dedup.rank_order()}) AS _dedup_rn "
                f"FROM {source}) _d WHERE _dedup_rn = 1)"
            )
            source = f"{prefix}_dedup"
        return ctes, source

    @staticmethod
    def _value_expr(metric: Metric, measure: Measure, alias: str, coalesce: bool) -> str:
        expr = measure.sql_expr()
        if coalesce and metric.empty_value is not None:
            expr = f"coalesce({expr}, {sql_literal(metric.empty_value)})"
        return f"{expr} AS {alias}"

    def _compile_simple(self, metric: Metric, base_cte: str, group_exprs: list[str], group_aliases: list[str]) -> str:
        assert metric.measure is not None
        ctes, source = self._measure_source_cte("m", base_cte, metric.measure)
        select_cols = group_exprs + [self._value_expr(metric, metric.measure, metric.name, coalesce=True)]
        sql = f"WITH {', '.join(ctes)} SELECT {', '.join(select_cols)} FROM {source}"
        if group_aliases:
            joined = ", ".join(group_aliases)
            sql += f" GROUP BY {joined} ORDER BY {joined}"
        return sql

    def _compile_ratio(self, metric: Metric, base_cte: str, group_exprs: list[str], group_aliases: list[str]) -> str:
        assert metric.numerator is not None and metric.denominator is not None
        num_ctes, num_src = self._measure_source_cte("num", base_cte, metric.numerator)
        den_ctes, den_src = self._measure_source_cte("den", base_cte, metric.denominator)
        ctes = den_ctes + [c for c in num_ctes if c not in den_ctes]
        num_value = self._value_expr(metric, metric.numerator, "_numerator", coalesce=True)
        den_value = self._value_expr(metric, metric.denominator, "_denominator", coalesce=False)
        num_sql = f"num AS (SELECT {', '.join(group_exprs + [num_value])} FROM {num_src}"
        den_sql = f"den AS (SELECT {', '.join(group_exprs + [den_value])} FROM {den_src}"
        if group_aliases:
            joined = ", ".join(group_aliases)
            num_sql += f" GROUP BY {joined}"
            den_sql += f" GROUP BY {joined}"
        ctes += [num_sql + ")", den_sql + ")"]

        ratio_expr = (
            "CASE WHEN den._denominator IS NULL OR den._denominator = 0 THEN "
            f"{sql_literal(metric.empty_value)} "
            "ELSE 1.0 * coalesce(num._numerator, 0) / den._denominator END AS " + metric.name
        )
        if group_aliases:
            joined = ", ".join(group_aliases)
            select_cols = [f"den.{a} AS {a}" for a in group_aliases] + [ratio_expr]
            sql = (
                f"WITH {', '.join(ctes)} SELECT {', '.join(select_cols)} "
                f"FROM den LEFT JOIN num USING ({joined}) ORDER BY {joined}"
            )
        else:
            sql = f"WITH {', '.join(ctes)} SELECT {ratio_expr} FROM den LEFT JOIN num ON TRUE"
        return sql

    def _fill_series(self, metric: Metric, sql: str, granularity: str, time_range: dict[str, str]) -> str:
        """在 [start, end) 覆盖的桶上补齐空缺。

        桶边界与 date_trunc 对齐（周一对齐、月初对齐），否则 start 落在桶中间时
        序列会与 GROUP BY 的桶错位。
        """
        step = GRANULARITY_STEP[granularity]
        start = time_range["start"]
        end = time_range["end"]
        trunc = GRANULARITY_TRUNC[granularity]
        if trunc is None:
            lo = f"DATE '{start}'"
        else:
            lo = f"CAST(date_trunc('{trunc}', DATE '{start}') AS DATE)"
        if trunc is None:
            hi = f"CAST(DATE '{end}' AS TIMESTAMP) - INTERVAL 1 DAY"
        else:
            hi = f"CAST(date_trunc('{trunc}', CAST(DATE '{end}' AS TIMESTAMP) - INTERVAL 1 DAY) AS DATE)"
        fill = sql_literal(metric.empty_value)
        series = f"series AS (SELECT CAST(unnest(generate_series({lo}, {hi}, {step})) AS DATE) AS date)"
        return (
            f"WITH agg AS ({sql}), {series} "
            f"SELECT series.date AS date, coalesce(agg.{metric.name}, {fill}) AS {metric.name} "
            "FROM series LEFT JOIN agg USING (date) ORDER BY date"
        )
