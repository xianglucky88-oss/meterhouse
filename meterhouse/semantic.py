"""指标语义层：YAML 定义加载与历史兼容外观。

真正的编译逻辑在 meterhouse.compiler，执行逻辑在 meterhouse.engine。
本模块负责把 metrics/metrics.yaml 解析为不可变 Catalog，并保留
Metric / MetricStore 外观供旧调用方使用；新代码请直接使用
build_catalog() + Compiler + MetricEngine。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .compiler import (
    SUPPORTED_SCHEMA_VERSIONS,
    AggSpec,
    Catalog,
    Compiler,
    Filter,
    MetricSpec,
    QueryRequest,
    RatioComponent,
    SourceSpec,
    UnsupportedSchemaError,
)

_ADDITIVITY = frozenset({"additive", "semi_additive", "non_additive"})
_KINDS = frozenset({"measure", "distinct", "ratio"})
_DEDUP_STRATEGIES = frozenset({"distinct", "none"})


# --------------------------------------------------------------------------- YAML 解析


def _parse_filter(raw: Any) -> Filter:
    """支持两种写法：

    结构化（推荐，可参数化）：{column: status, op: "=", value: paid}
    受信任原生表达式（仅 YAML 作者可用）："is_active" / "status = 'paid'"
    请求路径传入的过滤永远走结构化形式，原生表达式不会从 API 进入。
    """
    if isinstance(raw, str):
        return Filter(column=f"__raw__({raw})", op="__raw__", value=raw)
    if not isinstance(raw, dict):
        raise UnsupportedSchemaError(f"过滤条件必须是字符串或映射，收到: {raw!r}")
    try:
        return Filter(
            column=str(raw["column"]),
            op=str(raw["op"]).lower(),
            value=raw.get("value"),
        )
    except KeyError as exc:  # pragma: no cover - 防御
        raise UnsupportedSchemaError(f"过滤条件缺少字段 {exc}: {raw!r}") from exc


def _parse_agg(raw: dict[str, Any]) -> AggSpec:
    return AggSpec(func=str(raw["func"]).lower(), expr=str(raw["expr"]))


def _parse_component(raw: dict[str, Any] | None) -> RatioComponent | None:
    if raw is None:
        return None
    return RatioComponent(
        agg=_parse_agg(raw["agg"]) if raw.get("agg") else None,
        distinct=str(raw["distinct"]) if raw.get("distinct") else None,
        filters=tuple(_parse_filter(f) for f in raw.get("filters", [])),
    )


def _parse_metric(raw: dict[str, Any], biz_offset: int) -> MetricSpec:
    kind = str(raw.get("kind", "measure"))
    if kind not in _KINDS:
        raise UnsupportedSchemaError(f"指标 {raw.get('name')} 的 kind 非法: {kind}")
    additivity = raw.get("additivity", "additive")
    if additivity not in _ADDITIVITY:
        raise UnsupportedSchemaError(
            f"指标 {raw.get('name')} 的 additivity 非法: {additivity}"
        )
    filters = tuple(_parse_filter(f) for f in raw.get("filters", []))
    numerator = _parse_component(raw.get("numerator"))
    denominator = _parse_component(raw.get("denominator"))
    if kind == "ratio" and (numerator is None or denominator is None):
        raise UnsupportedSchemaError(
            f"比率指标 {raw.get('name')} 必须显式声明 numerator 与 denominator"
        )
    if kind == "distinct" and not raw.get("distinct"):
        raise UnsupportedSchemaError(f"去重指标 {raw.get('name')} 缺少 distinct 列")
    if kind == "measure" and not raw.get("agg"):
        raise UnsupportedSchemaError(f"聚合指标 {raw.get('name')} 缺少 agg 定义")
    return MetricSpec(
        name=str(raw["name"]),
        label=str(raw.get("label", raw["name"])),
        description=str(raw.get("description", "")).strip(),
        kind=kind,
        source=str(raw["source"]),
        additivity=additivity,
        time_column=str(raw["time_column"]),
        dimensions=tuple(str(d) for d in raw.get("dimensions", [])),
        filters=filters,
        valid_from=int(raw.get("valid_from", 1)),
        agg=_parse_agg(raw["agg"]) if raw.get("agg") else None,
        distinct=str(raw["distinct"]) if raw.get("distinct") else None,
        numerator=numerator,
        denominator=denominator,
    )


def build_catalog(path: str | Path) -> Catalog:
    """解析 YAML 为不可变语义目录，并做完整的结构校验。"""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise UnsupportedSchemaError("语义定义文件顶层必须是映射")
    version = raw.get("version")
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise UnsupportedSchemaError(
            f"语义 schema 版本 {version!r} 不受支持，"
            f"当前编译器支持: {list(SUPPORTED_SCHEMA_VERSIONS)}；"
            "拒绝按旧语义猜测执行，请升级定义文件或编译器"
        )

    tz = raw.get("timezone", {}) or {}
    biz_offset = int(tz.get("business_offset_hours", 0))
    if not (-23 <= biz_offset <= 23):
        raise UnsupportedSchemaError(f"business_offset_hours 非法: {biz_offset}")

    sources: dict[str, SourceSpec] = {}
    for sname, sraw in (raw.get("sources") or {}).items():
        columns = tuple(str(c) for c in sraw.get("columns", []))
        dims_raw = sraw.get("dimensions", {}) or {}
        dimensions = tuple(dims_raw.keys())
        dim_types = {str(k): str(v.get("type", "string")) for k, v in dims_raw.items()}
        unknown_types = set(dim_types.values()) - {"string", "number", "boolean"}
        if unknown_types:
            raise UnsupportedSchemaError(
                f"来源 {sname} 存在未注册维度类型: {sorted(unknown_types)}"
            )
        dedup = (sraw.get("dedup") or {}).get("strategy", "none")
        if dedup not in _DEDUP_STRATEGIES:
            raise UnsupportedSchemaError(f"来源 {sname} 去重策略非法: {dedup}")
        sources[str(sname)] = SourceSpec(
            name=str(sname),
            table=str(sraw.get("table", sname)),
            time_column=str(sraw["time_column"]),
            columns=columns,
            dimensions=dimensions,
            dimension_types=dim_types,
            dedup_strategy=dedup,
            biz_offset_hours=biz_offset,
        )

    metrics: dict[str, MetricSpec] = {}
    for mraw in raw.get("metrics", []):
        metric = _parse_metric(mraw, biz_offset)
        if metric.source not in sources:
            raise UnsupportedSchemaError(
                f"指标 {metric.name} 引用了未定义来源 {metric.source}"
            )
        source = sources[metric.source]
        if metric.time_column != source.time_column:
            raise UnsupportedSchemaError(
                f"指标 {metric.name} 的 time_column 与来源 {source.name} 不一致"
            )
        bad_dims = [d for d in metric.dimensions if d not in source.dimensions]
        if bad_dims:
            raise UnsupportedSchemaError(
                f"指标 {metric.name} 的维度 {bad_dims} 未在来源 {source.name} 注册"
            )
        if metric.name in metrics:
            raise UnsupportedSchemaError(f"指标名重复: {metric.name}")
        metrics[metric.name] = metric

    return Catalog(
        schema_version=int(version),
        biz_offset_hours=biz_offset,
        sources=sources,
        metrics=metrics,
    )


# --------------------------------------------------------------------------- 历史外观


@dataclass
class Metric:
    """指标定义的只读视图（历史 API，字段与 v1 保持一致）。"""

    name: str
    label: str
    description: str
    kind: str
    source: str
    expr: str
    filters: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    time_column: str | None = None
    additivity: str = "additive"
    valid_from: int = 1

    @classmethod
    def from_spec(cls, spec: MetricSpec) -> "Metric":
        if spec.kind == "measure":
            assert spec.agg is not None
            expr = f"{spec.agg.func}({spec.agg.expr})"
        elif spec.kind == "distinct":
            expr = f"count(DISTINCT {spec.distinct})"
        else:
            assert spec.numerator is not None and spec.denominator is not None
            num = spec.numerator.distinct or (
                f"{spec.numerator.agg.func}({spec.numerator.agg.expr})"
                if spec.numerator.agg
                else "?"
            )
            den = spec.denominator.distinct or (
                f"{spec.denominator.agg.func}({spec.denominator.agg.expr})"
                if spec.denominator.agg
                else "?"
            )
            expr = f"{num} / {den}"
        return cls(
            name=spec.name,
            label=spec.label,
            description=spec.description,
            kind=spec.kind,
            source=spec.source,
            expr=expr,
            filters=[f.value if f.op == "__raw__" else f"{f.column} {f.op} {f.value}" for f in spec.filters],
            dimensions=list(spec.dimensions),
            time_column=spec.time_column,
            additivity=spec.additivity,
            valid_from=spec.valid_from,
        )


class MetricStore:
    """历史外观：内部已改为 v2 Catalog + Compiler。"""

    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self._compiler = Compiler(catalog)
        self.metrics: dict[str, Metric] = {
            name: Metric.from_spec(spec) for name, spec in catalog.metrics.items()
        }

    @classmethod
    def load(cls, path: Path | str) -> "MetricStore":
        return cls(build_catalog(path))

    def get(self, name: str) -> Metric:
        if name not in self.metrics:
            raise KeyError(f"unknown metric: {name}")
        return self.metrics[name]

    def names(self) -> list[str]:
        return sorted(self.metrics)

    @property
    def compiler(self) -> Compiler:
        return self._compiler

    # ----- 历史编译方法：签名不变，内部走 v2 编译器（参数化 SQL） -----

    def compile(self, name: str, **kwargs: Any) -> str:
        request = QueryRequest.build(metric=name)
        return self._compiler.compile(request).base.sql

    def compile_grouped(self, name: str, dimensions: list[str]) -> str:
        request = QueryRequest.build(metric=name, dimensions=list(dimensions))
        return self._compiler.compile(request).base.sql
