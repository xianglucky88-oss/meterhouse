"""回归场景的声明式模型。

设计约束
--------
* 场景描述只允许声明语义（数据形状、查询、期望不变量），不能内嵌“期望数字”。
  期望数字全部来自基准清单，基准变更必须走审批（见 baseline.py）。
* 场景组、场景、数据集都有规范化指纹，用于基准匹配与差异归因。
* 数据以“行 + 显式字段”描述，避免依赖生产 seed 的随机细节。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable

VALID_GRANULARITY = ("day", "week", "month")
VALID_CHECK_KINDS = (
    "value",            # 对照基准
    "invariant",        # 场景内自洽（可加性、比率重算等）
    "non_additive",     # 上卷不可加
    "ratio_recompute",  # 比率需按分子分母重算
    "sequence",         # 结果顺序敏感
    "cardinality",      # 结果基数
    "null_policy",      # 空值处理
)


def canonical_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class Query:
    """场景中的一个查询。参数与语义层 API 一一对应。"""

    metric: str
    dimensions: tuple[str, ...] = ()
    granularity: str | None = None
    filters: tuple[tuple[str, str, Any], ...] = ()
    time_range: tuple[str, str] | None = None
    fill_missing: bool = False

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "Query":
        filters = tuple(
            (f["column"], f.get("op", "eq"), f.get("value"))
            for f in raw.get("filters", [])
        )
        tr = raw.get("time_range")
        return cls(
            metric=raw["metric"],
            dimensions=tuple(raw.get("dimensions", [])),
            granularity=raw.get("granularity"),
            filters=filters,
            time_range=(tr["start"], tr["end"]) if tr else None,
            fill_missing=bool(raw.get("fill_missing", False)),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "dimensions": list(self.dimensions),
            "granularity": self.granularity,
            "filters": [{"column": c, "op": o, "value": v} for c, o, v in self.filters],
            "time_range": {"start": self.time_range[0], "end": self.time_range[1]} if self.time_range else None,
            "fill_missing": self.fill_missing,
        }

    @property
    def fingerprint(self) -> str:
        return canonical_hash(self.as_dict())

    def validate(self, metric_dimensions: Iterable[str]) -> None:
        bad = [d for d in self.dimensions if d not in set(metric_dimensions)]
        if bad:
            raise ValueError(f"query {self.metric}: undeclared dimensions {bad}")
        if self.granularity is not None and self.granularity not in VALID_GRANULARITY:
            raise ValueError(f"query {self.metric}: bad granularity {self.granularity}")
        # time_range 可以不带粒度（只是一个时间窗口）；fill_missing 必须有粒度。
        if self.fill_missing and self.granularity is None:
            raise ValueError(f"query {self.metric}: fill_missing requires granularity")
        if self.time_range is not None and self.time_range[0] >= self.time_range[1]:
            raise ValueError(f"query {self.metric}: time_range start must be before end")

    def key(self) -> tuple:
        return (self.fingerprint,)


@dataclass(frozen=True)
class Check:
    """场景里的一条断言。

    kind=value 时，expected 由基准提供，场景里不写明数字。
    kind=invariant 时，由 ``name`` 指向内置不变量检查器。
    """

    kind: str
    name: str
    query: Query | None = None
    related: tuple[Query, ...] = ()
    tolerance: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "Check":
        if raw["kind"] not in VALID_CHECK_KINDS:
            raise ValueError(f"unknown check kind {raw['kind']!r}")
        return cls(
            kind=raw["kind"],
            name=raw["name"],
            query=Query.from_raw(raw["query"]) if raw.get("query") else None,
            related=tuple(Query.from_raw(q) for q in raw.get("related", [])),
            tolerance=dict(raw.get("tolerance", {})),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "name": self.name,
            "query": self.query.as_dict() if self.query else None,
            "related": [q.as_dict() for q in self.related],
            "tolerance": self.tolerance,
        }

    @property
    def fingerprint(self) -> str:
        return canonical_hash(self.as_dict())


@dataclass(frozen=True)
class Scenario:
    """一个场景：数据集 + 一组声明式查询/断言 + 可选的抽样配置。"""

    group: str
    id: str
    description: str
    dataset: str
    queries: tuple[Query, ...] = ()
    checks: tuple[Check, ...] = ()
    sampling: dict[str, Any] | None = None
    tags: tuple[str, ...] = ()

    @classmethod
    def from_raw(cls, group: str, raw: dict[str, Any]) -> "Scenario":
        return cls(
            group=group,
            id=raw["id"],
            description=raw.get("description", ""),
            dataset=raw["dataset"],
            queries=tuple(Query.from_raw(q) for q in raw.get("queries", [])),
            checks=tuple(Check.from_raw(c) for c in raw.get("checks", [])),
            sampling=raw.get("sampling"),
            tags=tuple(raw.get("tags", [])),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "group": self.group,
            "id": self.id,
            "description": self.description,
            "dataset": self.dataset,
            "queries": [q.as_dict() for q in self.queries],
            "checks": [c.as_dict() for c in self.checks],
            "sampling": self.sampling,
            "tags": list(self.tags),
        }

    @property
    def fingerprint(self) -> str:
        # 描述与标签不影响口径，不进指纹。
        return canonical_hash(
            {
                "group": self.group,
                "id": self.id,
                "dataset": self.dataset,
                "queries": [q.as_dict() for q in self.queries],
                "checks": [c.as_dict() for c in self.checks],
                "sampling": self.sampling,
            }
        )

    @property
    def full_name(self) -> str:
        return f"{self.group}/{self.id}"

    def all_queries(self) -> list[Query]:
        out: list[Query] = list(self.queries)
        for c in self.checks:
            if c.query:
                out.append(c.query)
            out.extend(c.related)
        return out


@dataclass
class ScenarioSet:
    scenarios: list[Scenario] = field(default_factory=list)

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "ScenarioSet":
        out = cls()
        for group in raw.get("groups", []):
            for s in group.get("scenarios", []):
                scenario = Scenario.from_raw(group["name"], s)
                out.scenarios.append(scenario)
        out._validate()
        return out

    def _validate(self) -> None:
        ids = [s.full_name for s in self.scenarios]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate scenario id")
        for s in self.scenarios:
            for q in s.all_queries():
                if q.granularity is not None and q.granularity not in VALID_GRANULARITY:
                    raise ValueError(f"{s.full_name}: bad granularity {q.granularity}")

    def get(self, full_name: str) -> Scenario:
        for s in self.scenarios:
            if s.full_name == full_name:
                return s
        raise KeyError(f"unknown scenario: {full_name}")

    def by_group(self, group: str) -> list[Scenario]:
        return [s for s in self.scenarios if s.group == group]

    def validate_against(self, store: Any) -> None:
        """加载后、执行前的声明校验：指标存在、维度白名单、粒度合法。"""
        for scenario in self.scenarios:
            if scenario.dataset and not scenario.dataset.strip():
                raise ValueError(f"{scenario.full_name}: empty dataset name")
            for query in scenario.all_queries():
                metric = store.get(query.metric)  # KeyError -> 未知指标
                query.validate(metric.dimensions)
            sampling = scenario.sampling or {}
            if sampling:
                if "metric" not in sampling:
                    raise ValueError(f"{scenario.full_name}: sampling requires metric")
                metric = store.get(str(sampling["metric"]))
                Query(metric=str(sampling["metric"]), dimensions=tuple(sampling.get("dimensions", []))).validate(
                    metric.dimensions
                )
