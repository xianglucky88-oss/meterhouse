"""指标语义层（骨架）。

状态：**未完成**。目前只有 YAML 加载与最简单的 SQL 拼装，
不支持维度下钻、时间粒度、同环比，也不能表达"去重后求平均"这类指标。

这是题目要补的部分。设计意图见 docs/design.md。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class Metric:
    """一个指标定义。"""

    name: str
    label: str
    description: str
    # 指标类型：simple（聚合）、ratio（分子/分母）、distinct（去重计数）
    kind: str
    source: str
    expr: str
    filters: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    # 时间列。语义层需要知道按哪个列做时间过滤。
    time_column: str | None = None
    # 可加性：additive | semi_additive | non_additive
    # 上卷时的行为由它决定（见 docs/design.md「可加性」）
    additivity: str = "additive"

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Metric":
        return cls(
            name=raw["name"],
            label=raw.get("label", raw["name"]),
            description=raw.get("description", ""),
            kind=raw.get("kind", "simple"),
            source=raw["source"],
            expr=raw["expr"],
            filters=raw.get("filters", []),
            dimensions=raw.get("dimensions", []),
            time_column=raw.get("time_column"),
            additivity=raw.get("additivity", "additive"),
        )


@dataclass
class MetricStore:
    """指标定义集合。"""

    metrics: dict[str, Metric] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "MetricStore":
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        store = cls()
        for m in raw.get("metrics", []):
            metric = Metric.from_dict(m)
            store.metrics[metric.name] = metric
        return store

    def get(self, name: str) -> Metric:
        if name not in self.metrics:
            raise KeyError(f"unknown metric: {name}")
        return self.metrics[name]

    def names(self) -> list[str]:
        return sorted(self.metrics)

    # ---------- 编译（未完成） ----------

    def compile(self, name: str, **kwargs: Any) -> str:
        """把一个指标编译成 SQL。

        当前只支持最简单的情况：无维度、无时间过滤、单一聚合表达式。
        参数 `dimensions` / `granularity` / `compare` 会被忽略。
        """
        m = self.get(name)
        where = ""
        if m.filters:
            where = "WHERE " + " AND ".join(m.filters)
        return f"SELECT {m.expr} AS {m.name} FROM {m.source} {where}"

    def compile_grouped(self, name: str, dimensions: list[str]) -> str:
        """按维度分组。维度必须已在指标定义里声明。

        未实现：粒度的合法性校验、维度上卷后的可加性处理。
        """
        m = self.get(name)
        bad = [d for d in dimensions if d not in m.dimensions]
        if bad:
            raise ValueError(f"metric {name} has no dimensions {bad}")
        dims = ", ".join(dimensions)
        where = ""
        if m.filters:
            where = "WHERE " + " AND ".join(m.filters)
        return (
            f"SELECT {dims}, {m.expr} AS {m.name} "
            f"FROM {m.source} {where} "
            f"GROUP BY {dims} ORDER BY {dims}"
        )
