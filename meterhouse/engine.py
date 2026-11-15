"""执行引擎：编译器与数据库之间唯一的执行通道。

职责：
    * 只接受 MetricPlan，不接受裸 SQL 字符串，接口层无法绕过语义约束；
    * 统一处理空结果（无维度查询返回 0 而非 None/缺行；分组查询返回空列表）；
    * 比率指标的隐藏分母列在此剥离，并在零分母时打 zero_denominator 标记；
    * 提供 rollup_total：可加指标求和，非可加/比率指标显式拒绝；
    * 同环比结果按分组键对齐到主查询行上，比较窗口缺组时给 None 而不是
      伪造 0（0 与"该组不存在"是两种语义）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb

from .compiler import (
    CompiledQuery,
    Compiler,
    MetricPlan,
    QueryRequest,
    SemanticError,
)


@dataclass(frozen=True)
class QueryResult:
    metric: str
    rows: list[dict[str, Any]]
    value: Any  # 无维度查询的标量结果；分组查询为 None
    warnings: tuple[str, ...]
    comparisons: dict[str, list[dict[str, Any]]]
    sql: str  # 参数化 SQL（参数单独给出，便于审计）
    params: tuple[Any, ...]
    rendered_sql: str  # 仅展示用

    def as_payload(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "value": self.value,
            "rows": self.rows,
            "warnings": list(self.warnings),
            "comparisons": self.comparisons,
            "sql": self.sql,
            "params": list(self.params),
            "rendered_sql": self.rendered_sql,
        }


class MetricEngine:
    def __init__(self, con: duckdb.DuckDBPyConnection, compiler: Compiler):
        self._con = con
        self._compiler = compiler

    @property
    def compiler(self) -> Compiler:
        return self._compiler

    def run(self, request: QueryRequest) -> QueryResult:
        plan = self._compiler.compile(request)
        rows, warnings = self._fetch(plan.base)
        grouped = bool(request.dimensions or request.granularity)

        comparisons: dict[str, list[dict[str, Any]]] = {}
        for cq in plan.comparisons:
            # 比较查询不做标量空归一：空窗口映射为缺组 None，与"窗口内真实为 0"区分开。
            comp_rows, comp_warnings = self._fetch(cq)
            window_present = self._window_present(cq)
            warnings.extend(comp_warnings)
            comparisons[cq.level] = self._align_comparison(
                plan, cq, rows, comp_rows, window_present
            )

        if grouped:
            value = None
        else:
            # 仅主查询的无维度标量做空结果归一：空表返回 0 而不是 None/缺行。
            value = rows[0][request.metric] if rows else 0
            rows = []

        return QueryResult(
            metric=request.metric,
            rows=rows,
            value=value,
            warnings=tuple(dict.fromkeys(plan.warnings + tuple(warnings))),
            comparisons=comparisons,
            sql=plan.base.sql,
            params=plan.base.params,
            rendered_sql=plan.rendered_sql(),
        )

    def rollup_total(self, metric_name: str, rows: list[dict[str, Any]]) -> Any:
        """沿维度上卷的唯一入口：可加性判断在编译器里，错误口径在此被挡住。"""
        return self._compiler.compile_rollup(metric_name, rows)

    # ------------------------------------------------------------------ 内部

    def _window_present(self, cq: CompiledQuery) -> bool:
        """窗口内是否存在原始行，用于区分空窗口与聚合零值。"""
        if cq.presence_sql is None:
            return True
        return bool(
            self._con.execute(cq.presence_sql, list(cq.presence_params)).fetchone()[0]
        )

    def _fetch(self, cq: CompiledQuery) -> tuple[list[dict[str, Any]], list[str]]:
        """执行并剥离隐藏分母列；不做空结果归一（归一策略由调用场景决定）。"""
        cursor = self._con.execute(cq.sql, list(cq.params))
        cols = [d[0] for d in cursor.description]
        raw = [dict(zip(cols, row)) for row in cursor.fetchall()]
        warnings: list[str] = []

        denom = cq.denominator_column
        if denom is not None:
            zero_rows = [r for r in raw if r.get(denom) in (0, None)]
            # 空窗口没有输出行，但对比率指标而言等价于分母为 0，同样需要显式告知。
            if zero_rows or not raw:
                warnings.append(
                    f"zero_denominator: {len(zero_rows)} 个分组分母为 0"
                    f"{'（含空窗口）' if not raw else ''}，"
                    f"{cq.request.metric} 在这些分组按 NULL 处理（已归一为 None）"
                )
            for r in raw:
                r.pop(denom, None)
        return raw, warnings

    def _align_comparison(
        self,
        plan: MetricPlan,
        cq: CompiledQuery,
        base_rows: list[dict[str, Any]],
        comp_rows: list[dict[str, Any]],
        window_present: bool,
    ) -> list[dict[str, Any]]:
        """把比较窗口的行按分组键对齐到主查询行序；缺组显式给 None。"""
        keys = self._group_keys(plan.request)
        if not keys:
            # 标量比较：窗口内无原始行时给一行 None，明确表达"窗口内无行"，
            # 而不是空列表（无结果）或聚合产生的 0（真实零值）。
            if not window_present:
                return [{plan.metric.name: None}]
            return comp_rows
        index = {tuple(r[k] for k in keys): r for r in comp_rows}
        aligned: list[dict[str, Any]] = []
        for base in base_rows:
            key = tuple(base[k] for k in keys)
            hit = index.get(key)
            if hit is None:
                row = {k: base[k] for k in keys}
                row[plan.metric.name] = None
            else:
                row = dict(hit)
            aligned.append(row)
        return aligned

    @staticmethod
    def _group_keys(request: QueryRequest) -> list[str]:
        keys: list[str] = []
        if request.granularity:
            keys.append("time_bucket")
        keys.extend(request.dimensions)
        return keys


__all__ = ["MetricEngine", "QueryResult", "SemanticError"]
