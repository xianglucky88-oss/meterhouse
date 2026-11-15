"""结果规范化、容差比较与差异归因。

比较分三层，归因顺序固定，避免把真实语义回归误判为“精度差异”：

1. 形状层：列集合/基数/分组键。任何错位都是 shape_mismatch。
2. 数值层：按场景声明的容差比较；容差内记 storage_precision。
3. 参考层：oracle 重算与已审批基准的比较；不一致记 reference_drift。

``attribute`` 只负责归因，不做通过/失败判定；判定由 runner 依据
``Comparison.ok`` 与基准审批状态给出。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

from .models import Query


# --------------------------------------------------------------------------
# 规范化
# --------------------------------------------------------------------------


def normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        # 存储层（ClickHouse Decimal / DuckDB DECIMAL）返回 Decimal；
        # 统一成 float 只在比较时降精度，展示仍保留原始 repr。
        return float(value)
    if isinstance(value, datetime):
        return value.date().isoformat() if value.time() == datetime.min.time() else value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return value


def normalize_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: normalize_value(v) for k, v in row.items()} for row in rows]


def row_key(row: dict[str, Any], columns: list[str]) -> tuple:
    key = []
    for col in columns:
        value = row.get(col)
        # 空字符串与 None 是两种不同事实，不合并。
        key.append((value is None, value))
    return tuple(key)


def value_columns(query: Query) -> list[str]:
    return list(query.dimensions) + (["date"] if query.granularity else []) + [query.metric]


def sort_rows(rows: list[dict[str, Any]], columns: list[str]) -> list[dict[str, Any]]:
    def key(row: dict[str, Any]) -> tuple:
        return tuple((row.get(c) is None, str(row.get(c))) for c in columns)

    return sorted(rows, key=key)


# --------------------------------------------------------------------------
# 容差
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Tolerance:
    absolute: float = 0.0
    relative: float = 0.0
    # 存储精度差异：例如 ClickHouse 的 Decimal(18,2) 与 double 之间。
    # 只在两值都可量化到该小数位时使用。
    quantize: int | None = None
    # 允许的基数字段：用于大基数抽样时的期望缺席分组。
    allow_missing_rows: bool = False
    allow_extra_rows: bool = False

    @classmethod
    def from_raw(cls, raw: dict[str, Any] | None) -> "Tolerance":
        raw = raw or {}
        return cls(
            absolute=float(raw.get("absolute", raw.get("abs", 0.0))),
            relative=float(raw.get("relative", raw.get("rel", 0.0))),
            quantize=raw.get("quantize"),
            allow_missing_rows=bool(raw.get("allow_missing_rows", False)),
            allow_extra_rows=bool(raw.get("allow_extra_rows", False)),
        )

    def close(self, a: float, b: float) -> tuple[bool, str]:
        if a is None or b is None:
            return False, "null"
        if math.isnan(a) and math.isnan(b):
            return True, "nan"
        if self.quantize is not None:
            q = Decimal(10) ** -int(self.quantize)
            if Decimal(str(a)).quantize(q) == Decimal(str(b)).quantize(q):
                return True, "quantize"
        diff = abs(a - b)
        if diff <= self.absolute:
            return True, "absolute"
        scale = max(abs(a), abs(b))
        if scale and diff / scale <= self.relative:
            return True, "relative"
        return False, f"diff={diff:g}"


# --------------------------------------------------------------------------
# 比较
# --------------------------------------------------------------------------


@dataclass
class RowDiff:
    key: tuple
    expected: Any
    actual: Any
    kind: str  # value | missing | extra
    within_tolerance: bool = False
    detail: str = ""


@dataclass
class Comparison:
    query: Query
    ok: bool
    exact: bool
    tolerance: Tolerance
    diffs: list[RowDiff] = field(default_factory=list)
    precision_only: list[RowDiff] = field(default_factory=list)
    shape_error: str | None = None
    expected_rows: int = 0
    actual_rows: int = 0

    def summary(self) -> dict[str, Any]:
        return {
            "query": self.query.as_dict(),
            "ok": self.ok,
            "exact": self.exact,
            "precision_only": len(self.precision_only),
            "diffs": [
                {
                    "key": list(d.key),
                    "kind": d.kind,
                    "expected": d.expected,
                    "actual": d.actual,
                    "detail": d.detail,
                }
                for d in self.diffs[:50]
            ],
            "shape_error": self.shape_error,
            "expected_rows": self.expected_rows,
            "actual_rows": self.actual_rows,
        }


def compare_rows(
    query: Query,
    expected: list[dict[str, Any]],
    actual: list[dict[str, Any]],
    tolerance: Tolerance | None = None,
) -> Comparison:
    tol = tolerance or Tolerance()
    expected = normalize_rows(expected)
    actual = normalize_rows(actual)
    key_cols = list(query.dimensions) + (["date"] if query.granularity else [])
    cols = value_columns(query)

    for row in expected + actual:
        missing = [c for c in cols if c not in row]
        if missing:
            return Comparison(
                query=query,
                ok=False,
                exact=False,
                tolerance=tol,
                shape_error=f"missing columns {missing}",
                expected_rows=len(expected),
                actual_rows=len(actual),
            )

    exp_map = {row_key(r, key_cols): r for r in expected}
    act_map = {row_key(r, key_cols): r for r in actual}
    if len(exp_map) != len(expected):
        return Comparison(
            query=query, ok=False, exact=False, tolerance=tol,
            shape_error="duplicate group keys in expected", expected_rows=len(expected), actual_rows=len(actual),
        )
    if len(act_map) != len(actual):
        return Comparison(
            query=query, ok=False, exact=False, tolerance=tol,
            shape_error="duplicate group keys in actual", expected_rows=len(expected), actual_rows=len(actual),
        )

    diffs: list[RowDiff] = []
    precision: list[RowDiff] = []
    for key, exp in exp_map.items():
        act = act_map.get(key)
        if act is None:
            if tol.allow_missing_rows:
                continue
            diffs.append(RowDiff(key=key, expected=exp.get(query.metric), actual=None, kind="missing"))
            continue
        ev = exp.get(query.metric)
        av = act.get(query.metric)
        if _is_null(ev) and _is_null(av):
            continue
        if _is_null(ev) or _is_null(av):
            diffs.append(RowDiff(key=key, expected=ev, actual=av, kind="value", detail="null mismatch"))
            continue
        if isinstance(ev, (int, float)) and isinstance(av, (int, float)):
            close, detail = tol.close(float(ev), float(av))
            if not close:
                diffs.append(RowDiff(key=key, expected=ev, actual=av, kind="value", detail=detail))
            elif ev != av:
                precision.append(RowDiff(key=key, expected=ev, actual=av, kind="value", within_tolerance=True, detail=detail))
        elif ev != av:
            diffs.append(RowDiff(key=key, expected=ev, actual=av, kind="value", detail="exact mismatch"))

    for key, act in act_map.items():
        if key not in exp_map and not tol.allow_extra_rows:
            diffs.append(RowDiff(key=key, expected=None, actual=act.get(query.metric), kind="extra"))

    return Comparison(
        query=query,
        ok=not diffs,
        exact=not diffs and not precision,
        tolerance=tol,
        diffs=diffs,
        precision_only=precision,
        expected_rows=len(expected),
        actual_rows=len(actual),
    )


def _is_null(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


# --------------------------------------------------------------------------
# 归因
# --------------------------------------------------------------------------

ATTRIBUTIONS = (
    "match",
    "storage_precision",
    "candidate_regression",
    "reference_drift",
    "unapproved_baseline",
    "shape_mismatch",
)


@dataclass
class Attribution:
    verdict: str
    detail: str
    metric_changed: bool = False
    definition_version_from: int | None = None
    definition_version_to: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "detail": self.detail,
            "metric_changed": self.metric_changed,
            "definition_version_from": self.definition_version_from,
            "definition_version_to": self.definition_version_to,
        }


def attribute(
    comparison: Comparison,
    *,
    baseline_present: bool,
    baseline_metric: dict[str, Any] | None,
    current_metric: dict[str, Any] | None,
    reference_drift: bool,
) -> Attribution:
    """按固定优先级归因：基准 > 形状 > 定义 > 参考 > 候选 > 精度。

    * unapproved_baseline：没有已审批基准。
    * shape_mismatch：列缺失、分组键重复。
    * definition_change：定义指纹变了；旧审批自动失效，必须重新审批基准。
      定义一变，哪怕数字恰好相同也不能算 match，否则“改定义 + 顺手更新期望值”
      就能绕过门禁。
    * reference_drift：定义没变，但 oracle 重算与基准不一致 —— 夹具内容变了
      或参考实现变了。注意：oracle 由定义派生，所以定义变更引发的漂移必须
      归到 definition_change，不能借 reference_drift 逃避审批。
    * storage_precision：结果在容差内，仅表示精度差异。
    * candidate_regression：定义没变，候选结果越界。
    """
    if not baseline_present:
        return Attribution("unapproved_baseline", "no approved baseline for this scenario/query")
    if comparison.shape_error:
        return Attribution("shape_mismatch", comparison.shape_error)

    changed = bool(
        baseline_metric and current_metric
        and (baseline_metric.get("semantic_hash") != current_metric.get("semantic_hash")
             or baseline_metric.get("definition_version") != current_metric.get("definition_version"))
    )
    kwargs = {
        "metric_changed": changed,
        "definition_version_from": (baseline_metric or {}).get("definition_version"),
        "definition_version_to": (current_metric or {}).get("definition_version"),
    }
    if changed:
        return Attribution(
            "definition_change",
            "metric definition changed since baseline approval; re-approval required",
            **kwargs,
        )
    if reference_drift:
        return Attribution("reference_drift", "oracle recomputation differs from approved reference")
    if comparison.ok and comparison.exact:
        return Attribution("match", "candidate equals reference", **kwargs)
    if comparison.ok:
        return Attribution("storage_precision", "differences within declared tolerance", **kwargs)
    return Attribution(
        "candidate_regression",
        f"{len(comparison.diffs)} row(s) outside tolerance",
        **kwargs,
    )
