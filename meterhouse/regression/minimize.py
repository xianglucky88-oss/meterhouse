"""失败用例最小化（delta debugging）。

回归失败时要给业务方/开发一个“最小可复现输入”，否则 2 万行夹具里的一个
重复事实很难定位。这里实现 ddmin 的两个变体：

* ``minimize_dataset``：在保持失败的前提下，尽可能删掉事实行；
* ``minimize_scenario``：在保持失败的前提下，尽可能删掉查询与检查。

算法只在“删除后仍然失败”时才保留删除，否则恢复；结果完全可复现。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Sequence, TypeVar

from .fixtures import Dataset
from .models import Scenario

T = TypeVar("T")


@dataclass
class MinimizationResult:
    dataset: Dataset
    removed_supplier_rows: int
    removed_order_rows: int
    attempts: int


@dataclass
class ScenarioMinimization:
    scenario: Scenario
    removed_queries: int
    removed_checks: int
    attempts: int


def _subset_dataset(dataset: Dataset, keep_supplier: Sequence[int], keep_order: Sequence[int]) -> Dataset:
    return Dataset(
        name=dataset.name,
        supplier_stats=[dict(dataset.supplier_stats[i]) for i in keep_supplier],
        order_facts=[dict(dataset.order_facts[i]) for i in keep_order],
        origin={**dataset.origin, "minimized": True},
    )


def ddmin(items: Sequence[T], predicate: Callable[[list[T]], bool]) -> tuple[list[T], int]:
    """通用 ddmin，返回保留的元素与尝试次数。"""
    current = list(items)
    attempts = 0
    granularity = 2
    while len(current) >= 2:
        chunk = max(1, len(current) // granularity)
        reduced = False
        for offset in range(0, len(current), chunk):
            candidate = current[:offset] + current[offset + chunk:]
            if not candidate:
                continue
            attempts += 1
            if predicate(candidate):
                current = candidate
                granularity = max(granularity - 1, 2)
                reduced = True
                break
        if not reduced:
            if granularity >= len(current):
                break
            granularity = min(len(current), granularity * 2)
    return current, attempts


def minimize_dataset(
    dataset: Dataset,
    fails: Callable[[Dataset], bool],
    *,
    min_order_rows: int = 0,
) -> MinimizationResult:
    """先删订单行，再删快照行，最后再尝试删订单行。"""
    supplier = list(dataset.supplier_stats)
    orders = list(dataset.order_facts)
    attempts = 0

    if len(orders) > min_order_rows:
        kept, a = ddmin(orders, lambda cand: fails(Dataset(dataset.name, supplier, cand, {**dataset.origin, "minimized": True})))
        orders, attempts = kept, attempts + a

    if supplier:
        def supplier_pred(cand: list[dict[str, Any]]) -> bool:
            return fails(Dataset(dataset.name, cand, orders, {**dataset.origin, "minimized": True}))

        supplier, a = ddmin(supplier, supplier_pred)
        attempts += a

    if len(orders) > min_order_rows:
        def order_pred(cand: list[dict[str, Any]]) -> bool:
            return fails(Dataset(dataset.name, supplier, cand, {**dataset.origin, "minimized": True}))

        orders, a = ddmin(orders, order_pred)
        attempts += a

    result = Dataset(dataset.name, supplier, orders, {**dataset.origin, "minimized": True})
    return MinimizationResult(
        dataset=result,
        removed_supplier_rows=len(dataset.supplier_stats) - len(supplier),
        removed_order_rows=len(dataset.order_facts) - len(orders),
        attempts=attempts,
    )


def minimize_scenario(scenario: Scenario, fails: Callable[[Scenario], bool]) -> ScenarioMinimization:
    """收缩查询与检查集合；至少保留一条检查（无断言的场景永远通过）。"""
    attempts = 0
    queries = list(scenario.queries)
    checks = list(scenario.checks)

    if len(queries) > 1:
        queries, a = ddmin(queries, lambda cand: fails(_replace(scenario, queries=cand)))
        attempts += a

    if len(checks) > 1:
        checks, a = ddmin(checks, lambda cand: fails(_replace(scenario, checks=cand)))
        attempts += a

    return ScenarioMinimization(
        scenario=_replace(scenario, queries=queries, checks=checks),
        removed_queries=len(scenario.queries) - len(queries),
        removed_checks=len(scenario.checks) - len(checks),
        attempts=attempts,
    )


def _replace(scenario: Scenario, queries: list[Any] | None = None, checks: list[Any] | None = None) -> Scenario:
    return Scenario(
        group=scenario.group,
        id=scenario.id,
        description=scenario.description,
        dataset=scenario.dataset,
        queries=tuple(queries if queries is not None else scenario.queries),
        checks=tuple(checks if checks is not None else scenario.checks),
        sampling=scenario.sampling,
        tags=scenario.tags,
    )
