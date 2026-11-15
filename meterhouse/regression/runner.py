"""场景执行、检查与报告聚合。

执行模型
--------
* 每个场景在独立进程中执行（ProcessPoolExecutor），各自使用临时 DuckDB
  与独立夹具，避免连接共享、临时目录冲突与随机状态串扰。
* 场景内的执行顺序固定：候选结果 -> oracle 重算 -> 基准对照 -> 不变量检查。
* 报告按场景名排序，保证并行执行的输出可复现。
"""

from __future__ import annotations

import os
import traceback
from dataclasses import dataclass, field
from typing import Any

from meterhouse.semantic import MetricStore

from .candidates import Candidate, get_candidate
from .compare import Comparison, Tolerance, attribute, compare_rows, normalize_rows
from .fixtures import Dataset, FixtureLibrary
from .minimize import minimize_dataset
from .models import Check, Query, Scenario, ScenarioSet
from .oracle import Oracle
from .sampling import ProportionEstimate, decide

SEVERITY = {
    "candidate_regression": "block",
    "definition_change": "block",
    "reference_drift": "block",
    "unapproved_baseline": "block",
    "shape_mismatch": "block",
    "storage_precision": "note",
    "match": "ok",
    "invariant_failed": "block",
    "inconclusive": "warn",
}


@dataclass
class QueryOutcome:
    query: Query
    comparison: Comparison | None = None
    reference_drift: bool = False
    drift_diff: Comparison | None = None
    attribution: dict[str, Any] = field(default_factory=dict)
    reference_rows: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "query": self.query.as_dict(),
            "attribution": self.attribution,
            "comparison": self.comparison.summary() if self.comparison else None,
            "reference_drift": self.reference_drift,
            "reference_rows": self.reference_rows[:20],
        }


@dataclass
class InvariantOutcome:
    name: str
    kind: str
    ok: bool
    detail: str
    sampling: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        out = {"name": self.name, "kind": self.kind, "ok": self.ok, "detail": self.detail}
        if self.sampling:
            out["sampling"] = self.sampling
        return out


@dataclass
class ScenarioOutcome:
    scenario: str
    status: str
    queries: list[QueryOutcome] = field(default_factory=list)
    invariants: list[InvariantOutcome] = field(default_factory=list)
    skipped: list[dict[str, str]] = field(default_factory=list)
    error: str | None = None
    minimized: dict[str, Any] | None = None

    @property
    def failed(self) -> bool:
        if self.error:
            return True
        if any(SEVERITY.get(q.attribution.get("verdict", ""), "block") == "block" for q in self.queries):
            return True
        return any(not i.ok for i in self.invariants)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "status": self.status,
            "error": self.error,
            "queries": [q.as_dict() for q in self.queries],
            "invariants": [i.as_dict() for i in self.invariants],
            "skipped": self.skipped,
            "minimized": self.minimized,
        }


# --------------------------------------------------------------------------
# 场景执行
# --------------------------------------------------------------------------


def run_scenario(
    scenario: Scenario,
    *,
    dataset: Dataset,
    store: MetricStore,
    candidate: Candidate,
    baseline: Any,
    minimize: bool = False,
    minimize_attempts: int = 40,
) -> ScenarioOutcome:
    """执行单个场景，返回结构化结果。纯函数式，便于并行与最小化重放。"""
    outcome = ScenarioOutcome(scenario=scenario.full_name, status="pass")
    oracle = Oracle.from_store(store)
    metric_fps = store.fingerprints()
    dataset_drift = _dataset_drift(scenario, dataset, baseline)
    # 场景本身（查询集合/断言/容差）变了，旧基准同样失效；
    # 不能只在 diff 里提示，否则 CI 的 run 会静默放行。
    if baseline is not None:
        ref = baseline.scenario(scenario)
        if ref is not None and ref.fingerprint != scenario.fingerprint:
            outcome.error = (
                "scenario fingerprint changed since baseline approval; "
                "re-approval required (query/check/tolerance changed)"
            )
            outcome.status = "fail"
            return outcome
    queries = list(scenario.queries)
    for check in scenario.checks:
        if check.query:
            queries.append(check.query)
        queries.extend(check.related)

    seen: set[str] = set()
    skipped: list[dict[str, str]] = []
    for query in queries:
        if query.fingerprint in seen:
            continue
        seen.add(query.fingerprint)
        if not candidate.supports(query):
            skipped.append({"metric": query.metric, "query": query.fingerprint})
            continue
        try:
            outcome.queries.append(
                _run_query(
                    query,
                    scenario=scenario,
                    dataset=dataset,
                    store=store,
                    oracle=oracle,
                    candidate=candidate,
                    baseline=baseline,
                    metric_fps=metric_fps,
                    tolerance=Tolerance.from_raw(_tolerance_for(scenario, query)),
                    dataset_drift=dataset_drift,
                )
            )
        except Exception as exc:  # noqa: BLE001
            outcome.error = f"{query.metric}: {exc}\n{traceback.format_exc()}"
            outcome.status = "error"
            return outcome

    for check in scenario.checks:
        if check.kind == "value" or check.query is None:
            continue
        if not all(candidate.supports(q) for q in [check.query] + list(check.related) if q):
            skipped.append({"check": check.name, "kind": check.kind})
            continue
        try:
            inv = _run_invariant(check, scenario, dataset, store, oracle, candidate, baseline)
            outcome.invariants.append(inv)
        except Exception as exc:  # noqa: BLE001
            outcome.invariants.append(InvariantOutcome(check.name, check.kind, False, f"error: {exc}"))
    if scenario.sampling:
        sampling_query = Query(
            metric=str(scenario.sampling.get("metric")),
            dimensions=tuple(scenario.sampling.get("dimensions", [])),
        )
        if not candidate.supports(sampling_query):
            skipped.append({"check": "sampling", "kind": "sampling"})
        else:
            try:
                outcome.invariants.append(_run_sampling(scenario, dataset, store, oracle, candidate))
            except Exception as exc:  # noqa: BLE001
                outcome.invariants.append(InvariantOutcome("sampling", "sampling", False, f"error: {exc}"))

    outcome.status = "fail" if outcome.failed else "pass"
    if outcome.status == "pass" and any(
        (i.kind == "sampling" and i.sampling and i.sampling.get("status") == "inconclusive")
        for i in outcome.invariants
    ):
        # inconclusive 只告警：样本量不足不代表语义错误，不能阻断门禁。
        outcome.status = "warn"
    outcome.skipped = skipped
    if minimize and outcome.failed:
        outcome.minimized = _minimize_outcome(
            scenario, dataset, store, candidate, baseline, attempts=minimize_attempts
        )
    return outcome


def _tolerance_for(scenario: Scenario, query: Query) -> dict[str, Any] | None:
    for check in scenario.checks:
        if check.kind == "value" and check.query and check.query.fingerprint == query.fingerprint:
            return check.tolerance
    return None


def _dataset_drift(scenario: Scenario, dataset: Dataset, baseline: Any) -> bool:
    """夹具内容变化也必须归因为参考漂移，哪怕数字碰巧相同。

    基准里记录的是数据集内容哈希；只比数字会漏掉“数据变了但结果没变”的情况。
    """
    if baseline is None:
        return False
    recorded = baseline.dataset_hash(scenario.dataset)
    return recorded is not None and recorded != dataset.hash()


def _run_query(
    query: Query,
    *,
    scenario: Scenario,
    dataset: Dataset,
    store: MetricStore,
    oracle: Oracle,
    candidate: Candidate,
    baseline: Any,
    metric_fps: dict[str, Any],
    tolerance: Tolerance,
    dataset_drift: bool = False,
) -> QueryOutcome:
    actual = candidate.execute(query, dataset, store)
    reference = oracle.evaluate(query, dataset)
    comparison = compare_rows(query, reference, actual, tolerance)
    baseline_metric = baseline.metric(query.metric) if baseline else None
    current_metric = metric_fps.get(query.metric)
    ref = baseline.query_ref(scenario, query) if baseline else None

    drift = dataset_drift
    drift_comparison = None
    if ref is not None:
        drift_comparison = compare_rows(query, ref.rows, reference, Tolerance())
        drift = drift or not drift_comparison.ok

    attribution = attribute(
        comparison,
        baseline_present=ref is not None,
        baseline_metric=baseline_metric,
        current_metric=current_metric,
        reference_drift=drift,
    )
    return QueryOutcome(
        query=query,
        comparison=comparison,
        reference_drift=drift,
        drift_diff=drift_comparison,
        attribution=attribution.as_dict(),
        reference_rows=normalize_rows(reference),
    )


# --------------------------------------------------------------------------
# 不变量
# --------------------------------------------------------------------------


def _metric_rows(candidate: Candidate, query: Query, dataset: Dataset, store: MetricStore) -> list[dict[str, Any]]:
    return normalize_rows(candidate.execute(query, dataset, store))


def _value(row: dict[str, Any], metric: str) -> float | None:
    value = row.get(metric)
    return None if value is None else float(value)


def _sum_rows(rows: list[dict[str, Any]], metric: str, predicate: Any = None) -> float:
    total = 0.0
    for row in rows:
        value = _value(row, metric)
        if value is None:
            continue
        if predicate and not predicate(row):
            continue
        total += value
    return total


def _run_invariant(
    check: Check,
    scenario: Scenario,
    dataset: Dataset,
    store: MetricStore,
    oracle: Oracle,
    candidate: Candidate,
    baseline: Any,
) -> InvariantOutcome:
    kind = check.kind
    if kind == "invariant":
        return _invariant_named(check, candidate, dataset, store)
    if kind == "non_additive":
        return _non_additive(check, candidate, dataset, store)
    if kind == "ratio_recompute":
        return _ratio_recompute(check, candidate, dataset, store)
    if kind == "null_policy":
        return _null_policy(check, candidate, dataset, store)
    if kind == "sequence":
        return _sequence(check, candidate, dataset, store)
    if kind == "cardinality":
        return _cardinality(check, scenario, oracle, candidate, dataset, store)
    raise ValueError(f"unsupported check kind {kind}")


def _invariant_named(check: Check, candidate: Candidate, dataset: Dataset, store: MetricStore) -> InvariantOutcome:
    name = check.name
    if name == "additive_rollup":
        assert check.query and check.related, "additive_rollup requires query + related[coarse]"
        fine = _metric_rows(candidate, check.query, dataset, store)
        coarse = _metric_rows(candidate, check.related[0], dataset, store)
        fine_total = _sum_rows(fine, check.query.metric)
        coarse_value = _value(coarse[0], check.query.metric) if coarse else None
        ok = coarse_value is not None and abs(fine_total - coarse_value) <= 1e-9 * max(1.0, abs(coarse_value))
        return InvariantOutcome(
            check.name, check.kind, bool(ok),
            f"sum(fine)={fine_total:g} coarse={coarse_value}",
        )
    if name == "null_group_preserves_total":
        assert check.query and check.related, "null_group_preserves_total requires grouped query + total query"
        grouped = _metric_rows(candidate, check.query, dataset, store)
        total_rows = _metric_rows(candidate, check.related[0], dataset, store)
        grouped_total = _sum_rows(grouped, check.query.metric)
        total = _value(total_rows[0], check.query.metric) if total_rows else None
        ok = total is not None and abs(grouped_total - total) <= 1e-9 * max(1.0, abs(total))
        return InvariantOutcome(
            check.name, check.kind, bool(ok),
            f"sum(grouped)={grouped_total:g} total={total}",
        )
    raise ValueError(f"unknown invariant {name}")


def _non_additive(check: Check, candidate: Candidate, dataset: Dataset, store: MetricStore) -> InvariantOutcome:
    """去重类指标沿时间上卷必须“不可加”：各期之和 >= 全期值，且在夹具设计上严格更大。"""
    assert check.related, "non_additive requires related queries (coarse + fine)"
    coarse_q, fine_q = check.related[0], check.related[1]
    coarse = _metric_rows(candidate, coarse_q, dataset, store)
    fine = _metric_rows(candidate, fine_q, dataset, store)
    coarse_value = _value(coarse[0], coarse_q.metric) if coarse else None
    fine_total = _sum_rows(fine, fine_q.metric)
    if coarse_value is None:
        return InvariantOutcome(check.name, check.kind, False, "coarse value is null")
    ok = fine_total >= coarse_value - 1e-9 and fine_total > coarse_value + 1e-9
    return InvariantOutcome(
        check.name, check.kind, ok,
        f"sum(fine)={fine_total:g} vs coarse={coarse_value:g} "
        f"({'strictly non-additive' if ok else 'unexpected: non-additivity not observable'})",
    )


def _ratio_recompute(check: Check, candidate: Candidate, dataset: Dataset, store: MetricStore) -> InvariantOutcome:
    """比率指标必须“分子/分母重算”，不能对分组比率求平均。

    check.query 是分组比率；check.related[0]/[1] 是同一分组的分子、分母查询。
    逐组校验 candidate_ratio == numerator/denominator，并校验全量比率 ==
    加权重算值（而不是分组比率的算术平均）。
    """
    assert check.query and len(check.related) >= 2, "ratio_recompute requires ratio query + numerator + denominator"
    ratio_q, num_q, den_q = check.query, check.related[0], check.related[1]
    ratio = _metric_rows(candidate, ratio_q, dataset, store)
    num = {tuple(r.get(d) for d in num_q.dimensions): _value(r, num_q.metric) for r in _metric_rows(candidate, num_q, dataset, store)}
    den = {tuple(r.get(d) for d in den_q.dimensions): _value(r, den_q.metric) for r in _metric_rows(candidate, den_q, dataset, store)}
    problems: list[str] = []
    weighted_num = 0.0
    weighted_den = 0.0
    for row in ratio:
        key = tuple(row.get(d) for d in ratio_q.dimensions)
        n = num.get(key)
        d = den.get(key)
        if d in (None, 0):
            continue
        recomputed = (n or 0) / d
        actual = _value(row, ratio_q.metric)
        if actual is None or abs(actual - recomputed) > 1e-9:
            problems.append(f"{key}: actual={actual} recomputed={recomputed}")
        weighted_num += n or 0
        weighted_den += d
    naive = sum(_value(r, ratio_q.metric) or 0 for r in ratio) / len(ratio) if ratio else None
    recomputed_global = weighted_num / weighted_den if weighted_den else None
    if naive is not None and recomputed_global is not None and abs(naive - recomputed_global) <= 1e-12:
        problems.append("fixture does not distinguish naive average from weighted recompute")
    ok = not problems
    detail = (
        f"groups={len(ratio)} weighted={recomputed_global} naive_mean={naive}"
        if ok
        else "; ".join(problems[:5])
    )
    return InvariantOutcome(check.name, check.kind, ok, detail)


def _null_policy(check: Check, candidate: Candidate, dataset: Dataset, store: MetricStore) -> InvariantOutcome:
    """按可空维度分组时，NULL 必须是独立分组且被计入总数。"""
    assert check.query and check.related, "null_policy requires grouped query + is_null query"
    grouped_q, filtered_q = check.query, check.related[0]
    grouped = _metric_rows(candidate, grouped_q, dataset, store)
    filtered = _metric_rows(candidate, filtered_q, dataset, store)
    null_key = tuple(None for _ in grouped_q.dimensions)
    grouped_value = None
    for row in grouped:
        if tuple(row.get(d) for d in grouped_q.dimensions) == null_key:
            grouped_value = _value(row, grouped_q.metric)
    filtered_value = _value(filtered[0], filtered_q.metric) if filtered else None
    ok = grouped_value is not None and filtered_value is not None and grouped_value == filtered_value
    return InvariantOutcome(
        check.name, check.kind, ok,
        f"null group={grouped_value} filtered={filtered_value}",
    )


def _sequence(check: Check, candidate: Candidate, dataset: Dataset, store: MetricStore) -> InvariantOutcome:
    """结果顺序必须是确定的升序（按维度与日期），不得依赖存储返回顺序。"""
    assert check.query
    rows = _metric_rows(candidate, check.query, dataset, store)
    cols = list(check.query.dimensions) + (["date"] if check.query.granularity else [])
    if not cols:
        return InvariantOutcome(check.name, check.kind, True, "single row; ordering trivially stable")
    observed = [tuple(r.get(c) for c in cols) for r in rows]
    expected = sorted(observed, key=lambda k: tuple((v is None, str(v)) for v in k))
    ok = observed == expected
    return InvariantOutcome(check.name, check.kind, ok, f"rows={len(rows)} sorted={ok}")


def _cardinality(check: Check, scenario: Scenario, oracle: Oracle, candidate: Candidate, dataset: Dataset, store: MetricStore) -> InvariantOutcome:
    """结果基数必须与参考一致（维度组合爆炸/漏分组都会在此暴露）。"""
    assert check.query
    actual = _metric_rows(candidate, check.query, dataset, store)
    expected = normalize_rows(oracle.evaluate(check.query, dataset))
    ok = len(actual) == len(expected)
    return InvariantOutcome(check.name, check.kind, ok, f"actual_rows={len(actual)} expected_rows={len(expected)}")


# --------------------------------------------------------------------------
# 抽样
# --------------------------------------------------------------------------


def _run_sampling(
    scenario: Scenario,
    dataset: Dataset,
    store: MetricStore,
    oracle: Oracle,
    candidate: Candidate,
) -> InvariantOutcome:
    """大基数场景的分组级抽样校验。

    从参考分组中按 sha256 确定性抽样，对每个抽样分组做点查询比较，
    用二项比例置信区间判定。样本不足时返回 inconclusive（warn），
    不因随机波动误报失败，也不放过区间完全在外的真实偏差。
    """
    config = dict(scenario.sampling or {})
    metric = config.get("metric")
    if not metric:
        raise ValueError("sampling config requires metric")
    sample_size = int(config.get("sample_size", 200))
    reference = float(config.get("expected_pass_ratio", 1.0))
    tolerance = float(config.get("pass_ratio_tolerance", 0.02))
    value_tolerance = float(config.get("value_relative_tolerance", 1e-9))
    dims = tuple(config.get("dimensions", []))

    base_query = Query(metric=metric, dimensions=dims)
    reference_rows = normalize_rows(oracle.evaluate(base_query, dataset))
    from .sampling import deterministic_hash_sample

    keys: list[tuple] = [tuple(row.get(d) for d in dims) for row in reference_rows]
    sampled_keys: list[tuple] = deterministic_hash_sample(keys, sample_size)

    successes = 0
    for key in sampled_keys:
        filters = []
        for i, dim in enumerate(dims):
            value = key[i]
            filters.append({"column": dim, "op": "is_null" if value is None else "eq", "value": value})
        point = Query(metric=metric, dimensions=dims, filters=tuple((f["column"], f["op"], f["value"]) for f in filters))
        # 点查询的参考是该分组在参考结果中的值。
        expected_row = next((r for r in reference_rows if tuple(r.get(d) for d in dims) == key), None)
        actual_rows = normalize_rows(candidate.execute(point, dataset, store))
        actual_row = next((r for r in actual_rows if tuple(r.get(d) for d in dims) == key), None)
        if expected_row is None or actual_row is None:
            continue
        ev, av = expected_row.get(metric), actual_row.get(metric)
        if (ev is None and av is None) or (
            ev is not None and av is not None and _close_float(float(ev), float(av), value_tolerance)
        ):
            successes += 1
    verdict = decide(ProportionEstimate(successes, len(sampled_keys)), reference, tolerance)
    return InvariantOutcome(
        "sampling", "sampling", verdict.status != "fail",
        f"status={verdict.status} p={verdict.estimate.p:.4f} n={verdict.estimate.n} "
        f"ci={verdict.estimate.interval[0]:.4f}..{verdict.estimate.interval[1]:.4f}",
        sampling=verdict.as_dict(),
    )


def _close_float(a: float, b: float, tol: float) -> bool:
    scale = max(abs(a), abs(b), 1.0)
    return abs(a - b) / scale <= tol


# --------------------------------------------------------------------------
# 最小化
# --------------------------------------------------------------------------


def _minimize_outcome(
    scenario: Scenario,
    dataset: Dataset,
    store: MetricStore,
    candidate: Candidate,
    baseline: Any,
    *,
    attempts: int,
) -> dict[str, Any]:
    """最小化“候选与参考口径不一致”的输入。

    注意：最小化不能拿静态基准当失败判据 —— 删行会让基准不再对应当前数据，
    任何收缩都会“保持失败”，最小化结果就失去意义。这里改为对当前子集
    重算 oracle 并比较候选，只保留真正的语义偏差。
    """
    oracle = Oracle.from_store(store)
    queries = [q for q in scenario.all_queries() if candidate.supports(q)]
    if not queries:
        return {"skipped": "no supported query"}

    def still_fails(ds: Dataset) -> bool:
        for query in queries:
            tolerance = Tolerance.from_raw(_tolerance_for(scenario, query))
            comparison = compare_rows(query, oracle.evaluate(query, ds), candidate.execute(query, ds, store), tolerance)
            if not comparison.ok:
                return True
        return False

    if not still_fails(dataset):
        return {"skipped": "no candidate/oracle mismatch to minimize"}

    result = minimize_dataset(dataset, still_fails, min_order_rows=min(1, attempts))
    return {
        "order_rows": len(result.dataset.order_facts),
        "supplier_rows": len(result.dataset.supplier_stats),
        "removed_order_rows": result.removed_order_rows,
        "removed_supplier_rows": result.removed_supplier_rows,
        "attempts": result.attempts,
        "sample_order_rows": [ {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()} for row in result.dataset.order_facts[:10]],
    }


# --------------------------------------------------------------------------
# 并行执行
# --------------------------------------------------------------------------


@dataclass
class RunReport:
    outcomes: list[ScenarioOutcome]
    candidate: str
    baseline_path: str

    def summary(self) -> dict[str, Any]:
        counts = {"pass": 0, "fail": 0, "error": 0, "warn": 0}
        verdicts: dict[str, int] = {}
        for outcome in self.outcomes:
            status = outcome.status
            counts[status] = counts.get(status, 0) + 1
            for q in outcome.queries:
                verdict = q.attribution.get("verdict", "unknown")
                verdicts[verdict] = verdicts.get(verdict, 0) + 1
            for inv in outcome.invariants:
                if not inv.ok:
                    verdicts["invariant_failed"] = verdicts.get("invariant_failed", 0) + 1
                elif inv.kind == "sampling" and inv.sampling and inv.sampling.get("status") == "inconclusive":
                    verdicts["inconclusive"] = verdicts.get("inconclusive", 0) + 1
        return {
            "candidate": self.candidate,
            "baseline": self.baseline_path,
            "counts": counts,
            "verdicts": dict(sorted(verdicts.items())),
            "failed": [o.scenario for o in self.outcomes if o.failed],
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary(),
            "scenarios": [o.as_dict() for o in sorted(self.outcomes, key=lambda o: o.scenario)],
        }


def run_all(
    scenario_set: ScenarioSet,
    fixtures: FixtureLibrary,
    store: MetricStore,
    baseline: Any,
    *,
    candidate_name: str = "semantic",
    jobs: int | None = None,
    minimize: bool = False,
    minimize_attempts: int = 40,
    only: list[str] | None = None,
) -> RunReport:
    selected = scenario_set.scenarios
    if only:
        selected = [s for s in selected if s.full_name in only or s.group in only]
        missing = set(only) - {s.full_name for s in selected} - {s.group for s in selected}
        if missing:
            raise KeyError(f"unknown scenario/group: {sorted(missing)}")

    if jobs is None:
        jobs = min(len(selected), max(1, (os.cpu_count() or 1)))

    # 为每个场景准备独立的临时数据集副本，保证互不污染。
    # 只传候选名字：候选实例可能持有 DuckDB 连接，不能跨进程序列化。
    contexts = [
        (scenario, fixtures.get(scenario.dataset), store, candidate_name, baseline, minimize, minimize_attempts)
        for scenario in selected
    ]

    if jobs <= 1:
        outcomes = [_run_star(c) for c in contexts]
    else:
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(max_workers=jobs) as pool:
            outcomes = list(pool.map(_run_star, contexts))
    return RunReport(outcomes=sorted(outcomes, key=lambda o: o.scenario), candidate=candidate_name, baseline_path=str(getattr(baseline, "path", "") if baseline else ""))


def _run_star(ctx: tuple) -> ScenarioOutcome:
    scenario, dataset, store, candidate_name, baseline, minimize, attempts = ctx
    return run_scenario(
        scenario,
        dataset=dataset,
        store=store,
        candidate=get_candidate(candidate_name),
        baseline=baseline,
        minimize=minimize,
        minimize_attempts=attempts,
    )
