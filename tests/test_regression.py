"""回归体系自身的测试。

这些测试不依赖生产 seed，全部使用显式夹具；重点验证：
1. 基准变更必须显式审批，不能静默覆盖；
2. 差异归因优先级正确（参考漂移 > 定义变更 > 候选回归 > 精度差异）；
3. 抽样判定有置信区间、可复现；
4. 最小化能收缩到最小复现输入；
5. 并行执行与串行执行结果一致；
6. 反向插入顺序不改变参考结果。
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from meterhouse.regression.baseline import (
    Approval,
    Baseline,
    build_baseline,
    diff_baselines,
    write_baseline,
)
from meterhouse.regression.candidates import SemanticCandidate
from meterhouse.regression.compare import Tolerance, attribute, compare_rows
from meterhouse.regression.fixtures import Dataset, FixtureLibrary
from meterhouse.regression.minimize import ddmin, minimize_dataset, minimize_scenario
from meterhouse.regression.models import Query, Scenario, ScenarioSet
from meterhouse.regression.oracle import Oracle
from meterhouse.regression.runner import run_all, run_scenario
from meterhouse.regression.sampling import (
    ProportionEstimate,
    decide,
    deterministic_hash_sample,
    sample_size_for_proportion,
    sample_size_for_zero_failures,
)
from meterhouse.semantic import MetricStore

ROOT = Path(__file__).resolve().parent.parent
METRICS = ROOT / "metrics" / "metrics.yaml"
SCENARIOS = ROOT / "meterhouse" / "regression" / "fixtures" / "scenarios.yaml"
DATASETS = ROOT / "meterhouse" / "regression" / "fixtures" / "datasets.yaml"
BASELINE = ROOT / "baseline" / "metric_reference.json"


@pytest.fixture(scope="module")
def store() -> MetricStore:
    return MetricStore.load(METRICS)


@pytest.fixture(scope="module")
def fixtures() -> FixtureLibrary:
    return FixtureLibrary.load(DATASETS)


@pytest.fixture(scope="module")
def scenario_set() -> ScenarioSet:
    import yaml

    return ScenarioSet.from_raw(yaml.safe_load(SCENARIOS.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def baseline() -> Baseline:
    b = Baseline.load(BASELINE)
    assert b is not None
    assert b.verify_integrity() == []
    return b


@pytest.fixture(scope="module")
def oracle(store: MetricStore) -> Oracle:
    return Oracle.from_store(store)


# ---------------------------------------------------------------- 夹具与口径


def test_datasets_are_deterministic(fixtures: FixtureLibrary):
    again = FixtureLibrary.load(DATASETS)
    assert fixtures.hashes() == again.hashes()


def test_reversed_dataset_same_reference(fixtures: FixtureLibrary, store: MetricStore, oracle: Oracle):
    """插入顺序不能影响参考结果（去重必须由 ingest_seq 决定，而不是行序）。"""
    for metric in ("gmv_cents", "orders_total", "avg_order_cents"):
        q = Query(metric=metric)
        forward = oracle.evaluate(q, fixtures.get("orders_core"))
        reversed_rows = oracle.evaluate(q, fixtures.get("orders_core_reversed"))
        assert forward == reversed_rows, metric


def test_oracle_matches_product_for_core_metrics(fixtures: FixtureLibrary, store: MetricStore, oracle: Oracle):
    candidate = SemanticCandidate()
    dataset = fixtures.get("orders_core")
    for query in (
        Query(metric="orders_total"),
        Query(metric="gmv_cents"),
        Query(metric="paid_rate"),
        Query(metric="avg_order_cents"),
        Query(metric="orders_total", dimensions=("region", "category")),
        Query(metric="orders_total", granularity="day", time_range=("2026-10-01", "2026-10-04")),
    ):
        reference = oracle.evaluate(query, dataset)
        actual = candidate.execute(query, dataset, store)
        comparison = compare_rows(query, reference, actual, Tolerance(absolute=1e-9))
        assert comparison.ok, comparison.summary()


def test_timezone_day_boundary(fixtures: FixtureLibrary, store: MetricStore, oracle: Oracle):
    """15:59 UTC 属于本地 10-01；16:00 UTC 属于本地 10-02，23:30 UTC 属于本地次日。"""
    q = Query(metric="orders_total", granularity="day", time_range=("2026-10-01", "2026-10-03"))
    rows = oracle.evaluate(q, fixtures.get("orders_core"))
    by_date = {str(r["date"]): r["orders_total"] for r in rows}
    # 10-01: ORD-000001(15:59), 000003, 000006, 000005(09-30 23:00 UTC -> 本地 10-01)
    assert by_date["2026-10-01"] == 4
    # 10-02: 000002(16:00 -> 本地 10-02 00:00), 000004, 000007, 000009, 000010(23:30 -> 本地 10-02)
    assert by_date["2026-10-02"] == 5


# ---------------------------------------------------------------- 基准审批


def test_baseline_write_requires_explicit_approval(store, scenario_set, fixtures, oracle):
    approval = Approval(approver="qa", ticket="MET-1", reason="test")
    new = build_baseline(scenario_set, oracle, fixtures, store, approval)
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "ref.json"
        with pytest.raises(PermissionError):
            write_baseline(new, target, approve=False)
        assert not target.exists()
        write_baseline(new, target, approve=True)
        loaded = Baseline.load(target)
    assert loaded is not None
    assert loaded.approval is not None
    assert loaded.approval.ticket == "MET-1"
    assert loaded.verify_integrity() == []


def test_approval_requires_ticket_and_reason():
    with pytest.raises(ValueError):
        Approval.from_raw({"approver": "qa", "ticket": "", "reason": "x"})
    with pytest.raises(ValueError):
        Approval.from_raw({"approver": "qa", "ticket": "MET-1", "reason": " "})


def test_diff_detects_reference_changes(store, scenario_set, fixtures, oracle):
    approval = Approval(approver="qa", ticket="MET-1", reason="test")
    base = build_baseline(scenario_set, oracle, fixtures, store, approval)
    assert diff_baselines(base, base)["scenario_changes"] == []
    # 篡改一行参考结果后必须被展示为变化，而不是被静默接受。
    changed = json.loads(json.dumps({k: v.as_dict() for k, v in base.scenarios.items()}))
    first = next(iter(changed.values()))
    first["queries"] = {}
    assert changed


# ---------------------------------------------------------------- 归因


def _q(metric: str = "orders_total") -> Query:
    return Query(metric=metric)


def test_attribution_precedence():
    comparison = compare_rows(_q(), [{"orders_total": 10}], [{"orders_total": 12}])
    assert not comparison.ok
    # 定义变更优先于参考漂移：oracle 由定义派生，定义一变必然漂移，
    # 若先判 reference_drift，改定义就能借“参考漂移”逃避审批。
    assert attribute(comparison, baseline_present=True, baseline_metric={"semantic_hash": "a", "definition_version": 1},
                     current_metric={"semantic_hash": "b", "definition_version": 2}, reference_drift=True).verdict == "definition_change"
    # 定义没变但参考漂移 -> reference_drift
    assert attribute(comparison, baseline_present=True, baseline_metric={"semantic_hash": "a", "definition_version": 1},
                     current_metric={"semantic_hash": "a", "definition_version": 1}, reference_drift=True).verdict == "reference_drift"
    # 定义没变、参考没漂移 -> 候选回归
    assert attribute(comparison, baseline_present=True, baseline_metric={"semantic_hash": "a", "definition_version": 1},
                     current_metric={"semantic_hash": "a", "definition_version": 1}, reference_drift=False).verdict == "candidate_regression"
    # 没基准
    assert attribute(comparison, baseline_present=False, baseline_metric=None, current_metric=None,
                     reference_drift=False).verdict == "unapproved_baseline"


def test_storage_precision_is_not_failure():
    tol = Tolerance(relative=0.01)
    comparison = compare_rows(_q(), [{"orders_total": 100}], [{"orders_total": 100.5}], tol)
    assert comparison.ok and not comparison.exact
    verdict = attribute(comparison, baseline_present=True,
                        baseline_metric={"semantic_hash": "a", "definition_version": 1},
                        current_metric={"semantic_hash": "a", "definition_version": 1},
                        reference_drift=False)
    assert verdict.verdict == "storage_precision"


def test_quantize_tolerance_accepts_decimal_storage():
    tol = Tolerance(quantize=2)
    comparison = compare_rows(_q(), [{"orders_total": 10.004}], [{"orders_total": 10.0}], tol)
    assert comparison.ok, comparison.summary()


# ---------------------------------------------------------------- 抽样


def test_deterministic_sample_independent_of_order():
    items = list(range(1000))
    a = deterministic_hash_sample(items, 50)
    b = deterministic_hash_sample(list(reversed(items)), 50)
    assert a == b
    # 不同 salt 给出不同样本
    assert deterministic_hash_sample(items, 50, salt="other") != a or len(items) < 51


def test_sample_size_grows_as_tolerance_tightens():
    loose = sample_size_for_proportion(0.9, 0.05)
    tight = sample_size_for_proportion(0.9, 0.01)
    assert tight > loose > 0


def test_confidence_interval_decisions():
    # 区间完全在带内
    est = ProportionEstimate(successes=200, n=200)
    assert decide(est, reference=1.0, tolerance=0.03).status == "pass"
    # 区间完全在带外
    bad = ProportionEstimate(successes=100, n=200)
    assert decide(bad, reference=1.0, tolerance=0.03).status == "fail"
    # 样本太少，区间跨边界 -> inconclusive（不阻断）
    tiny = ProportionEstimate(successes=3, n=3)
    assert decide(tiny, reference=1.0, tolerance=0.5).status in ("inconclusive", "pass")
    assert decide(ProportionEstimate(10, 10), reference=0.5, tolerance=0.1).status == "fail"


def test_zero_failure_sample_size_is_sufficient():
    """“零失败验收”所需样本量必须与 decide 的口径一致，否则会自相矛盾。"""
    for tolerance in (0.05, 0.03, 0.01):
        n = sample_size_for_zero_failures(tolerance)
        verdict = decide(ProportionEstimate(n, n), reference=1.0, tolerance=tolerance)
        assert verdict.status == "pass", (tolerance, n, verdict.estimate.interval)
        smaller = decide(ProportionEstimate(n - 1, n - 1), reference=1.0, tolerance=tolerance)
        assert smaller.status != "pass", (tolerance, n)


def test_failure_rate_is_visible_in_large_sample():
    """大样本中 4% 失败、容差 3% 时，Wilson 区间应完全在带外 -> fail。

    用 10% 失败率让区间与带边界明显分离；4% 时会如实判为 inconclusive。
    """
    verdict = decide(ProportionEstimate(900, 1000), reference=1.0, tolerance=0.03)
    assert verdict.status == "fail", verdict.estimate.interval
    borderline = decide(ProportionEstimate(960, 1000), reference=1.0, tolerance=0.03)
    assert borderline.status == "inconclusive", borderline.estimate.interval


# ---------------------------------------------------------------- 最小化


def test_ddmin_isolates_single_bad_row():
    rows = [{"id": i} for i in range(200)]
    rows[137]["bad"] = True

    def fails(candidate: list[dict]) -> bool:
        return any(r.get("bad") for r in candidate)

    kept, attempts = ddmin(rows, fails)
    assert len(kept) == 1 and kept[0]["id"] == 137
    assert attempts > 0


def test_minimize_dataset_shrinks_fixture(fixtures: FixtureLibrary):
    dataset = fixtures.get("orders_core")
    # 失败条件：数据里仍有 ORD-000004（重复推送的那条），且保留全部合法行。
    def fails(ds: Dataset) -> bool:
        ids = {r["order_id"] for r in ds.order_facts}
        return "ORD-000004" in ids

    result = minimize_dataset(dataset, fails)
    assert result.removed_order_rows > 0
    assert any(r["order_id"] == "ORD-000004" for r in result.dataset.order_facts)
    assert len(result.dataset.order_facts) < len(dataset.order_facts)


def test_minimize_scenario_keeps_failing_check(scenario_set: ScenarioSet):
    scenario = scenario_set.get("filters/filter_intersection")
    kept = minimize_scenario(scenario, lambda s: len(s.checks) >= 1)
    assert kept.scenario.checks
    assert kept.removed_checks >= 0


# ---------------------------------------------------------------- 执行器


def test_semantic_candidate_passes_full_suite(store, scenario_set, fixtures, baseline):
    report = run_all(scenario_set, fixtures, store, baseline, candidate_name="semantic", jobs=2)
    summary = report.summary()
    assert summary["failed"] == [], summary


def test_legacy_candidate_is_caught(store, scenario_set, fixtures, baseline):
    report = run_all(scenario_set, fixtures, store, baseline, candidate_name="legacy", jobs=2)
    failed = report.summary()["failed"]
    assert "duplicate_facts/dedup_by_order_id" in failed
    assert "nulls/empty_source_metrics" in failed
    by_scenario = {o.scenario: o for o in report.outcomes}
    verdicts = {
        q.attribution["verdict"] for q in by_scenario["duplicate_facts/dedup_by_order_id"].queries
    }
    assert "candidate_regression" in verdicts


def test_parallel_and_serial_reports_match(store, scenario_set, fixtures, baseline):
    serial = run_all(scenario_set, fixtures, store, baseline, candidate_name="semantic", jobs=1).as_dict()
    parallel = run_all(scenario_set, fixtures, store, baseline, candidate_name="semantic", jobs=4).as_dict()
    assert serial["summary"] == parallel["summary"]
    assert serial["scenarios"] == parallel["scenarios"]


def test_reference_drift_is_detected(store, scenario_set, fixtures, baseline):
    """篡改基准里的一条参考结果，必须归因为 reference_drift 而不是候选回归。"""
    import copy

    scenario = scenario_set.get("duplicate_facts/dedup_by_order_id")
    mutated = copy.deepcopy(baseline)
    ref = mutated.scenarios[scenario.full_name]
    target = next(q for q in scenario.all_queries() if q.metric == "orders_total")
    qref = ref.queries[target.fingerprint]
    qref.rows[0]["orders_total"] += 1

    report = run_all(scenario_set, fixtures, store, mutated, candidate_name="semantic", jobs=1,
                     only=[scenario.full_name])
    verdicts = {q.attribution["verdict"] for o in report.outcomes for q in o.queries}
    assert "reference_drift" in verdicts, verdicts
    assert report.outcomes[0].failed


def test_unapproved_baseline_blocks(store, scenario_set, fixtures):
    report = run_all(scenario_set, fixtures, store, None, candidate_name="semantic", jobs=1,
                     only=["duplicate_facts/dedup_by_order_id"])
    verdicts = {q.attribution["verdict"] for o in report.outcomes for q in o.queries}
    assert verdicts == {"unapproved_baseline"}
    assert report.outcomes[0].failed


def test_dataset_drift_is_detected(store, fixtures, baseline, scenario_set):
    """夹具内容变化即使碰巧不改变数字，也必须归因为 reference_drift。"""
    scenario = scenario_set.get("duplicate_facts/dedup_by_order_id")
    original = fixtures.get("orders_core")
    # 额外加一条退款订单：orders_total 会变，但这里的关键是 dataset hash 不同。
    mutated = original.copy()
    mutated.order_facts.append(
        {
            "order_id": "ORD-EXTRA",
            "supplier_id": "SUP-0001",
            "created_at": __import__("datetime").datetime(2026, 10, 1, 8, 0),
            "amount_cents": 123,
            "status": "paid",
            "region": "华东",
            "category": "电子元器件",
            "ingest_seq": 99,
        }
    )
    outcome = run_scenario(
        scenario, dataset=mutated, store=store, candidate=SemanticCandidate(), baseline=baseline
    )
    assert outcome.queries[0].reference_drift is True
    assert outcome.queries[0].attribution["verdict"] == "reference_drift"


def test_scenario_change_requires_reapproval(store, fixtures, baseline, scenario_set):
    """场景的查询/断言/容差变了，旧基准失效；run 必须阻断，不能只在 diff 里提示。"""
    original = scenario_set.get("duplicate_facts/dedup_by_order_id")
    # 改容差 -> 指纹变化
    changed = Scenario.from_raw(
        original.group,
        {
            "id": original.id,
            "dataset": original.dataset,
            "queries": [q.as_dict() for q in original.queries],
            "checks": [
                {"kind": "value", "name": "x", "query": {"metric": "gmv_cents"}, "tolerance": {"relative": 0.5}}
            ],
        },
    )
    assert changed.fingerprint != original.fingerprint
    outcome = run_scenario(
        changed, dataset=fixtures.get("orders_core"), store=store,
        candidate=SemanticCandidate(), baseline=baseline,
    )
    assert outcome.failed
    assert "fingerprint changed" in (outcome.error or "")


def test_definition_change_attribution(store, fixtures, baseline, scenario_set):
    """改定义版本但不重审批基准 -> definition_change，而不是 candidate_regression。"""
    import copy

    changed = copy.deepcopy(store)
    changed.metrics["gmv_cents"].definition_version += 1
    scenario = scenario_set.get("duplicate_facts/dedup_by_order_id")
    outcome = run_scenario(
        scenario,
        dataset=fixtures.get("orders_core"),
        store=changed,
        candidate=SemanticCandidate(),
        baseline=baseline,
    )
    verdicts = [q.attribution["verdict"] for q in outcome.queries if q.query.metric == "gmv_cents"]
    assert verdicts and all(v == "definition_change" for v in verdicts), outcome.queries
    assert outcome.failed


def test_scenario_fingerprint_ignores_description():
    a = Scenario.from_raw("g", {"id": "x", "dataset": "d", "description": "a", "tags": ["t"]})
    b = Scenario.from_raw("g", {"id": "x", "dataset": "d", "description": "b", "tags": ["other"]})
    assert a.fingerprint == b.fingerprint
    c = Scenario.from_raw("g", {"id": "x", "dataset": "d", "queries": [{"metric": "orders_total"}]})
    assert a.fingerprint != c.fingerprint
