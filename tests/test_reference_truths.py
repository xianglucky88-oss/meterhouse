"""手工核算真值：锚定“定义 + oracle”这一层自身。

`fixtures/expected_truths.yaml` 的数字是逐行手算的，与 metrics.yaml、编译器、
oracle 都无关。oracle 也由定义派生，若定义写错（如 dedup 放错层级），
候选与参考会一起错、回归全绿。这组断言专门防这一类“共模错误”。
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from meterhouse.regression.fixtures import FixtureLibrary
from meterhouse.regression.models import Query
from meterhouse.regression.oracle import Oracle
from meterhouse.semantic import MetricStore

ROOT = Path(__file__).resolve().parent.parent
METRICS = ROOT / "metrics" / "metrics.yaml"
DATASETS = ROOT / "meterhouse" / "regression" / "fixtures" / "datasets.yaml"
TRUTHS = ROOT / "meterhouse" / "regression" / "fixtures" / "expected_truths.yaml"


@pytest.fixture(scope="module")
def store() -> MetricStore:
    return MetricStore.load(METRICS)


@pytest.fixture(scope="module")
def fixtures() -> FixtureLibrary:
    return FixtureLibrary.load(DATASETS)


@pytest.fixture(scope="module")
def truths() -> dict:
    return yaml.safe_load(TRUTHS.read_text(encoding="utf-8"))["datasets"]


def _normalize(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        item = {}
        for key, value in row.items():
            if hasattr(value, "isoformat"):
                value = value.isoformat()
            item[key] = value
        out.append(item)
    return out


def _expected_value(raw):
    if isinstance(raw, list):
        return [_normalize([r])[0] for r in raw]
    return raw


def _compare_list(query: Query, got: list[dict], expected: list[dict]) -> bool:
    """分组结果按“分组键 -> 值”比较，不依赖行序（行序由单独的不变量检查负责）。"""
    keys = list(query.dimensions) + (["date"] if query.granularity else [])

    def as_map(rows: list[dict]) -> dict:
        return {tuple(r.get(k) for k in keys): r.get(query.metric) for r in rows}

    got_map = as_map(got)
    exp_map = as_map(expected)
    if set(got_map) != set(exp_map):
        return False
    return all(_close(got_map[k], exp_map[k]) for k in exp_map)


@pytest.mark.parametrize("dataset_name", ["orders_core", "orders_empty", "suppliers_span", "ratio_skew"])
def test_hand_computed_truths(dataset_name, store, fixtures, truths):
    oracle = Oracle.from_store(store)
    dataset = fixtures.get(dataset_name)
    failures = []
    for item in truths[dataset_name]:
        query = Query.from_raw(item["query"])
        got = _normalize(oracle.evaluate(query, dataset))
        expected = _expected_value(item["value"])
        if isinstance(expected, list):
            ok = _compare_list(query, got, expected)
        else:
            ok = len(got) == 1 and _close(got[0].get(query.metric), expected)
        if not ok:
            failures.append({"query": item["query"], "expected": expected, "got": got})
    assert not failures, failures


def _close(got, expected) -> bool:
    if expected is None:
        return got is None
    if got is None:
        return False
    if isinstance(expected, float):
        return abs(float(got) - expected) <= 1e-9 * max(1.0, abs(expected))
    return got == expected


def test_gmv_dedup_keeps_max_amount(fixtures, store):
    """重复推送必须取金额最大的一条，而不是累加或取第一条。"""
    oracle = Oracle.from_store(store)
    rows = oracle.evaluate(Query(metric="gmv_cents"), fixtures.get("orders_core"))
    # 若不去重会得到 9200 + 500 + 1100 = 10800；若取第一条会少 200。
    assert rows[0]["gmv_cents"] == 9200


def test_misplaced_dedup_is_rejected(tmp_path: Path):
    """dedup 写在指标级而不是 measure 内时必须报错，不能静默忽略。"""
    raw = yaml.safe_load(METRICS.read_text(encoding="utf-8"))
    for metric in raw["metrics"]:
        if metric["name"] == "gmv_cents":
            metric["dedup"] = metric["measure"].pop("dedup")
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown field"):
        MetricStore.load(path)


def test_raw_expr_is_rejected(tmp_path: Path):
    raw = yaml.safe_load(METRICS.read_text(encoding="utf-8"))
    raw["metrics"][0]["expr"] = "count(*)"
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValueError):
        MetricStore.load(path)
