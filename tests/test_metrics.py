"""指标口径测试。每个用例都直接对照 ground_truth。"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from meterhouse.data import connect, ground_truth, seed
from meterhouse.queries import QUERIES
from meterhouse.semantic import MetricStore


@pytest.fixture(scope="module")
def con():
    path = Path(tempfile.mkdtemp(prefix="meterhouse-")) / "test.duckdb"
    c = connect(path)
    seed(c)
    yield c
    c.close()


# ---------------------------------------------------------------- 数据形态


def test_seed_is_deterministic(con):
    before = con.execute("SELECT count(*), sum(amount_cents) FROM order_facts").fetchone()
    seed(con)
    after = con.execute("SELECT count(*), sum(amount_cents) FROM order_facts").fetchone()
    assert before == after


def test_orders_have_duplicates(con):
    rows = con.execute("SELECT count(*) FROM order_facts").fetchone()[0]
    distinct = con.execute("SELECT count(DISTINCT order_id) FROM order_facts").fetchone()[0]
    assert rows > distinct, "generator should create duplicate order rows"


def test_night_orders_exist(con):
    n = con.execute(
        "SELECT count(*) FROM order_facts WHERE CAST(strftime(created_at, '%H') AS INT) >= 16"
    ).fetchone()[0]
    assert n > 100


# ---------------------------------------------------------------- 三份口径

def test_semantic_store_loads_six_metrics():
    store = MetricStore.load(Path("metrics/metrics.yaml"))
    assert len(store.names()) == 6
    assert "active_suppliers" in store.names()


def test_semantic_compile_simple(con):
    store = MetricStore.load(Path("metrics/metrics.yaml"))
    sql = store.compile("orders_total")
    got = con.execute(sql).fetchone()[0]
    gt = ground_truth(con)
    assert got == gt["orders_total"], "semantic layer should dedup correctly"

def test_semantic_rejects_unknown_dimension():
    store = MetricStore.load(Path("metrics/metrics.yaml"))
    with pytest.raises(ValueError):
        store.compile_grouped("gmv_cents", ["nonexistent_dim"])


def test_semantic_unknown_metric():
    store = MetricStore.load(Path("metrics/metrics.yaml"))
    with pytest.raises(KeyError):
        store.get("does_not_exist")


# ---------------------------------------------------------------- 可加性
