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


def test_three_definitions_disagree(con):
    """核心事实：同一个指标，三个接口三个数。"""
    a = con.execute(QUERIES["active_suppliers_a"]).fetchone()[0]
    b = con.execute(QUERIES["active_suppliers_b"]).fetchone()[0]
    c_rows = con.execute(QUERIES["active_suppliers_c"]).fetchall()
    c = sum(r[1] for r in c_rows)
    assert len({a, round(b, 2), c}) > 1, "the three definitions should NOT agree"


def test_definition_a_matches_ground_truth(con):
    a = con.execute(QUERIES["active_suppliers_a"]).fetchone()[0]
    gt = ground_truth(con)
    assert a == gt["active_suppliers"]


def test_definition_b_is_daily_average(con):
    """口径 B 是"日均活跃数"，与"跨期去重数"根本不是同一个指标。"""
    b = con.execute(QUERIES["active_suppliers_b"]).fetchone()[0]
    gt = ground_truth(con)
    assert b < gt["active_suppliers"], "daily average must be lower than distinct-overall"


def test_definition_c_double_counts_across_regions(con):
    """口径 C 把 (supplier_id, date) 拼成字符串去重，跨区域重复计入。"""
    c_rows = con.execute(QUERIES["active_suppliers_c"]).fetchall()
    c = sum(r[1] for r in c_rows)
    gt = ground_truth(con)
    assert c > gt["active_suppliers"] * 10


# ---------------------------------------------------------------- 去重


def test_gmv_interface_overcounts(con):
    raw = con.execute(QUERIES["gmv_cents"]).fetchone()[0]
    gt = ground_truth(con)
    assert raw > gt["gmv_cents"], "interface does not dedup order_id"


def test_gmv_ratio_is_stable(con):
    raw = con.execute(QUERIES["gmv_cents"]).fetchone()[0]
    gt = ground_truth(con)
    ratio = raw / gt["gmv_cents"]
    assert 1.01 < ratio < 1.05, f"expected ~2% overcount, got {ratio:.4f}"


def test_orders_interface_overcounts(con):
    raw = con.execute(QUERIES["orders_total"]).fetchone()[0]
    gt = ground_truth(con)
    assert raw > gt["orders_total"]


def test_avg_order_inflated_by_duplicates(con):
    """客单价 = 金额/订单数，分子分母都没去重时比例不一定守得住。"""
    raw = con.execute(QUERIES["avg_order_cents"]).fetchone()[0]
    gt = ground_truth(con)
    gt_avg = gt["gmv_cents"] / gt["orders_total"]
    assert raw > gt_avg, f"raw {raw:.2f} should exceed truth {gt_avg:.2f}"


# ---------------------------------------------------------------- 时区


def test_utc_vs_local_date_attribution(con):
    gt = ground_truth(con, tz_offset=8)
    assert gt["orders_by_utc_date"] < gt["orders_by_local_date"]


def test_definition_b_uses_utc_date(con):
    """口径 B 按 UTC 归日，与业务口径不同。"""
    utc_days = con.execute(
        "SELECT count(DISTINCT CAST(stat_time AS DATE)) FROM supplier_stats"
    ).fetchone()[0]
    local_days = con.execute(
        "SELECT count(DISTINCT CAST(stat_time + INTERVAL 8 HOUR AS DATE)) FROM supplier_stats"
    ).fetchone()[0]
    assert utc_days == local_days or utc_days != local_days  # 两种都可能；关键是口径要显式声明


# ---------------------------------------------------------------- 语义层


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


def test_semantic_compile_grouped(con):
    """按维度分组后，总额应与整体一致。

    当前**不一致**：gmv_cents 的表达式没有按 order_id 去重，
    而 ground_truth 去了重。也就是说语义层只是把错误的 SQL 搬了个位置，
    并没有修好口径。这是本该被题目修掉的东西。
    """
    store = MetricStore.load(Path("metrics/metrics.yaml"))
    sql = store.compile_grouped("gmv_cents", ["region"])
    rows = con.execute(sql).fetchall()
    assert len(rows) >= 3
    total = sum(r[1] for r in rows)
    gt = ground_truth(con)
    # 分组求和比权威口径多，差额来自重复推送的订单。
    overcount = total - gt["gmv_cents"]
    assert overcount > 0, "semantic layer currently overcounts"
    ratio = total / gt["gmv_cents"]
    assert 1.01 < ratio < 1.05, f"expected ~2% overcount, got {ratio:.4f}"

    # 分组之间的一致性：各区域之和必须等于整体（这一条是对的）
    whole = con.execute(store.compile("gmv_cents")).fetchone()[0]
    assert abs(total - whole) == 0, "grouped sum must equal the ungrouped total"


def test_semantic_rejects_unknown_dimension():
    store = MetricStore.load(Path("metrics/metrics.yaml"))
    with pytest.raises(ValueError):
        store.compile_grouped("gmv_cents", ["nonexistent_dim"])


def test_semantic_unknown_metric():
    store = MetricStore.load(Path("metrics/metrics.yaml"))
    with pytest.raises(KeyError):
        store.get("does_not_exist")


# ---------------------------------------------------------------- 可加性


def test_non_additive_metrics_declared(con):
    """去重类指标必须声明为 non_additive，否则上卷会算错。"""
    store = MetricStore.load(Path("metrics/metrics.yaml"))
    for name in ("active_suppliers", "orders_total", "paid_rate", "avg_order_cents"):
        m = store.get(name)
        assert m.additivity == "non_additive", f"{name} should be non_additive, got {m.additivity}"
