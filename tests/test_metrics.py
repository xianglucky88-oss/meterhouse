"""指标口径与执行层测试。

分三组：

1. 数据形态与权威口径（不依赖外部引擎）
2. 语义层编译：两种方言 + 分组/时间窗口/粒度，全部对照 ground_truth
3. 切流、回退、故障隔离（用假引擎，不依赖真实 ClickHouse）
4. ClickHouse 端到端（设置了 CLICKHOUSE_HOST 才跑）
"""

from __future__ import annotations

import os
import tempfile
from datetime import date
from pathlib import Path

import pytest

from meterhouse.data import connect, ground_truth, seed
from meterhouse.engine import CapacityPolicy, DuckDBEngine, ResultSet
from meterhouse.errors import (
    DefinitionError,
    EngineUnavailable,
    ServiceUnavailable,
    UnknownMetric,
)
from meterhouse.routing import CutoverStore, RouteConfig, Router, compare_result_sets
from meterhouse.semantic import MetricStore, TimeWindow

METRICS = Path("metrics/metrics.yaml")


@pytest.fixture(scope="module")
def con():
    path = Path(tempfile.mkdtemp(prefix="meterhouse-")) / "test.duckdb"
    c = connect(str(path))
    seed(c)
    yield c
    c.close()


@pytest.fixture(scope="module")
def store():
    return MetricStore.load(METRICS)


# ---------------------------------------------------------------------------
# 1. 数据形态 + 权威口径
# ---------------------------------------------------------------------------


def test_seed_is_deterministic(con):
    before = con.execute("SELECT count(*), sum(amount_cents) FROM order_facts").fetchone()
    seed(con)
    after = con.execute("SELECT count(*), sum(amount_cents) FROM order_facts").fetchone()
    assert before == after


def test_orders_have_duplicates(con):
    rows, distinct = con.execute(
        "SELECT count(*), count(DISTINCT order_id) FROM order_facts"
    ).fetchone()
    assert rows > distinct, "generator should create duplicate order rows"


def test_duplicates_have_ordered_versions(con):
    """同主键的多行必须有可比较的版本号，否则"取最新一行"不可复现。"""
    bad = con.execute(
        """SELECT count(*) FROM (
             SELECT order_id, count(*) c, count(DISTINCT row_version) v
             FROM order_facts GROUP BY 1 HAVING c > 1 AND c != v)"""
    ).fetchone()[0]
    assert bad == 0


def test_ingest_ledger_records_batches(con):
    rows = con.execute(
        "SELECT table_name, rows_loaded FROM ingest_batches ORDER BY table_name"
    ).fetchall()
    assert dict(rows)["order_facts"] == con.execute(
        "SELECT count(*) FROM order_facts"
    ).fetchone()[0]


def test_ground_truth_internal_consistency(con):
    gt = ground_truth(con)
    assert gt["gmv_cents"] > 0
    assert gt["paid_orders"] <= gt["orders_total"]
    assert 0 < gt["paid_rate"] < 1
    assert gt["avg_order_cents"] == pytest.approx(
        gt["gmv_cents"] / gt["paid_orders"], rel=1e-6
    )


def test_timezone_attribution_differs(con):
    gt = ground_truth(con, tz_offset=8)
    assert gt["orders_by_local_date"] != gt["orders_by_utc_date"]


# ---------------------------------------------------------------------------
# 2. 语义层编译
# ---------------------------------------------------------------------------


def test_store_loads_all_metrics(store):
    assert set(store.names()) == {
        "active_suppliers",
        "active_suppliers_daily_avg",
        "avg_order_cents",
        "gmv_cents",
        "orders_total",
        "paid_rate",
        "suppliers_total",
    }


@pytest.mark.parametrize(
    "name",
    [
        "active_suppliers",
        "active_suppliers_daily_avg",
        "avg_order_cents",
        "gmv_cents",
        "orders_total",
        "paid_rate",
        "suppliers_total",
    ],
)
def test_metric_matches_ground_truth(con, store, name):
    compiled = store.compile_query(name, engine="duckdb")
    got = con.execute(compiled.sql).fetchone()[0]
    expected = ground_truth(con)[name]
    assert got == pytest.approx(expected, rel=1e-6)


def test_dedup_happens_at_query_time_not_preaggregation(con, store):
    """gmv 必须按 order_id 去重；原始重复行仍保留在事实表里。"""
    raw = con.execute("SELECT sum(amount_cents) FROM order_facts WHERE status='paid'").fetchone()[0]
    deduped = con.execute(store.compile_query("gmv_cents", engine="duckdb").sql).fetchone()[0]
    assert deduped < raw, "semantic layer should dedup duplicate pushes"
    assert con.execute("SELECT count(*) FROM order_facts").fetchone()[0] > con.execute(
        "SELECT count(DISTINCT order_id) FROM order_facts"
    ).fetchone()[0]


def test_additive_metric_grouped_sum_matches_whole(con, store):
    whole = con.execute(store.compile_query("gmv_cents", engine="duckdb").sql).fetchone()[0]
    grouped = store.compile_query("gmv_cents", engine="duckdb", dimensions=["region"])
    total = sum(r[-1] for r in con.execute(grouped.sql).fetchall())
    assert total == whole


def test_non_additive_does_not_double_count_across_dimensions(con, store):
    """去重指标跨维度分组后，加总不应超过整体（不是简单相加关系）。"""
    whole = con.execute(
        store.compile_query("active_suppliers", engine="duckdb").sql
    ).fetchone()[0]
    grouped = store.compile_query(
        "active_suppliers", engine="duckdb", dimensions=["region", "category"]
    )
    rows = con.execute(grouped.sql).fetchall()
    assert sum(r[-1] for r in rows) == whole  # 供应商只属于一个 (region, category)


def test_time_window_is_applied_before_dedup(con, store):
    window = TimeWindow(start=date(2026, 10, 1), end=date(2026, 10, 31))
    compiled = store.compile_query("gmv_cents", engine="duckdb", time_window=window)
    assert "biz_day" in compiled.sql, "窗口过滤应下推到业务日列以命中分区"
    windowed = con.execute(compiled.sql).fetchone()[0]
    whole = con.execute(store.compile_query("gmv_cents", engine="duckdb").sql).fetchone()[0]
    assert 0 < windowed < whole


def test_granularity_series_sums_to_total(con, store):
    compiled = store.compile_query(
        "orders_total", engine="duckdb", time_window=TimeWindow(granularity="month")
    )
    rows = con.execute(compiled.sql).fetchall()
    assert len(rows) == 2
    assert sum(r[-1] for r in rows) == ground_truth(con)["orders_total"]


def test_utc_basis_differs_from_business_basis(con, store):
    biz = store.compile_query(
        "orders_total",
        engine="duckdb",
        time_window=TimeWindow(granularity="day", basis="business"),
    )
    utc = store.compile_query(
        "orders_total",
        engine="duckdb",
        time_window=TimeWindow(granularity="day", basis="utc"),
    )
    biz_rows = dict(con.execute(biz.sql).fetchall())
    utc_rows = dict(con.execute(utc.sql).fetchall())
    assert any(biz_rows[k] != utc_rows.get(k, 0) for k in biz_rows), "时区基准应改变归日"


def test_unknown_dimension_rejected(store):
    with pytest.raises(DefinitionError):
        store.compile_query("gmv_cents", engine="duckdb", dimensions=["nonexistent_dim"])


def test_unknown_metric_rejected(store):
    with pytest.raises(UnknownMetric):
        store.get("does_not_exist")
    # 旧契约：KeyError
    with pytest.raises(KeyError):
        store.get("does_not_exist")


def test_unsupported_granularity_rejected(store):
    with pytest.raises(DefinitionError):
        store.compile_query(
            "orders_total", engine="duckdb", time_window=TimeWindow(granularity="hour")
        )


def test_non_additive_metrics_declared(store):
    for name in (
        "active_suppliers",
        "active_suppliers_daily_avg",
        "orders_total",
        "paid_rate",
        "avg_order_cents",
        "suppliers_total",
    ):
        assert store.get(name).additivity == "non_additive", name


def test_sql_injection_in_dimension_rejected(store):
    with pytest.raises(DefinitionError):
        store.compile_query(
            "gmv_cents", engine="duckdb", dimensions=["region; DROP TABLE order_facts"]
        )


# ---------------------------------------------------------------------------
# 3. 切流 / 回退 / 故障隔离（假引擎）
# ---------------------------------------------------------------------------


class FakeEngine:
    """可控引擎：按需返回结果或抛异常。"""

    def __init__(self, name: str, *, fail: bool = False, value: float = 1.0):
        self.name = name
        self.fail = fail
        self.value = value
        self.calls = 0

    def query(self, sql, *, params=None, interface=None, max_rows=None):
        self.calls += 1
        if self.fail:
            raise EngineUnavailable("down", engine=self.name, interface=interface)
        cols = ["v"]
        return ResultSet(
            columns=cols, rows=[(self.value,)], engine=self.name, elapsed_ms=0.1, row_count=1
        )

    def status(self):
        return {"engine": self.name, "fail": self.fail, "calls": self.calls}


class FakeBuilder:
    """build_sql：返回带引擎名的假编译结果。"""

    def __init__(self):
        from meterhouse.semantic import CompiledQuery

        self._cq = CompiledQuery(
            engine="x",
            metric="m",
            sql="SELECT 1",
            value_column="v",
            dimension_columns=[],
            time_column_alias=None,
            value_type="float",
            empty_value=0,
            description="",
            definition_version="t",
        )

    def __call__(self, engine_name):
        from dataclasses import replace

        return replace(self._cq, engine=engine_name)


def _cutover(tmp_path, *, mode="off", fallback=True):
    store = CutoverStore(tmp_path / "routes.yaml", ["iface"])
    store.update("iface", mode=mode, fallback=fallback, actor="test")
    return store


def test_off_mode_uses_control_only(tmp_path):
    duck = FakeEngine("duckdb", value=10)
    ch = FakeEngine("clickhouse", value=20)
    router = Router({"duckdb": duck, "clickhouse": ch}, _cutover(tmp_path, mode="off"))
    result = router.execute("iface", FakeBuilder())
    assert result.engine == "duckdb"
    assert result.scalar() == 10
    assert ch.calls == 0


def test_shadow_mode_serves_control_and_compares(tmp_path):
    duck = FakeEngine("duckdb", value=10)
    ch = FakeEngine("clickhouse", value=10)
    router = Router({"duckdb": duck, "clickhouse": ch}, _cutover(tmp_path, mode="shadow"))
    result = router.execute("iface", FakeBuilder())
    assert result.engine == "duckdb", "shadow 不应改变对外数字来源"
    assert ch.calls == 1, "shadow 必须调用目标引擎比对"
    assert router.status()["comparison_counts"]["match"] == 1


def test_shadow_detects_divergence(tmp_path):
    duck = FakeEngine("duckdb", value=10)
    ch = FakeEngine("clickhouse", value=12)
    router = Router({"duckdb": duck, "clickhouse": ch}, _cutover(tmp_path, mode="shadow"))
    router.execute("iface", FakeBuilder())
    comparisons = router.status()["recent_comparisons"]
    assert comparisons[0]["status"] == "mismatch"
    assert comparisons[0]["detail"]["max_relative_delta"] == pytest.approx(2 / 12)


def test_primary_mode_uses_target(tmp_path):
    duck = FakeEngine("duckdb", value=10)
    ch = FakeEngine("clickhouse", value=20)
    router = Router({"duckdb": duck, "clickhouse": ch}, _cutover(tmp_path, mode="primary"))
    result = router.execute("iface", FakeBuilder())
    assert result.engine == "clickhouse"
    assert result.degraded is False


def test_primary_failure_falls_back_and_marks_degraded(tmp_path):
    duck = FakeEngine("duckdb", value=10)
    ch = FakeEngine("clickhouse", fail=True)
    router = Router({"duckdb": duck, "clickhouse": ch}, _cutover(tmp_path, mode="primary"))
    result = router.execute("iface", FakeBuilder())
    assert result.engine == "duckdb"
    assert result.degraded is True
    assert result.fallback_from == "clickhouse"
    assert result.failure_reason


def test_primary_failure_without_fallback_raises_not_zero(tmp_path):
    duck = FakeEngine("duckdb", value=10)
    ch = FakeEngine("clickhouse", fail=True)
    store = _cutover(tmp_path, mode="primary", fallback=False)
    router = Router({"duckdb": duck, "clickhouse": ch}, store)
    with pytest.raises(ServiceUnavailable) as excinfo:
        router.execute("iface", FakeBuilder())
    assert excinfo.value.causes and excinfo.value.causes[0].engine == "clickhouse"


def test_control_failure_is_not_masked_as_success(tmp_path):
    duck = FakeEngine("duckdb", fail=True)
    ch = FakeEngine("clickhouse", value=20)
    router = Router({"duckdb": duck, "clickhouse": ch}, _cutover(tmp_path, mode="off"))
    with pytest.raises(ServiceUnavailable):
        router.execute("iface", FakeBuilder())


def test_failure_is_isolated_per_interface(tmp_path):
    """一个接口的引擎故障不应影响其它接口。"""
    duck = FakeEngine("duckdb", value=10)
    ch = FakeEngine("clickhouse", fail=True)
    store = CutoverStore(tmp_path / "routes.yaml", ["a", "b"])
    store.update("a", mode="primary", actor="t")
    router = Router({"duckdb": duck, "clickhouse": ch}, store)
    with pytest.raises(ServiceUnavailable):
        # a 无回退、故障
        store.update("a", fallback=False, actor="t")
        router.execute("a", FakeBuilder())
    result = router.execute("b", FakeBuilder())
    assert result.engine == "duckdb" and not result.degraded


def test_cutover_update_is_audited_and_persisted(tmp_path):
    audit = tmp_path / "routes.audit.jsonl"
    store = CutoverStore(tmp_path / "routes.yaml", ["iface"], audit_path=audit)
    store.update("iface", mode="shadow", actor="alice", reason="验证")
    assert (tmp_path / "routes.yaml").exists()
    lines = audit.read_text().strip().splitlines()
    assert len(lines) == 1
    import json

    record = json.loads(lines[0])
    assert record["actor"] == "alice" and record["after"]["mode"] == "shadow"


def test_cutover_rejects_unknown_interface(tmp_path):
    store = CutoverStore(tmp_path / "routes.yaml", ["iface"])
    with pytest.raises(DefinitionError):
        store.update("nope", mode="primary")


def test_cutover_target_and_control_must_differ(tmp_path):
    store = CutoverStore(tmp_path / "routes.yaml", ["iface"])
    with pytest.raises(DefinitionError):
        store.update("iface", target="duckdb", control="duckdb")


def test_yaml_off_is_not_boolean(tmp_path):
    """YAML 1.1 会把 off 解析成 False；必须还原成 'off'，不能静默切流。"""
    path = tmp_path / "routes.yaml"
    path.write_text("interfaces:\n- interface: iface\n  mode: off\n", encoding="utf-8")
    store = CutoverStore(path, ["iface"])
    assert store.route("iface").mode == "off"


def test_compare_result_sets_flags_row_count_mismatch():
    from meterhouse.semantic import CompiledQuery

    cq = CompiledQuery(
        engine="x", metric="m", sql="", value_column="v", dimension_columns=["region"],
        time_column_alias=None, value_type="float", empty_value=0, description="",
        definition_version="t",
    )
    left = ResultSet(columns=["region", "v"], rows=[("a", 1.0)], engine="l", elapsed_ms=0, row_count=1)
    right = ResultSet(columns=["region", "v"], rows=[("a", 1.0), ("b", 2.0)], engine="r", elapsed_ms=0, row_count=2)
    left.compiled = cq
    right.compiled = cq
    diff = compare_result_sets(left, right)
    assert diff["within_tolerance"] is False
    assert diff["reason"] == "row_count_mismatch"


# ---------------------------------------------------------------------------
# 4. ClickHouse 端到端（需要真实引擎）
# ---------------------------------------------------------------------------

CH_ENABLED = bool(os.environ.get("CLICKHOUSE_HOST"))

pytestmark_ch = pytest.mark.skipif(
    not CH_ENABLED, reason="set CLICKHOUSE_HOST to run ClickHouse end-to-end tests"
)


@pytest.fixture(scope="module")
def ch_engine():
    from meterhouse.engine import ClickHouseEngine

    engine = ClickHouseEngine(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        database=os.environ.get("CLICKHOUSE_DATABASE", "meterhouse"),
    )
    return engine


@pytest.fixture(scope="module")
def loaded(con, ch_engine):
    from meterhouse.ingest import ClickHouseLoader

    loader = ClickHouseLoader(ch_engine)
    loader.ensure_schema()
    loader.sync_all(con, truncate=True)
    return loader


@pytestmark_ch
def test_ingest_parity(con, loaded):
    report = loaded.verify(con)
    assert report["ok"], report["mismatches"]


@pytestmark_ch
@pytest.mark.parametrize("name", [
    "active_suppliers", "active_suppliers_daily_avg", "avg_order_cents",
    "gmv_cents", "orders_total", "paid_rate", "suppliers_total",
])
def test_clickhouse_matches_ground_truth(con, store, ch_engine, loaded, name):
    compiled = store.compile_query(name, engine="clickhouse")
    got = ch_engine.query(compiled.sql, interface="test").scalar()
    assert got == pytest.approx(ground_truth(con)[name], rel=1e-6)


@pytestmark_ch
@pytest.mark.parametrize("dims,window", [
    (["region"], None),
    ([], TimeWindow(start=date(2026, 10, 1), end=date(2026, 10, 31))),
    (["region"], TimeWindow(start=date(2026, 10, 1), end=date(2026, 10, 31), granularity="month")),
])
def test_clickhouse_matches_duckdb(con, store, ch_engine, loaded, dims, window):
    for name in ("gmv_cents", "orders_total", "active_suppliers"):
        qd = store.compile_query(name, engine="duckdb", dimensions=dims, time_window=window)
        qc = store.compile_query(name, engine="clickhouse", dimensions=dims, time_window=window)
        left = con.execute(qd.sql).fetchall()
        right = ch_engine.query(qc.sql, interface="test").rows
        assert [tuple(str(x) for x in r) for r in left] == [
            tuple(str(x) for x in r) for r in right
        ]


# ---------------------------------------------------------------------------
# 5. 老库迁移（原始 schema → 新布局）
# ---------------------------------------------------------------------------


def test_legacy_schema_is_migrated_in_place(tmp_path):
    """原仓库的 schema 没有 ingest_batch_id / row_version / biz_day。

    迁移必须做到：
    * 每一行原样保留（重复推送的两行都在，只是获得不同 row_version）；
    * 补齐可追溯列与业务日列；
    * 迁移后口径正确（去重后订单数 = 2，不是 3）。
    """
    import duckdb

    path = tmp_path / "legacy.duckdb"
    con = duckdb.connect(str(path))
    con.execute(
        """CREATE TABLE order_facts (order_id VARCHAR NOT NULL, supplier_id VARCHAR NOT NULL,
           created_at TIMESTAMP NOT NULL, amount_cents BIGINT NOT NULL,
           status VARCHAR NOT NULL, region VARCHAR NOT NULL)"""
    )
    con.execute(
        """CREATE TABLE supplier_stats (supplier_id VARCHAR NOT NULL, stat_time TIMESTAMP NOT NULL,
           is_active BOOLEAN NOT NULL, region VARCHAR NOT NULL, category VARCHAR NOT NULL)"""
    )
    con.executemany(
        "INSERT INTO order_facts VALUES (?,?,?,?,?,?)",
        [
            ("O1", "S1", "2026-10-31 16:30:00", 100, "paid", "华东"),
            ("O1", "S1", "2026-10-31 16:30:00", 100, "paid", "华东"),
            ("O2", "S2", "2026-10-31 15:00:00", 200, "paid", "华北"),
        ],
    )
    con.executemany(
        "INSERT INTO supplier_stats VALUES (?,?,?,?,?)",
        [("S1", "2026-10-31 16:30:00", True, "华东", "电子元器件")],
    )
    con.close()

    migrated = connect(str(path))
    try:
        columns = {r[0] for r in migrated.execute("DESCRIBE order_facts").fetchall()}
        assert {"ingest_batch_id", "row_version", "biz_day"} <= columns
        assert migrated.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 3
        # 重复行保留为两个版本，去重后只剩一单
        assert migrated.execute(
            "SELECT count(DISTINCT row_version) FROM order_facts WHERE order_id='O1'"
        ).fetchone()[0] == 2
        store = MetricStore.load(METRICS)
        assert migrated.execute(
            store.compile_query("orders_total", engine="duckdb").sql
        ).fetchone()[0] == 2
        assert migrated.execute(
            store.compile_query("gmv_cents", engine="duckdb").sql
        ).fetchone()[0] == 300
        # 16:30 UTC 属次日本地日
        assert migrated.execute(
            "SELECT biz_day FROM order_facts WHERE order_id='O1' LIMIT 1"
        ).fetchone()[0] == date(2026, 11, 1)
    finally:
        migrated.close()
    # 幂等：再次连接不应重复迁移
    again = connect(str(path))
    try:
        assert again.execute("SELECT count(*) FROM order_facts").fetchone()[0] == 3
    finally:
        again.close()


@pytestmark_ch
def test_ingest_batch_is_idempotent_on_append(con, ch_engine, loaded):
    """append 模式重复投递同一 batch 必须被拒绝，否则原始事实会翻倍。"""
    import uuid

    from meterhouse.errors import BatchAlreadyLoaded
    from meterhouse.ingest import ClickHouseLoader

    loader = ClickHouseLoader(ch_engine)
    batch = f"idem-{uuid.uuid4().hex[:8]}"
    loader.load_table(con, "order_facts", batch_id=batch)
    before = loader.row_counts()["order_facts"]
    with pytest.raises(BatchAlreadyLoaded):
        loader.load_table(con, "order_facts", batch_id=batch)
    assert loader.row_counts()["order_facts"] == before
    # 复原成干净状态
    loader.sync_all(con, truncate=True)


@pytestmark_ch
def test_clickhouse_reports_scanned_rows(ch_engine, loaded):
    result = ch_engine.query(
        "SELECT count() FROM order_facts", interface="test"
    )
    assert result.scanned_rows is not None and result.scanned_rows >= 0
