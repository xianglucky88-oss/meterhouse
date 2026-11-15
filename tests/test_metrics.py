"""指标语义层测试：编译器约束、引擎执行、口径与 ground_truth 对齐。

覆盖题目要求的每一类失败模式：
去重/过滤顺序、比率分子分母、未知维度、空结果、参数化安全、
业务时区归桶、非可加上卷拒绝、同环比重算、语义版本。
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from meterhouse.compiler import (
    Filter,
    InvalidRequestError,
    MetricPlan,
    QueryRequest,
    SemanticError,
    UnknownDimensionError,
    UnknownMetricError,
    UnsupportedSchemaError,
)
from meterhouse.engine import MetricEngine
from meterhouse.data import connect, ground_truth, seed
from meterhouse.semantic import MetricStore, build_catalog

YAML = Path("metrics/metrics.yaml")


@pytest.fixture(scope="module")
def con():
    path = Path(tempfile.mkdtemp(prefix="meterhouse-")) / "test.duckdb"
    c = connect(path)
    seed(c)
    yield c
    c.close()


@pytest.fixture(scope="module")
def catalog():
    return build_catalog(YAML)


@pytest.fixture(scope="module")
def engine(con, catalog):
    return MetricEngine(con, catalog.compiler)


# ---------------------------------------------------------------- 数据形态


def test_seed_is_deterministic(con):
    before = con.execute("SELECT count(*), sum(amount_cents) FROM order_facts").fetchone()
    seed(con)
    after = con.execute("SELECT count(*), sum(amount_cents) FROM order_facts").fetchone()
    assert before == after


def test_orders_have_duplicates(con):
    rows = con.execute("SELECT count(*) FROM order_facts").fetchone()[0]
    distinct = con.execute("SELECT count(DISTINCT order_id) FROM order_facts").fetchone()[0]
    assert rows > distinct


# ---------------------------------------------------------------- 加载与版本


def test_catalog_loads_v2_six_metrics(catalog):
    assert catalog.schema_version == 2
    assert len(catalog.metrics) == 6
    assert "active_suppliers" in catalog.metrics


def test_unknown_schema_version_rejected(tmp_path):
    bad = tmp_path / "m.yaml"
    bad.write_text("version: 99\nmetrics: []\n", encoding="utf-8")
    with pytest.raises(UnsupportedSchemaError):
        build_catalog(bad)


def test_metric_referencing_unknown_source_rejected(tmp_path):
    bad = tmp_path / "m.yaml"
    bad.write_text(
        """
version: 2
sources:
  t:
    table: t
    time_column: ts
    columns: [id]
    dimensions: {}
metrics:
  - name: m
    kind: distinct
    source: missing
    distinct: id
    time_column: ts
""",
        encoding="utf-8",
    )
    with pytest.raises(UnsupportedSchemaError):
        build_catalog(bad)


def test_legacy_store_facade_still_works():
    store = MetricStore.load(YAML)
    assert len(store.names()) == 6
    assert store.get("active_suppliers").additivity == "non_additive"
    with pytest.raises(KeyError):
        store.get("does_not_exist")


# ---------------------------------------------------------------- 去重与过滤顺序


def test_gmv_matches_ground_truth(engine, con):
    got = engine.run(QueryRequest.build("gmv_cents")).value
    assert got == ground_truth(con)["gmv_cents"]


def test_orders_total_dedup_matches_ground_truth(engine, con):
    got = engine.run(QueryRequest.build("orders_total")).value
    assert got == ground_truth(con)["orders_total"]


def test_dedup_cte_precedes_filter_in_sql(catalog):
    plan = catalog.compiler.compile(QueryRequest.build("gmv_cents"))
    sql = plan.sql
    assert sql.startswith("WITH src_dedup AS (SELECT DISTINCT * FROM order_facts)")
    # paid 过滤在去重 CTE 之后
    assert sql.index("src_dedup") < sql.index("status")


def test_legacy_compile_simple(con):
    store = MetricStore.load(YAML)
    sql = store.compile("orders_total")
    assert con.execute(sql).fetchone()[0] == ground_truth(con)["orders_total"]


# ---------------------------------------------------------------- 维度白名单


def test_unknown_dimension_rejected(engine):
    with pytest.raises(UnknownDimensionError):
        engine.run(QueryRequest.build("gmv_cents", dimensions=["nonexistent_dim"]))


def test_legacy_grouped_rejects_unknown_dimension():
    store = MetricStore.load(YAML)
    with pytest.raises(ValueError):
        store.compile_grouped("gmv_cents", ["nonexistent_dim"])


def test_unknown_metric_rejected(engine):
    with pytest.raises(UnknownMetricError):
        engine.run(QueryRequest.build("does_not_exist"))


def test_unknown_filter_column_rejected(engine):
    with pytest.raises(UnknownDimensionError):
        engine.run(QueryRequest.build("orders_total", filters=[Filter("hacker_col", "=", 1)]))


def test_illegal_identifier_rejected_at_gate(engine):
    # 注入形态的维度名先被指标白名单拒绝，绝无机会进入 SQL 拼接
    with pytest.raises(SemanticError):
        engine.run(
            QueryRequest.build(
                "orders_total", dimensions=("region; DROP TABLE order_facts--",)
            )
        )


def test_identifier_syntax_validator():
    from meterhouse.compiler import _ident

    assert _ident("region", "维度") == "region"
    for bad in ("a;b", "a b", "1col", "a--", "a'b", ""):
        with pytest.raises(InvalidRequestError):
            _ident(bad, "维度")


# ---------------------------------------------------------------- 参数化安全


def test_filter_values_are_bound_not_inlined(catalog):
    evil = "paid' OR '1'='1"
    plan = catalog.compiler.compile(
        QueryRequest.build("orders_total", filters=[Filter("status", "=", evil)])
    )
    assert evil not in plan.sql
    assert "?" in plan.sql
    assert evil in plan.base.params

def test_in_filter_expands_placeholders(catalog):
    plan = catalog.compiler.compile(
        QueryRequest.build("orders_total", filters=[Filter("region", "in", ["华东", "华南"])])
    )
    assert plan.sql.count("?") == 2
    assert plan.base.params == ("华东", "华南")


def test_bad_operator_rejected(engine):
    with pytest.raises(InvalidRequestError):
        engine.run(QueryRequest.build("orders_total", filters=[Filter("status", "--", "x")]))


def test_type_mismatch_rejected(engine):
    with pytest.raises(InvalidRequestError):
        engine.run(QueryRequest.build("active_suppliers", filters=[Filter("is_active", "=", "not-bool")]))


# ---------------------------------------------------------------- 时间粒度与时区


def test_granularity_whitelist(engine):
    with pytest.raises(InvalidRequestError):
        engine.run(QueryRequest.build("orders_total", granularity="century"))


def test_business_tz_bucketing_matches_gt(engine, con):
    gt = ground_truth(con, 8)
    # 北京时间 10-31 对应 UTC 10-30 16:00 起的半开区间
    start = datetime(2026, 10, 30, 16, 0, 0)
    end = start + timedelta(days=1)
    got = engine.run(QueryRequest.build("orders_total", start=start, end=end)).value
    assert got == gt["orders_by_local_date"]
    assert got != gt["orders_by_utc_date"]


def test_bucket_uses_business_offset(catalog):
    plan = catalog.compiler.compile(
        QueryRequest.build("orders_total", granularity="day")
    )
    assert "INTERVAL 8 HOUR" in plan.sql
    assert "date_trunc('day'" in plan.sql


def test_inverted_time_window_rejected(engine):
    with pytest.raises(InvalidRequestError):
        engine.run(
            QueryRequest.build(
                "orders_total",
                start=datetime(2026, 10, 2),
                end=datetime(2026, 10, 1),
            )
        )


def test_daily_series_groups_by_local_day(engine):
    rows = engine.run(QueryRequest.build("orders_total", granularity="day")).rows
    assert rows, "60 天数据必须产生日序列"
    assert "time_bucket" in rows[0]
    # 序列按时间桶有序
    buckets = [r["time_bucket"] for r in rows]
    assert buckets == sorted(buckets)


# ---------------------------------------------------------------- 比率指标


def test_paid_rate_recomputed_from_components(engine, con):
    result = engine.run(QueryRequest.build("paid_rate"))
    gt = ground_truth(con)
    paid = engine.run(
        QueryRequest.build("orders_total", filters=[Filter("status", "=", "paid")])
    ).value
    assert result.value == pytest.approx(paid / gt["orders_total"])


def test_ratio_carries_warning(engine):
    result = engine.run(QueryRequest.build("paid_rate"))
    assert any("比率" in w for w in result.warnings)


def test_avg_order_denominator_is_deduped(engine, con):
    result = engine.run(QueryRequest.build("avg_order_cents"))
    gt = ground_truth(con)
    paid = engine.run(
        QueryRequest.build("orders_total", filters=[Filter("status", "=", "paid")])
    ).value
    assert result.value == pytest.approx(gt["gmv_cents"] / paid)


def test_zero_denominator_flagged_and_null(engine):
    # 用一个不可能命中的状态制造空窗口：分母为 0，比率必须是 None 且有告警
    result = engine.run(
        QueryRequest.build(
            "paid_rate",
            filters=[Filter("status", "=", "nonexistent-status")],
            start=datetime(2030, 1, 1),
            end=datetime(2030, 1, 2),
        )
    )
    assert result.value is None
    assert any("zero_denominator" in w for w in result.warnings)
    # 隐藏分母列不能泄露到结果行
    assert all("_denom_paid_rate" not in r for r in result.rows)


# ---------------------------------------------------------------- 可加性上卷


def test_non_additive_rollup_rejected(engine):
    rows = engine.run(QueryRequest.build("active_suppliers", dimensions=["region"])).rows
    with pytest.raises(InvalidRequestError):
        engine.rollup_total("active_suppliers", rows)


def test_ratio_rollup_rejected(engine):
    with pytest.raises(InvalidRequestError):
        engine.rollup_total("paid_rate", [{"paid_rate": 0.5}])


def test_additive_rollup_matches_recomputed_total(engine):
    rows = engine.run(QueryRequest.build("gmv_cents", dimensions=["region"])).rows
    rolled = engine.rollup_total("gmv_cents", rows)
    total = engine.run(QueryRequest.build("gmv_cents")).value
    assert rolled == total


def test_region_total_recomputed_not_summed(engine, con):
    # 区域分组之和（跨区域重复计数）与无维度重算不同；接口必须采信重算值
    rows = engine.run(QueryRequest.build("active_suppliers", dimensions=["region"])).rows
    naive_sum = sum(r["active_suppliers"] for r in rows)
    total = engine.run(QueryRequest.build("active_suppliers")).value
    assert total == ground_truth(con)["active_suppliers"]
    assert naive_sum >= total  # 换过区域的供应商在分组和里被重复计入


# ---------------------------------------------------------------- 同环比


def test_compare_requires_window(engine):
    with pytest.raises(InvalidRequestError):
        engine.run(QueryRequest.build("gmv_cents", compare="previous_period"))


def test_previous_period_recomputes_shifted_window(engine):
    start = datetime(2026, 10, 1)
    end = datetime(2026, 10, 8)
    result = engine.run(
        QueryRequest.build("gmv_cents", start=start, end=end, compare="previous_period")
    )
    direct = engine.run(
        QueryRequest.build("gmv_cents", start=start - timedelta(days=7), end=end - timedelta(days=7))
    ).value
    assert result.comparisons["previous_period"][0]["gmv_cents"] == direct


def test_compare_works_for_non_additive_metric(engine):
    start = datetime(2026, 10, 1)
    end = datetime(2026, 10, 8)
    result = engine.run(
        QueryRequest.build(
            "active_suppliers", start=start, end=end, compare="previous_year"
        )
    )
    # 主窗口有值；去年同期窗口无数据，标量比较行缺组语义为 None，而不是伪造 0
    assert result.value is not None
    assert result.comparisons["previous_year"][0]["active_suppliers"] is None


def test_unknown_compare_mode_rejected(engine):
    with pytest.raises(InvalidRequestError):
        engine.run(
            QueryRequest.build(
                "gmv_cents",
                start=datetime(2026, 10, 1),
                end=datetime(2026, 10, 8),
                compare="tomorrow",
            )
        )


# ---------------------------------------------------------------- 空结果


def test_empty_window_scalar_is_zero(engine):
    result = engine.run(
        QueryRequest.build(
            "gmv_cents",
            start=datetime(2030, 1, 1),
            end=datetime(2030, 1, 2),
        )
    )
    assert result.value == 0


def test_empty_window_grouped_is_empty_list(engine):
    result = engine.run(
        QueryRequest.build(
            "gmv_cents",
            dimensions=["region"],
            start=datetime(2030, 1, 1),
            end=datetime(2030, 1, 2),
        )
    )
    assert result.rows == []
    assert result.value is None


def test_rollup_empty_groups_is_zero(engine):
    assert engine.rollup_total("gmv_cents", []) == 0


# ---------------------------------------------------------------- limit


def test_limit_bounds(engine):
    with pytest.raises(InvalidRequestError):
        engine.run(QueryRequest.build("orders_total", granularity="day", limit=0))
    with pytest.raises(InvalidRequestError):
        engine.run(QueryRequest.build("orders_total", granularity="day", limit=99999))


def test_limit_is_parameterized(engine):
    result = engine.run(QueryRequest.build("orders_total", granularity="day", limit=3))
    assert len(result.rows) == 3
    assert result.params[-1] == 3


# ---------------------------------------------------------------- 端到端口径


def test_active_suppliers_matches_ground_truth(engine, con):
    got = engine.run(QueryRequest.build("active_suppliers")).value
    assert got == ground_truth(con)["active_suppliers"]


def test_grouped_dimension_values(engine):
    rows = engine.run(QueryRequest.build("gmv_cents", dimensions=["region"])).rows
    assert {r["region"] for r in rows} == {"华东", "华南", "华北", "西南"}
    assert all("gmv_cents" in r for r in rows)


def test_compiler_returns_plan_not_string(engine):
    plan = engine.compiler.compile(QueryRequest.build("gmv_cents", dimensions=["region"]))
    assert isinstance(plan, MetricPlan)
    assert plan.base.level == "base"
    # rendered_sql 仅用于展示，与执行 SQL 分离
    assert plan.rendered_sql() != plan.sql or plan.params == ()
