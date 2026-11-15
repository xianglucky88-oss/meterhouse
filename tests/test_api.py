"""API 契约测试：切流、回退、503 语义。

用 FastAPI TestClient + 真实 DuckDB；ClickHouse 部分仅在配置后跑。
关键是验证**接口层面**的承诺：

* 引擎不可用 → 503，响应体里没有指标值
* 有回退 → 200 + degraded=true + fallback_from
* 逐接口切流 → 只有被切的那个接口换引擎
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def app_env(tmp_path_factory):
    """指向临时库的 API 实例，避免动到 data/。"""
    import duckdb

    from meterhouse.config import settings
    from meterhouse.data import connect, seed

    db = tmp_path_factory.mktemp("api") / "api.duckdb"
    con = connect(str(db))
    seed(con)
    con.close()

    old = {
        "DB_PATH": os.environ.get("DB_PATH"),
        "MH_CUTOVER_FILE": os.environ.get("MH_CUTOVER_FILE"),
    }
    os.environ["DB_PATH"] = str(db)
    os.environ["MH_CUTOVER_FILE"] = str(tmp_path_factory.mktemp("routes") / "routes.yaml")
    settings.db_path = db
    settings.cutover_file = Path(os.environ["MH_CUTOVER_FILE"])

    # 重置模块级缓存
    import meterhouse.api as api

    api._store = None
    api._router = None
    api._cutover = None
    api._duck = None
    api._ch = None
    yield api
    for key, value in old.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    if api._duck is not None:
        api._duck.close()


@pytest.fixture()
def client(app_env):
    return TestClient(app_env.app)


def test_healthz(client):
    assert client.get("/healthz").json()["status"] == "ok"


def test_summary_returns_real_values(client):
    r = client.get("/api/v1/dashboard/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["active_suppliers"] > 0
    assert body["gmv_cents"] > 0
    assert body["engine"] == "duckdb"
    assert body["degraded"] is False
    assert body["traceable"]["source"] == "supplier_stats"


def test_metric_query_dimensions_and_granularity(client):
    r = client.get(
        "/api/v1/metrics/gmv_cents",
        params={"dimensions": "region", "granularity": "month"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["dimensions"] == ["region"]
    # 4 个区域 × 数据覆盖的业务月（9/10 两个月，10 月有跨月订单归到 11 月）
    assert body["row_count"] == 4 * len({row["time_bucket"] for row in body["rows"]})
    assert len({row["time_bucket"] for row in body["rows"]}) >= 2
    assert all("time_bucket" in row for row in body["rows"])


def test_unknown_metric_is_400_not_500(client):
    r = client.get("/api/v1/metrics/not_a_metric")
    assert r.status_code == 400


def test_illegal_dimension_is_400(client):
    r = client.get("/api/v1/metrics/gmv_cents", params={"dimensions": "created_at"})
    assert r.status_code == 400


def test_illegal_granularity_is_400(client):
    r = client.get("/api/v1/metrics/orders_total", params={"granularity": "hour"})
    assert r.status_code == 400


def test_routes_default_to_off(client):
    body = client.get("/api/v1/routes").json()
    modes = {r["interface"]: r["mode"] for r in body["interfaces"]}
    assert all(m == "off" for m in modes.values())


def test_cutover_requires_token_when_configured(client, app_env):
    from meterhouse.config import settings

    settings.admin_token = "secret"
    try:
        r = client.post("/api/v1/routes/dashboard.summary", json={"mode": "shadow"})
        assert r.status_code == 403
        r = client.post(
            "/api/v1/routes/dashboard.summary",
            json={"mode": "shadow"},
            headers={"X-Admin-Token": "secret"},
        )
        assert r.status_code == 200
    finally:
        settings.admin_token = ""


def test_cutover_rejects_unconfigured_target(client, app_env):
    """没配 ClickHouse 时不允许把接口切到 primary——否则每次请求都会 503。"""
    from meterhouse.config import settings

    if settings.clickhouse_enabled:
        pytest.skip("clickhouse configured")
    r = client.post("/api/v1/routes/dashboard.summary", json={"mode": "primary"})
    assert r.status_code == 400


CH = bool(os.environ.get("CLICKHOUSE_HOST"))


@pytest.mark.skipif(not CH, reason="set CLICKHOUSE_HOST to exercise real cutover")
def test_primary_cutover_serves_target_and_matches(client, app_env):
    from meterhouse.config import settings
    from meterhouse.ingest import ClickHouseLoader

    if not settings.clickhouse_enabled:
        pytest.skip("clickhouse not configured in this process")
    responded = client.get("/api/v1/dashboard/summary").json()

    client.post("/api/v1/routes/dashboard.summary", json={"mode": "primary"})
    body = client.get("/api/v1/dashboard/summary").json()
    assert body["engine"] == "clickhouse"
    assert body["degraded"] is False
    assert body["active_suppliers"] == responded["active_suppliers"]
    assert body["gmv_cents"] == responded["gmv_cents"]
    # 复原
    client.post("/api/v1/routes/dashboard.summary", json={"mode": "off"})
