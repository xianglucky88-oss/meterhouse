"""容量与故障隔离测试。

这些用例不依赖真实 ClickHouse 的规模数据，只验证**控制面**行为：

* 结果行数上限 → CapacityExceeded（而不是把响应撑爆或截断后冒充完整结果）
* 并发额度 → 超出即 CapacityExceeded，而不是无限排队
* 熔断 → 连续失败后快速失败，不再去打已宕机的引擎
* 引擎错误 → ServiceUnavailable，绝不返回 0
"""

from __future__ import annotations

import threading
import time

import pytest

from meterhouse.engine import (
    Breaker,
    CapacityPolicy,
    ConcurrencyGate,
    DuckDBEngine,
    ResultSet,
)
from meterhouse.errors import CapacityExceeded, EngineUnavailable


@pytest.fixture()
def duck(tmp_path):
    import duckdb

    path = tmp_path / "cap.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE t AS SELECT range AS n FROM range(0, 1000)")
    con.close()
    engine = DuckDBEngine(path=str(path), policy=CapacityPolicy(max_result_rows=10))
    yield engine
    engine.close()


def test_result_row_limit_raises_capacity_exceeded(duck):
    with pytest.raises(CapacityExceeded):
        duck.query("SELECT n FROM t", interface="cap")


def test_result_row_limit_allows_exactly_at_limit(duck):
    result = duck.query("SELECT n FROM t LIMIT 10", interface="cap")
    assert result.row_count == 10


def test_capacity_exceeded_carries_engine_and_interface(duck):
    with pytest.raises(CapacityExceeded) as excinfo:
        duck.query("SELECT n FROM t", interface="dashboard.summary")
    assert excinfo.value.engine == "duckdb"
    assert excinfo.value.interface == "dashboard.summary"


def test_missing_table_is_query_error_not_empty_result(duck):
    from meterhouse.errors import EngineQueryError

    with pytest.raises(EngineQueryError):
        duck.query("SELECT * FROM does_not_exist", interface="cap")


def test_breaker_opens_after_threshold():
    breaker = Breaker(threshold=3, cooldown_s=10)
    for _ in range(3):
        breaker.record_failure()
    assert breaker.state == "open"
    assert breaker.allow() is False


def test_breaker_half_opens_after_cooldown():
    breaker = Breaker(threshold=1, cooldown_s=0.05)
    breaker.record_failure()
    assert breaker.state == "open"
    time.sleep(0.06)
    assert breaker.state == "half_open"
    assert breaker.allow() is True
    breaker.record_success()
    assert breaker.state == "closed"


def test_breaker_open_engine_fails_fast(tmp_path):
    engine = DuckDBEngine(
        path=str(tmp_path / "x.duckdb"),
        policy=CapacityPolicy(breaker_threshold=1, breaker_cooldown_s=60),
    )
    # 人为打开熔断
    engine._breaker.record_failure()
    start = time.perf_counter()
    with pytest.raises(EngineUnavailable):
        engine.query("SELECT 1", interface="cap")
    assert (time.perf_counter() - start) < 0.5, "熔断打开后应立即失败，而不是连接超时"


def test_concurrency_gate_rejects_when_full():
    gate = ConcurrencyGate(limit=1, timeout_s=0.05)
    with gate.slot(engine="duckdb", interface="cap"):
        with pytest.raises(CapacityExceeded):
            with gate.slot(engine="duckdb", interface="cap"):
                pass


def test_concurrency_gate_releases_on_error():
    gate = ConcurrencyGate(limit=1, timeout_s=0.05)
    with pytest.raises(RuntimeError):
        with gate.slot(engine="duckdb", interface="cap"):
            raise RuntimeError("boom")
    # 额度必须已释放
    with gate.slot(engine="duckdb", interface="cap"):
        pass
    assert gate.snapshot()["in_flight"] == 0


def test_concurrent_reads_are_served(duck):
    errors = []
    results = []

    def worker():
        try:
            results.append(duck.query("SELECT count(*) FROM t LIMIT 1", interface="cap").scalar())
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert results == [1000] * 8


def test_capacity_failure_on_target_falls_back(tmp_path):
    """容量回退：target 抛 CapacityExceeded 时，primary 模式应回退 control。"""
    from meterhouse.routing import CutoverStore, Router
    from meterhouse.semantic import CompiledQuery

    class CapacityEngine:
        name = "clickhouse"

        def query(self, sql, *, params=None, interface=None, max_rows=None):
            raise CapacityExceeded("too many rows", engine=self.name, interface=interface)

        def status(self):
            return {}

    class ControlEngine:
        name = "duckdb"

        def query(self, sql, *, params=None, interface=None, max_rows=None):
            return ResultSet(
                columns=["v"], rows=[(7,)], engine=self.name, elapsed_ms=0.1, row_count=1
            )

        def status(self):
            return {}

    cq = CompiledQuery(
        engine="x", metric="m", sql="SELECT 1", value_column="v",
        dimension_columns=[], time_column_alias=None, value_type="int",
        empty_value=0, description="", definition_version="t",
    )
    build = lambda engine_name: cq  # noqa: E731

    store = CutoverStore(tmp_path / "r.yaml", ["iface"])
    store.update("iface", mode="primary", fallback=True, actor="t")
    router = Router(
        {"duckdb": ControlEngine(), "clickhouse": CapacityEngine()}, store
    )
    result = router.execute("iface", build)
    assert result.engine == "duckdb"
    assert result.degraded is True
    assert result.fallback_from == "clickhouse"
    assert "too many rows" in result.failure_reason


def test_both_engines_failing_is_service_unavailable(tmp_path):
    """primary + fallback 时两个引擎都失败：必须包装成 ServiceUnavailable。"""
    from meterhouse.errors import ServiceUnavailable
    from meterhouse.routing import CutoverStore, Router
    from meterhouse.semantic import CompiledQuery

    class Down:
        def __init__(self, name):
            self.name = name

        def query(self, *a, **k):
            raise EngineUnavailable("down", engine=self.name)

        def status(self):
            return {}

    cq = CompiledQuery(
        engine="x", metric="m", sql="", value_column="v", dimension_columns=[],
        time_column_alias=None, value_type="float", empty_value=0,
        description="", definition_version="t",
    )
    store = CutoverStore(tmp_path / "r.yaml", ["iface"])
    store.update("iface", mode="primary", fallback=True, actor="t")
    router = Router({"duckdb": Down("duckdb"), "clickhouse": Down("clickhouse")}, store)
    with pytest.raises(ServiceUnavailable) as excinfo:
        router.execute("iface", lambda n: cq)
    engines = {c.engine for c in excinfo.value.causes}
    assert engines == {"duckdb", "clickhouse"}
