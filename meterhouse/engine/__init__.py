"""查询执行引擎抽象 + 连接管理 + 容量控制。

设计要点
--------
1. **连接与查询分离**：接口的切流开关只控制"读哪个引擎"，连接池、
   熔断器、并发额度对每个引擎各自独立，一个引擎故障不会拖垮另一个。
2. **故障显式化**：所有引擎异常都转换成 `meterhouse.errors` 里的分类异常，
   绝不返回零值/空值冒充成功结果。
3. **有界资源**：每次查询都有并发上限、行数上限、扫描行数上限和超时，
   超限抛 `CapacityExceeded`，由路由层决定是否回退。
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Protocol

from ..errors import (
    CapacityExceeded,
    EngineError,
    EngineQueryError,
    EngineTimeout,
    EngineUnavailable,
)


@dataclass
class ResultSet:
    """一次查询的结果。`columns` 与 `rows` 一一对应。

    `compiled` / `interface` / `degraded` / `fallback_from` 由路由层填充，
    用于审计与降级标记；引擎本身不关心它们。
    """

    columns: list[str]
    rows: list[tuple[Any, ...]]
    engine: str
    elapsed_ms: float
    row_count: int
    scanned_rows: int | None = None
    compiled: Any = None
    interface: str | None = None
    degraded: bool = False
    fallback_from: str | None = None
    failure_reason: str | None = None

    def as_dicts(self) -> list[dict[str, Any]]:
        return [dict(zip(self.columns, r)) for r in self.rows]

    def scalar(self) -> Any:
        if not self.rows or not self.rows[0]:
            return None
        return self.rows[0][0]


class Engine(Protocol):
    name: str

    def query(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        interface: str | None = None,
        max_rows: int | None = None,
    ) -> ResultSet: ...

    def healthy(self) -> bool: ...


# ---------------------------------------------------------------------------
# 容量控制：并发额度 + 熔断
# ---------------------------------------------------------------------------


@dataclass
class CapacityPolicy:
    """单个引擎的容量策略。"""

    max_concurrency: int = 8
    # 排队等待并发额度的最长时间；超时视为容量不足，不做无限阻塞。
    acquire_timeout_s: float = 2.0
    # 单次查询最多返回的行数（防止高基数维度把响应撑爆）。
    max_result_rows: int = 50_000
    # 熔断：连续失败多少次后打开，打开后多久进入半开。
    breaker_threshold: int = 5
    breaker_cooldown_s: float = 15.0


class Breaker:
    """极简熔断器。

    打开期间 `allow()` 直接返回 False，调用方应立即失败而不是去连引擎，
    这样引擎整体不可用时不会把每个请求都拖到连接超时。
    """

    def __init__(self, threshold: int, cooldown_s: float) -> None:
        self._threshold = threshold
        self._cooldown_s = cooldown_s
        self._failures = 0
        self._opened_at: float | None = None
        # 可重入：`snapshot()` 会在持锁状态下读 `state`。
        self._lock = threading.RLock()

    @property
    def state(self) -> str:
        with self._lock:
            if self._opened_at is None:
                return "closed"
            if time.monotonic() - self._opened_at >= self._cooldown_s:
                return "half_open"
            return "open"

    def allow(self) -> bool:
        return self.state != "open"

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self._threshold and self._opened_at is None:
                self._opened_at = time.monotonic()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "state": self.state,
                "consecutive_failures": self._failures,
            }


class ConcurrencyGate:
    """非阻塞有上限的并发闸门。

    用信号量实现；`acquire_timeout_s` 内拿不到额度就抛 `CapacityExceeded`，
    避免请求在锁上无限堆积。
    """

    def __init__(self, limit: int, timeout_s: float) -> None:
        self._sem = threading.BoundedSemaphore(limit)
        self._timeout = timeout_s
        self._limit = limit
        self._in_flight = 0
        self._lock = threading.Lock()

    @contextmanager
    def slot(self, *, engine: str, interface: str | None) -> Iterator[None]:
        if not self._sem.acquire(timeout=self._timeout):
            raise CapacityExceeded(
                f"{engine} concurrency limit reached (limit={self._limit})",
                engine=engine,
                interface=interface,
            )
        with self._lock:
            self._in_flight += 1
        try:
            yield
        finally:
            with self._lock:
                self._in_flight -= 1
            self._sem.release()

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "limit": self._limit,
                "in_flight": self._in_flight,
                "available": self._limit - self._in_flight,
            }


# ---------------------------------------------------------------------------
# DuckDB 引擎
# ---------------------------------------------------------------------------


@dataclass
class DuckDBEngine:
    """本地分析引擎。

    DuckDB 的连接对象不能跨线程并发 execute，因此这里维护一个父连接，
    每次查询用 `cursor()` 派生出独立的子连接（共享同一数据库实例）。
    父连接在进程内只建一次，避免与写连接（seed/admin）配置冲突。
    """

    name: str = "duckdb"
    path: str = "data/meterhouse.duckdb"
    policy: CapacityPolicy = field(default_factory=CapacityPolicy)
    _parent: Any = field(default=None, init=False, repr=False)
    _parent_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _breaker: Breaker = field(init=False, repr=False)
    _gate: ConcurrencyGate = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._breaker = Breaker(self.policy.breaker_threshold, self.policy.breaker_cooldown_s)
        self._gate = ConcurrencyGate(self.policy.max_concurrency, self.policy.acquire_timeout_s)

    def _connection(self):
        import duckdb

        with self._parent_lock:
            if self._parent is None:
                self._parent = duckdb.connect(self.path)
                self._parent.execute("SET threads = 4")
            return self._parent.cursor()

    def close(self) -> None:
        """显式关闭父连接（进程退出或测试收尾）。"""
        with self._parent_lock:
            if self._parent is not None:
                try:
                    self._parent.close()
                except Exception:  # noqa: BLE001
                    pass
                self._parent = None

    def query(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        interface: str | None = None,
        max_rows: int | None = None,
    ) -> ResultSet:
        limit = min(max_rows or self.policy.max_result_rows, self.policy.max_result_rows)
        if not self._breaker.allow():
            raise EngineUnavailable("breaker open", engine=self.name, interface=interface)
        with self._gate.slot(engine=self.name, interface=interface):
            start = time.perf_counter()
            try:
                cur = self._connection()
                raw = cur.execute(sql, params) if params else cur.execute(sql)
                columns = [d[0] for d in raw.description] if raw.description else []
                rows = raw.fetchmany(limit + 1)
                elapsed = (time.perf_counter() - start) * 1000
            except EngineError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._breaker.record_failure()
                raise _translate_duckdb(exc, self.name, interface) from exc
            self._breaker.record_success()

        if len(rows) > limit:
            raise CapacityExceeded(
                f"result exceeds row limit ({limit})",
                engine=self.name,
                interface=interface,
            )
        return ResultSet(
            columns=columns,
            rows=[tuple(r) for r in rows],
            engine=self.name,
            elapsed_ms=round(elapsed, 2),
            row_count=len(rows),
        )

    def healthy(self) -> bool:
        try:
            self.query("SELECT 1", interface="health")
            return True
        except EngineError:
            return False

    def status(self) -> dict[str, Any]:
        return {
            "engine": self.name,
            "breaker": self._breaker.snapshot(),
            "capacity": self._gate.snapshot(),
            "path": self.path,
        }


# ---------------------------------------------------------------------------
# ClickHouse 引擎
# ---------------------------------------------------------------------------


@dataclass
class ClickHouseEngine:
    """列式分析引擎（生产目标）。

    通过 HTTP 接口 + clickhouse-connect 访问。`clickhouse-connect` 自带连接池，
    这里只负责把异常归一化，并施加行数与并发上限。
    """

    name: str = "clickhouse"
    host: str = "127.0.0.1"
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "meterhouse"
    connect_timeout_s: float = 3.0
    policy: CapacityPolicy = field(default_factory=CapacityPolicy)
    _client: Any = field(default=None, init=False, repr=False)
    _client_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _breaker: Breaker = field(init=False, repr=False)
    _gate: ConcurrencyGate = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._breaker = Breaker(self.policy.breaker_threshold, self.policy.breaker_cooldown_s)
        self._gate = ConcurrencyGate(self.policy.max_concurrency, self.policy.acquire_timeout_s)

    def _get_client(self, database: str | None = None):
        """返回客户端；`database` 用于管理操作（建库）临时连到 `system`。"""
        if database is not None:
            import clickhouse_connect

            return clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=database,
                connect_timeout=self.connect_timeout_s,
            )
        with self._client_lock:
            if self._client is None:
                import clickhouse_connect

                self._client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    database=self.database,
                    connect_timeout=self.connect_timeout_s,
                )
            return self._client

    def ensure_database(self) -> None:
        client = self._get_client(database="system")
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        finally:
            client.close()

    def _drop_client(self) -> None:
        with self._client_lock:
            if self._client is not None:
                try:
                    self._client.close()
                except Exception:  # noqa: BLE001
                    pass
                self._client = None

    def query(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        interface: str | None = None,
        max_rows: int | None = None,
    ) -> ResultSet:
        limit = min(max_rows or self.policy.max_result_rows, self.policy.max_result_rows)
        if not self._breaker.allow():
            raise EngineUnavailable("breaker open", engine=self.name, interface=interface)
        with self._gate.slot(engine=self.name, interface=interface):
            start = time.perf_counter()
            try:
                client = self._get_client()
                # settings 用参数形式传，避免把配置拼进 SQL。
                result = client.query(
                    sql,
                    parameters=params or {},
                    settings={
                        "max_result_rows": limit,
                        "result_overflow_mode": "throw",
                        "max_execution_time": 30,
                        "max_rows_to_read": 500_000_000,
                    },
                )
                columns = list(result.column_names)
                rows = [tuple(r) for r in result.result_rows]
                # read_rows 是实际扫描行数，可用于容量观测与回退决策。
                scanned = result.summary.get("read_rows") if result.summary else None
                scanned_rows = int(scanned) if scanned is not None else None
                elapsed = (time.perf_counter() - start) * 1000
            except EngineError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._breaker.record_failure()
                self._drop_client()
                raise _translate_clickhouse(exc, self.name, interface) from exc
            self._breaker.record_success()

        if len(rows) > limit:
            raise CapacityExceeded(
                f"result exceeds row limit ({limit})",
                engine=self.name,
                interface=interface,
            )
        return ResultSet(
            columns=columns,
            rows=rows,
            engine=self.name,
            elapsed_ms=round(elapsed, 2),
            row_count=len(rows),
            scanned_rows=scanned_rows,
        )

    def healthy(self) -> bool:
        try:
            self.query("SELECT 1", interface="health")
            return True
        except EngineError:
            return False

    def status(self) -> dict[str, Any]:
        return {
            "engine": self.name,
            "breaker": self._breaker.snapshot(),
            "capacity": self._gate.snapshot(),
            "host": f"{self.host}:{self.port}",
            "database": self.database,
        }


# ---------------------------------------------------------------------------
# 异常翻译
# ---------------------------------------------------------------------------


def _translate_duckdb(exc: Exception, engine: str, interface: str | None) -> EngineError:
    name = type(exc).__name__.lower()
    text = str(exc)
    if "timeout" in name or "timeout" in text.lower() or "interrupt" in name:
        return EngineTimeout(text, engine=engine, interface=interface)
    if "out of memory" in text.lower() or "memory" in text.lower():
        return CapacityExceeded(text, engine=engine, interface=interface)
    if "cannot open" in text.lower() or "unable to open" in text.lower() or "io error" in text.lower():
        return EngineUnavailable(text, engine=engine, interface=interface)
    return EngineQueryError(text, engine=engine, interface=interface)


def _translate_clickhouse(exc: Exception, engine: str, interface: str | None) -> EngineError:
    name = type(exc).__name__
    text = str(exc)
    # clickhouse-connect 的错误文本形如 "received ClickHouse error code 396"。
    code = ""
    marker = "error code "
    if marker in text:
        tail = text.split(marker, 1)[1]
        code = "".join(ch for ch in tail[:10] if ch.isdigit())
    # clickhouse-connect 的 OperationalError 表示网络/连接层失败。
    if name in {"OperationalError", "ConnectionError", "ConnectTimeout", "ReadTimeout"}:
        return EngineUnavailable(text, engine=engine, interface=interface)
    if "TIMEOUT" in text or "Timeout exceeded" in text:
        return EngineTimeout(text, engine=engine, interface=interface)
    # 158: TOO_MANY_ROWS; 396: TOO_MANY_ROWS_OR_BYTES(result); 241: MEMORY_LIMIT; 160: TOO_MANY_SIMULTANEOUS_QUERIES
    if (
        "TOO_MANY_ROWS" in text
        or "QUOTA" in text
        or "MEMORY_LIMIT" in text
        or "TOO_MANY_SIMULTANEOUS_QUERIES" in text
        or code in {"158", "396", "241", "160", "159"}
    ):
        return CapacityExceeded(text, engine=engine, interface=interface)
    if "UNKNOWN_DATABASE" in text or "UNKNOWN_TABLE" in text:
        return EngineUnavailable(text, engine=engine, interface=interface)
    return EngineQueryError(text, engine=engine, interface=interface)
