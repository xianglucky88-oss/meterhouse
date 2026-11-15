"""逐接口切流与回退。

切流是**按接口**的，而不是全局开关：一个接口要先在 ClickHouse 上验证
口径与容量，才把流量切过去；其余接口继续走 DuckDB。

三个角色
--------
* `control`  当前可信引擎（默认 DuckDB），迁移期始终可回退。
* `target`   迁移目标引擎（默认 ClickHouse）。
* `mode`     决定请求由谁服务：

        off      只走 control
        shadow   仍由 control 服务；同时把同一份查询发到 target，
                 比对结果并记录差异（**不改变响应数字**）
        primary  由 target 服务；target 故障时按 fallback 回退 control

每次配置变更都写审计日志，并立即持久化。
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .errors import DefinitionError, EngineError, ServiceUnavailable

log = logging.getLogger("meterhouse.routing")

VALID_ENGINES = {"duckdb", "clickhouse"}
VALID_MODES = {"off", "shadow", "primary"}


def _mode_value(raw: Any) -> str:
    """YAML 1.1 会把 `off/on` 解析成布尔；显式还原 `off`，其余布尔值拒绝。

    `off` 是文档里的默认值，被解析成 False 时必须还原；`on` 没有对应语义，
    静默猜成 primary 会导致"以为没切、其实切了"，因此直接报错。
    """
    if raw is False or raw is None:
        return "off"
    if raw is True:
        raise DefinitionError("mode: on is ambiguous; use one of 'off', 'shadow', 'primary'")
    value = str(raw)
    if value not in VALID_MODES:
        raise DefinitionError(f"invalid mode {raw!r}; expected one of {sorted(VALID_MODES)}")
    return value


def _engine_value(raw: Any, field: str) -> str:
    value = str(raw)
    if value not in VALID_ENGINES:
        raise DefinitionError(
            f"invalid {field} {raw!r}; expected one of {sorted(VALID_ENGINES)}"
        )
    return value


@dataclass
class RouteConfig:
    """一个接口的切流配置。"""

    interface: str
    mode: str = "off"
    target: str = "clickhouse"
    control: str = "duckdb"
    fallback: bool = True
    # 结果差异容忍度（相对误差）。0 表示要求完全一致。
    tolerance_pct: float = 0.0
    last_changed_at: str | None = None
    last_changed_by: str | None = None

    def validate(self) -> None:
        if self.mode not in VALID_MODES:
            raise DefinitionError(f"invalid mode {self.mode!r} for {self.interface}")
        _engine_value(self.target, "target")
        _engine_value(self.control, "control")
        if self.target == self.control:
            raise DefinitionError("target and control must be different engines")

    def serving_engine(self) -> str:
        """该模式下由谁服务请求。"""
        return self.control if self.mode in ("off", "shadow") else self.target

    def compare_engine(self) -> str | None:
        """shadow 模式下需要比对的另一侧。"""
        return self.target if self.mode == "shadow" else None


class CutoverStore:
    """切流配置的持久化 + 内存快照 + 审计。"""

    def __init__(self, path: Path, interfaces: list[str], audit_path: Path | None = None) -> None:
        self._path = path
        self._audit_path = audit_path or path.with_suffix(".audit.jsonl")
        self._lock = threading.RLock()
        self._interfaces = interfaces
        self._routes: dict[str, RouteConfig] = {
            name: RouteConfig(interface=name) for name in interfaces
        }
        self._load()

    # ------------------------------------------------------------------ 读写
    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = yaml.safe_load(self._path.read_text(encoding="utf-8")) or {}
        with self._lock:
            for item in raw.get("interfaces", []):
                name = item["interface"]
                if name not in self._routes:
                    # 未知接口不静默接受，避免拼写错误导致"以为切了"。
                    log.warning("ignoring unknown interface in cutover file: %s", name)
                    continue
                self._routes[name] = RouteConfig(
                    interface=name,
                    mode=_mode_value(item.get("mode", "off")),
                    target=_engine_value(item.get("target", "clickhouse"), "target"),
                    control=_engine_value(item.get("control", "duckdb"), "control"),
                    fallback=bool(item.get("fallback", True)),
                    tolerance_pct=float(item.get("tolerance_pct", 0.0)),
                    last_changed_at=item.get("last_changed_at"),
                    last_changed_by=item.get("last_changed_by"),
                )

    def route(self, interface: str) -> RouteConfig:
        with self._lock:
            if interface not in self._routes:
                raise DefinitionError(f"unknown interface: {interface}")
            return self._routes[interface]

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                asdict(r) for r in sorted(self._routes.values(), key=lambda r: r.interface)
            ]

    def update(
        self,
        interface: str,
        *,
        mode: str | None = None,
        target: str | None = None,
        control: str | None = None,
        fallback: bool | None = None,
        actor: str = "anonymous",
        reason: str = "",
    ) -> RouteConfig:
        with self._lock:
            if interface not in self._routes:
                raise DefinitionError(f"unknown interface: {interface}")
            current = self._routes[interface]
            new = RouteConfig(**asdict(current))
            if mode is not None:
                new.mode = mode
            if target is not None:
                new.target = target
            if control is not None:
                new.control = control
            if fallback is not None:
                new.fallback = fallback
            new.last_changed_at = datetime.now(timezone.utc).isoformat()
            new.last_changed_by = actor
            new.validate()
            # 先审计再落盘：即使落盘失败，变更意图也已记录。
            self._audit(interface, asdict(current), asdict(new), actor, reason)
            self._routes[interface] = new
            self._persist()
            return new

    def _persist(self) -> None:
        payload = {"interfaces": self.snapshot()}
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        header = (
            "# 逐接口切流配置（由 meterhouse.routing.CutoverStore 生成）。\n"
            "# 角色：control=当前可信引擎，target=迁移目标引擎。\n"
            "# mode：'off' 只走 control；'shadow' 由 control 服务并比对 target；\n"
            "#       'primary' 由 target 服务，故障时按 fallback 回退 control。\n"
            "# 直接手改本文件也可以，但注释与顺序会被下一次写入规范化。\n\n"
        )
        tmp.write_text(
            header + yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        os.replace(tmp, self._path)

    def _audit(
        self,
        interface: str,
        before: dict[str, Any],
        after: dict[str, Any],
        actor: str,
        reason: str,
    ) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "interface": interface,
            "actor": actor,
            "reason": reason,
            "before": before,
            "after": after,
        }
        try:
            self._audit_path.parent.mkdir(parents=True, exist_ok=True)
            with self._audit_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError as exc:  # noqa: BLE001
            log.error("failed to write cutover audit: %s", exc)


class Router:
    """按接口执行查询，负责影子比对、回退与差异记录。

    它与具体引擎解耦：只要求引擎实现 `query()` / `status()`。
    """

    def __init__(self, engines: dict[str, Any], cutover: CutoverStore) -> None:
        self._engines = engines
        self._cutover = cutover
        self._comparisons = ComparisonLog()

    # ------------------------------------------------------------------ 执行
    def execute(self, interface: str, build_sql, *, params=None, max_rows=None):
        """执行一次查询。

        `build_sql(engine_name)` 返回该引擎的 `CompiledQuery`。
        """
        route = self._cutover.route(interface)
        serving = route.serving_engine()
        if serving not in self._engines:
            raise ServiceUnavailable(
                f"interface {interface}: serving engine {serving!r} is not configured",
                interface=interface,
            )

        try:
            result = self._run(self._engines[serving], interface, build_sql, params, max_rows)

            if route.mode == "shadow":
                self._shadow_compare(interface, route, build_sql, params, max_rows, result)
            return result

        except EngineError as exc:
            if route.mode != "primary" or not route.fallback or route.control not in self._engines:
                # 关键：把故障如实上报，绝不返回占位指标。
                raise ServiceUnavailable(
                    f"interface {interface} unavailable: engine {serving} failed",
                    interface=interface,
                    causes=[exc],
                ) from exc
            if serving == route.control:
                raise ServiceUnavailable(
                    f"interface {interface} unavailable: engine {serving} failed",
                    interface=interface,
                    causes=[exc],
                ) from exc
            log.warning(
                "interface=%s engine=%s failed, falling back to %s: %s",
                interface,
                serving,
                route.control,
                exc,
            )
            try:
                result = self._run(
                    self._engines[route.control], interface, build_sql, params, max_rows
                )
            except EngineError as fallback_exc:
                # 回退引擎也失败：两个引擎的故障都要上报，不能让原始异常裸奔。
                raise ServiceUnavailable(
                    f"interface {interface} unavailable: engines {serving} and "
                    f"{route.control} both failed",
                    interface=interface,
                    causes=[exc, fallback_exc],
                ) from fallback_exc
            result.degraded = True
            result.fallback_from = serving
            result.failure_reason = str(exc)
            return result

    def _run(self, engine, interface: str, build_sql, params, max_rows):
        compiled = build_sql(engine.name)
        result = engine.query(
            compiled.sql, params=params, interface=interface, max_rows=max_rows
        )
        result.compiled = compiled
        result.interface = interface
        result.degraded = False
        result.fallback_from = None
        result.failure_reason = None
        return result

    def _shadow_compare(self, interface, route, build_sql, params, max_rows, serving_result):
        candidate = route.compare_engine()
        if not candidate or candidate not in self._engines:
            return
        try:
            candidate_result = self._run(
                self._engines[candidate], interface, build_sql, params, max_rows
            )
        except EngineError as exc:
            self._comparisons.record(
                interface,
                serving_result.engine,
                candidate,
                status="shadow_error",
                detail={"error": str(exc)},
                tolerance_pct=route.tolerance_pct,
            )
            return
        diff = compare_result_sets(serving_result, candidate_result)
        status = "match" if diff["within_tolerance"] else "mismatch"
        self._comparisons.record(
            interface,
            serving_result.engine,
            candidate,
            status=status,
            detail=diff,
            tolerance_pct=route.tolerance_pct,
        )
        if status == "mismatch":
            log.warning(
                "shadow mismatch interface=%s %s vs %s: %s",
                interface,
                serving_result.engine,
                candidate,
                diff,
            )

    # ------------------------------------------------------------------ 状态
    def status(self) -> dict[str, Any]:
        return {
            "routes": self._cutover.snapshot(),
            "engines": {name: e.status() for name, e in self._engines.items()},
            "recent_comparisons": self._comparisons.recent(20),
            "comparison_counts": self._comparisons.by_status(),
        }


class ComparisonLog:
    """最近 N 条影子对比结果，供差异排查与切流验收。"""

    def __init__(self, capacity: int = 500) -> None:
        self._items: list[dict[str, Any]] = []
        self._capacity = capacity
        self._lock = threading.Lock()

    def record(
        self, interface, left: str, right: str, *, status: str, detail, tolerance_pct: float
    ) -> None:
        item = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "interface": interface,
            "serving_engine": left,
            "compared_engine": right,
            "status": status,
            "tolerance_pct": tolerance_pct,
            "detail": detail,
        }
        with self._lock:
            self._items.append(item)
            if len(self._items) > self._capacity:
                self._items = self._items[-self._capacity :]

    def recent(self, n: int) -> list[dict[str, Any]]:
        with self._lock:
            return list(reversed(self._items[-n:]))

    def by_status(self) -> dict[str, int]:
        with self._lock:
            counts: dict[str, int] = {}
            for item in self._items:
                counts[item["status"]] = counts.get(item["status"], 0) + 1
            return counts


def _numeric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def compare_result_sets(left, right, *, relative_tolerance: float | None = None) -> dict[str, Any]:
    """比较两引擎结果：按维度/时间键逐行对齐。

    数值列用相对误差；维度值必须完全一致。行数不同或键错位都算结构差异。
    """
    compiled = getattr(left, "compiled", None)
    value_col = compiled.value_column if compiled else None
    dim_cols = compiled.dimension_columns if compiled else []
    time_alias = compiled.time_column_alias if compiled else None
    compare_keys = [*dim_cols, *([time_alias] if time_alias else [])]

    tolerance = relative_tolerance
    if tolerance is None:
        tolerance = 0.0

    if left.row_count != right.row_count:
        return {
            "within_tolerance": False,
            "reason": "row_count_mismatch",
            "left_rows": left.row_count,
            "right_rows": right.row_count,
            "max_relative_delta": None,
            "first_mismatch": None,
        }

    def key_of(row: tuple, names: list[str]) -> tuple:
        return tuple(row[names.index(c)] for c in compare_keys if c in names)

    mismatched = 0
    max_delta = 0.0
    first: dict[str, Any] | None = None
    for l_row, r_row in zip(left.rows, right.rows):
        l_key = key_of(l_row, left.columns)
        r_key = key_of(r_row, right.columns)
        if l_key != r_key:
            mismatched += 1
            first = first or {"kind": "key", "left": l_key, "right": r_key}
            continue
        if value_col and value_col in left.columns and value_col in right.columns:
            l_val = l_row[left.columns.index(value_col)]
            r_val = r_row[right.columns.index(value_col)]
            l_num = _numeric(l_val)
            r_num = _numeric(r_val)
            if l_num is None or r_num is None:
                if l_val != r_val:
                    mismatched += 1
                    first = first or {"kind": "null", "key": l_key, "left": l_val, "right": r_val}
                continue
            denom = max(abs(l_num), abs(r_num), 1e-9)
            delta = abs(l_num - r_num) / denom
            max_delta = max(max_delta, delta)
            if delta > tolerance:
                mismatched += 1
                if first is None:
                    first = {
                        "kind": "value",
                        "key": l_key,
                        "left": l_num,
                        "right": r_num,
                        "relative_delta": delta,
                    }
    return {
        "within_tolerance": mismatched == 0,
        "reason": None if mismatched == 0 else "value_or_key_mismatch",
        "rows_compared": left.row_count,
        "mismatched_rows": mismatched,
        "max_relative_delta": max_delta,
        "first_mismatch": first,
    }
