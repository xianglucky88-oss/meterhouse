"""配置。

所有可调参数都从环境变量读取，带安全默认值。迁移期最关键的三个开关：

    MH_CUTOVER_FILE      逐接口切流配置（默认 metrics/routes.yaml）
    CLICKHOUSE_HOST      列式引擎地址；未配置时 ClickHouse 路由不可用
    MH_ADMIN_TOKEN       切流管理接口的令牌；未配置时仅允许本机演示使用
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


@dataclass
class Settings:
    db_path: Path = field(default_factory=lambda: Path(_env("DB_PATH", "data/meterhouse.duckdb")))
    seed: int = field(default_factory=lambda: int(_env("SEED", "20261112")))
    # 业务时区。指标定义里可以按指标覆盖。
    biz_tz: str = field(default_factory=lambda: _env("BIZ_TZ", "Asia/Shanghai"))
    # UTC offset 仅保留给旧的 ground_truth 调用方（小时）。
    biz_tz_offset_hours: int = field(default_factory=lambda: int(_env("BIZ_TZ_OFFSET", "8")))
    metrics_file: Path = field(
        default_factory=lambda: Path(_env("METRICS_FILE", "metrics/metrics.yaml"))
    )
    cutover_file: Path = field(
        default_factory=lambda: Path(_env("MH_CUTOVER_FILE", "metrics/routes.yaml"))
    )
    # 管理接口令牌。设置后所有写操作接口必须带 X-Admin-Token。
    admin_token: str = field(default_factory=lambda: _env("MH_ADMIN_TOKEN", ""))

    # ---- ClickHouse ----
    ch_host: str = field(default_factory=lambda: _env("CLICKHOUSE_HOST", ""))
    ch_port: int = field(default_factory=lambda: _env_int("CLICKHOUSE_PORT", 8123))
    ch_user: str = field(default_factory=lambda: _env("CLICKHOUSE_USER", "default"))
    ch_password: str = field(default_factory=lambda: _env("CLICKHOUSE_PASSWORD", ""))
    ch_database: str = field(default_factory=lambda: _env("CLICKHOUSE_DATABASE", "meterhouse"))
    ch_timeout_s: float = field(default_factory=lambda: _env_float("CLICKHOUSE_TIMEOUT_S", 3.0))

    # ---- 容量 ----
    duckdb_max_concurrency: int = field(
        default_factory=lambda: _env_int("MH_DUCKDB_MAX_CONCURRENCY", 8)
    )
    clickhouse_max_concurrency: int = field(
        default_factory=lambda: _env_int("MH_CLICKHOUSE_MAX_CONCURRENCY", 16)
    )
    max_result_rows: int = field(default_factory=lambda: _env_int("MH_MAX_RESULT_ROWS", 50_000))
    acquire_timeout_s: float = field(default_factory=lambda: _env_float("MH_ACQUIRE_TIMEOUT_S", 2.0))

    @property
    def clickhouse_enabled(self) -> bool:
        return bool(self.ch_host)

    @property
    def clickhouse_url(self) -> str:
        return f"{self.ch_host}:{self.ch_port}" if self.ch_host else ""

    def ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
