"""配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class Settings:
    db_path: Path = field(default_factory=lambda: Path(_env("DB_PATH", "data/meterhouse.duckdb")))
    seed: int = field(default_factory=lambda: int(_env("SEED", "20261112")))
    # 业务时区。ClickHouse 的 toDate 依赖服务端时区，这是口径差异的来源之一。
    biz_tz_offset_hours: int = field(default_factory=lambda: int(_env("BIZ_TZ_OFFSET", "8")))
    # 是否启用语义层（实验开关）。默认关闭，接口仍走手写 SQL。
    use_semantic_layer: bool = field(default_factory=lambda: _env("USE_SEMANTIC_LAYER", "0") == "1")

    # ------------------------------------------------------------ 审计导出
    # 任务、快照与交付物的根目录（独立于数据库文件，便于单独挂载/回收）
    audit_root: Path = field(default_factory=lambda: Path(_env("AUDIT_ROOT", "data/audit")))
    # 快照 TTL：过期后任务显式失败，要求重新发起，避免交付陈旧视图
    audit_snapshot_ttl_seconds: int = field(
        default_factory=lambda: int(_env("AUDIT_SNAPSHOT_TTL_SECONDS", str(24 * 3600)))
    )
    # 每个分段的目标行数（决定续传粒度，不决定内存占用）
    audit_segment_size: int = field(
        default_factory=lambda: int(_env("AUDIT_SEGMENT_SIZE", "5000"))
    )
    # 单次从快照拉取的批大小：导出侧内存上限只与该值相关，与总结果集无关
    audit_fetch_batch_size: int = field(
        default_factory=lambda: int(_env("AUDIT_FETCH_BATCH_SIZE", "1000"))
    )
    # 单段扫描失败的有界重试次数
    audit_max_retries: int = field(
        default_factory=lambda: int(_env("AUDIT_MAX_RETRIES", "3"))
    )
    # 心跳落盘间隔（秒），供崩溃恢复判断任务活性
    audit_heartbeat_seconds: float = field(
        default_factory=lambda: float(_env("AUDIT_HEARTBEAT_SECONDS", "5"))
    )
    # 导出专用 DuckDB 连接的资源上限，避免与交互查询争抢
    audit_duckdb_memory_limit: str = field(
        default_factory=lambda: _env("AUDIT_DUCKDB_MEMORY_LIMIT", "256MB")
    )
    audit_duckdb_threads: int = field(
        default_factory=lambda: int(_env("AUDIT_DUCKDB_THREADS", "1"))
    )
    # 清单与快照 schema 版本，目录/编码逻辑不兼容升级时递增以使旧快照失效
    audit_manifest_version: int = field(default_factory=lambda: 1)
    audit_schema_version: int = field(default_factory=lambda: 1)

    def ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.audit_root.mkdir(parents=True, exist_ok=True)


settings = Settings()
