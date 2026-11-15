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
    # 业务时区。stat_time/created_at 以 UTC 存储，归桶前按该偏移换算。
    biz_tz_offset_hours: int = field(default_factory=lambda: int(_env("BIZ_TZ_OFFSET", "8")))

    def ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
