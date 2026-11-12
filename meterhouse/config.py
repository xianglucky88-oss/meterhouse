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

    def ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
