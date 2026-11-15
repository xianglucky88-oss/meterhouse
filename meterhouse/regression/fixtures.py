"""可复现的数据夹具。

两类数据源：

1. ``fixtures/regression/datasets.yaml`` 里的显式行数据 —— 逐行可审查、可追溯，
   覆盖时区边界、空值、重复事实、乱序重放等形态；
2. ``generator: large_cardinality`` —— 参数 + 种子声明的确定性生成器，
   用于大基数抽样与并行场景。

所有数据都只通过显式种子生成；``Dataset.hash`` 覆盖 schema 与插入顺序，
基准清单里记录该哈希，数据一旦变化会被归因为 reference_drift。
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from .models import canonical_hash

SUPPLIER_COLUMNS = ("supplier_id", "stat_time", "is_active", "region", "category")
ORDER_COLUMNS = ("order_id", "supplier_id", "created_at", "amount_cents", "status", "region", "category", "ingest_seq")


def parse_utc(value: str) -> datetime:
    """把夹具时间解析为 UTC naive datetime（DuckDB TIMESTAMP 无时区）。"""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ("stat_time", "created_at"):
        if key in out and isinstance(out[key], str):
            out[key] = parse_utc(out[key])
    return out


@dataclass
class Dataset:
    name: str
    supplier_stats: list[dict[str, Any]] = field(default_factory=list)
    order_facts: list[dict[str, Any]] = field(default_factory=list)
    origin: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.supplier_stats = [_normalize_row(r) for r in self.supplier_stats]
        self.order_facts = [_normalize_row(r) for r in self.order_facts]
        for row in self.supplier_stats:
            missing = [c for c in SUPPLIER_COLUMNS if c not in row]
            if missing:
                raise ValueError(f"dataset {self.name}: supplier row missing {missing}")
        for row in self.order_facts:
            missing = [c for c in ORDER_COLUMNS if c not in row]
            if missing:
                raise ValueError(f"dataset {self.name}: order row missing {missing}")

    def hash(self) -> str:
        cached = getattr(self, "_hash", None)
        if cached:
            return cached
        payload = {
            "name": self.name,
            "origin": self.origin,
            "supplier_stats": [[_json_value(r[c]) for c in SUPPLIER_COLUMNS] for r in self.supplier_stats],
            "order_facts": [[_json_value(r[c]) for c in ORDER_COLUMNS] for r in self.order_facts],
        }
        digest = canonical_hash(payload)
        object.__setattr__(self, "_hash", digest)
        return digest

    def copy(self) -> "Dataset":
        return Dataset(
            name=self.name,
            supplier_stats=[dict(r) for r in self.supplier_stats],
            order_facts=[dict(r) for r in self.order_facts],
            origin=dict(self.origin),
        )

    def create_tables(self, con: Any) -> None:
        """在给定连接里重建两张明细表（DDL 由 data.connect 提供）。

        批量装载走临时 CSV + read_csv：DuckDB 的逐行 executemany 在万行级
        夹具上要几十秒，会让并行回归与最小化重放都不可接受。
        """
        con.execute("DELETE FROM supplier_stats")
        con.execute("DELETE FROM order_facts")
        if self.supplier_stats:
            _bulk_load(con, "supplier_stats", SUPPLIER_COLUMNS, self.supplier_stats)
        if self.order_facts:
            _bulk_load(con, "order_facts", ORDER_COLUMNS, self.order_facts)

    def rows(self, table: str) -> list[dict[str, Any]]:
        return self.supplier_stats if table == "supplier_stats" else self.order_facts

    def subset(self, supplier_stats: list[dict[str, Any]], order_facts: list[dict[str, Any]]) -> "Dataset":
        return Dataset(name=self.name, supplier_stats=supplier_stats, order_facts=order_facts, origin={**self.origin, "subset": True})


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _bulk_load(con: Any, table: str, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    """把行数据写成 CSV 再 COPY：比 executemany 快两个数量级。

    CSV 中的 NULL 用空字段 + nullstr 表达；时间统一用 ISO8601 UTC。
    """
    import csv
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        for row in rows:
            writer.writerow(["" if row[c] is None else _csv_value(row[c]) for c in columns])
        path = fh.name
    try:
        cols = ", ".join(columns)
        con.execute(
            f"COPY {table} ({cols}) FROM '{path}' (FORMAT CSV, HEADER FALSE, NULL '', TIMESTAMPFORMAT '%Y-%m-%dT%H:%M:%S')"
        )
    finally:
        import os

        os.unlink(path)


def _csv_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


@dataclass
class FixtureLibrary:
    datasets: dict[str, Dataset] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path | str) -> "FixtureLibrary":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        lib = cls()
        for item in raw.get("datasets", []):
            if "generator" in item:
                dataset = _generate(item)
            else:
                dataset = Dataset(
                    name=item["name"],
                    supplier_stats=list(item.get("supplier_stats", [])),
                    order_facts=list(item.get("order_facts", [])),
                    origin={"source": "inline"},
                )
            lib.datasets[dataset.name] = dataset
        for item in raw.get("derived", []):
            base = lib.datasets[item["from"]]
            lib.datasets[item["name"]] = _derive(item, base)
        return lib

    def get(self, name: str) -> Dataset:
        if name not in self.datasets:
            raise KeyError(f"unknown dataset: {name}")
        return self.datasets[name]

    def hashes(self) -> dict[str, str]:
        return {name: ds.hash() for name, ds in sorted(self.datasets.items())}


def _derive(item: dict[str, Any], base: Dataset) -> Dataset:
    """从显式数据集派生（重排插入顺序、按条件剔除行等）。

    派生规则本身是声明式的，因此结果完全可复现。
    """
    rules = item.get("rules", {})
    supplier_rows = [dict(r) for r in base.supplier_stats]
    order_rows = [dict(r) for r in base.order_facts]
    if rules.get("reverse_order_facts"):
        order_rows.reverse()
    if rules.get("reverse_supplier_stats"):
        supplier_rows.reverse()
    if rules.get("drop_category_nulls"):
        order_rows = [r for r in order_rows if r["category"] is not None]
    return Dataset(name=item["name"], supplier_stats=supplier_rows, order_facts=order_rows, origin={"source": "derived", "from": base.name, "rules": rules})


def _generate(item: dict[str, Any]) -> Dataset:
    generator = item["generator"]
    if generator != "large_cardinality":
        raise ValueError(f"unknown dataset generator: {generator}")
    params = item.get("params", {})
    seed_value = int(params.get("seed", 7))
    n_orders = int(params.get("orders", 20000))
    n_suppliers = int(params.get("suppliers", 2000))
    days = int(params.get("days", 30))
    regions = params.get("regions", ["华东", "华南", "华北", "西南"])
    categories = params.get("categories", ["电子元器件", "包装材料", "物流服务", "办公用品"])
    rnd = random.Random(seed_value)
    base = datetime(2026, 9, 1, 0, 0, 0)

    supplier_rows = []
    for s in range(1, n_suppliers + 1):
        sid = f"SUP-{s:06d}"
        # 用两个互质的步长让 region/category 独立组合，避免退化成嵌套分组。
        region = regions[(s * 7) % len(regions)]
        category = categories[(s * 11) % len(categories)]
        for d in range(days):
            day = base + timedelta(days=d)
            if rnd.random() < 0.62:
                supplier_rows.append(
                    {
                        "supplier_id": sid,
                        "stat_time": day.replace(hour=rnd.randint(0, 23), minute=rnd.randint(0, 59)),
                        "is_active": True,
                        "region": region,
                        "category": category,
                    }
                )
    order_rows = []
    seq = 0
    for i in range(1, n_orders + 1):
        day = base + timedelta(days=rnd.randint(0, days - 1))
        hour = rnd.randint(16, 23) if rnd.random() < 0.1 else rnd.randint(0, 15)
        seq += 1
        status = "paid" if rnd.random() < 0.78 else "refunded"
        row = {
            "order_id": f"ORD-{i:08d}",
            "supplier_id": f"SUP-{rnd.randint(1, n_suppliers):06d}",
            "created_at": day.replace(hour=hour, minute=rnd.randint(0, 59)),
            "amount_cents": rnd.randint(100, 500000),
            "status": status,
            "region": regions[rnd.randint(0, len(regions) - 1)],
            "category": None if rnd.random() < 0.03 else categories[rnd.randint(0, len(categories) - 1)],
            "ingest_seq": seq,
        }
        order_rows.append(row)
        if rnd.random() < 0.015:
            seq += 1
            dup = dict(row)
            dup["amount_cents"] = row["amount_cents"] + rnd.randint(1, 5000)
            dup["ingest_seq"] = seq
            order_rows.append(dup)
    return Dataset(
        name=item["name"],
        supplier_stats=supplier_rows,
        order_facts=order_rows,
        origin={"source": "generator", "generator": generator, "params": params},
    )
