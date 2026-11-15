"""候选执行器。

候选 = 被测实现（语义层编译、手写 SQL、抽样执行……）。参考口径永远来自
``oracle``，候选只负责“给出数字”，两者不共享求值代码。

新增候选实现只需实现 ``Candidate.execute`` 并注册到 ``CANDIDATES``；
场景文件不需要知道候选细节。
"""

from __future__ import annotations

import atexit
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from meterhouse.data import connect
from meterhouse.queries import QUERIES
from meterhouse.semantic import MetricStore

from .fixtures import Dataset
from .models import Query


def _cleanup(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


class Candidate(Protocol):
    name: str

    def supports(self, query: Query) -> bool:
        ...

    def execute(self, query: Query, dataset: Dataset, store: MetricStore) -> list[dict[str, Any]]:
        ...


@dataclass
class SemanticCandidate:
    """通过语义层编译器取数——生产路径的候选实现。

    同一数据集在场景内复用同一个临时库，避免每次查询都重建夹具
    （抽样场景会对同一数据集发起上百次点查询）。
    """

    name: str = "semantic"
    _cache: dict[str, tuple[Any, str]] = field(default_factory=dict, init=False, repr=False)

    def supports(self, query: Query) -> bool:
        return True

    def _connection(self, dataset: Dataset) -> Any:
        key = f"{dataset.name}:{dataset.hash()}"
        cached = self._cache.get(key)
        if cached is not None:
            con, _ = cached
            if con.execute("SELECT 1").fetchone() is not None:
                return con
        tmp = tempfile.mkdtemp(prefix="meterhouse-cand-")
        con = connect(str(Path(tmp) / "candidate.duckdb"))
        dataset.create_tables(con)
        self._cache[key] = (con, tmp)
        atexit.register(_cleanup, tmp)
        return con

    def execute(self, query: Query, dataset: Dataset, store: MetricStore) -> list[dict[str, Any]]:
        sql = store.compile(
            query.metric,
            dimensions=list(query.dimensions),
            granularity=query.granularity,
            filters=[{"column": c, "op": o, "value": v} for c, o, v in query.filters],
            time_range=({"start": query.time_range[0], "end": query.time_range[1]} if query.time_range else None),
            fill_missing=query.fill_missing,
        )
        cur = self._connection(dataset).execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


@dataclass
class LegacyCandidate:
    """改造前的手写 SQL 候选。

    用于证明回归体系能独立发现生产实现与参考口径的偏差；默认不参与门禁。
    它只能回答“裸指标”查询，超出范围的查询记为 unsupported（跳过而不是误报）。
    """

    name: str = "legacy"

    # 指标 -> (SQL, 输出列到指标名的映射)
    MAPPING = {
        "active_suppliers": (QUERIES["active_suppliers_a"], "active_suppliers"),
        "gmv_cents": (QUERIES["gmv_cents"], "gmv_cents"),
        "orders_total": (QUERIES["orders_total"], "orders_total"),
        "paid_rate": (QUERIES["conversion_rate"], "paid_rate"),
        "avg_order_cents": (QUERIES["avg_order_cents"], "avg_order_cents"),
    }

    @staticmethod
    def supports(query: Query) -> bool:
        return (
            query.metric in LegacyCandidate.MAPPING
            and not query.dimensions
            and not query.granularity
            and not query.filters
            and not query.time_range
            and not query.fill_missing
        )

    def execute(self, query: Query, dataset: Dataset, store: MetricStore) -> list[dict[str, Any]]:
        if not self.supports(query):
            raise NotImplementedError("legacy candidate only supports the bare metric query")
        sql, output_column = self.MAPPING[query.metric]
        with tempfile.TemporaryDirectory(prefix="meterhouse-legacy-") as tmp:
            con = connect(str(Path(tmp) / "legacy.duckdb"))
            try:
                dataset.create_tables(con)
                cur = con.execute(sql)
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                # 手写 SQL 的输出列名不一定等于指标名，归一化后交给比较层。
                return [{query.metric: row[output_column]} for row in rows]
            finally:
                con.close()


CANDIDATES: dict[str, Candidate] = {
    "semantic": SemanticCandidate(),
    "legacy": LegacyCandidate(),
}


def get_candidate(name: str) -> Candidate:
    if name not in CANDIDATES:
        raise KeyError(f"unknown candidate: {name}")
    return CANDIDATES[name]
