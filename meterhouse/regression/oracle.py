"""独立参考实现（oracle）。

它读取指标定义，直接在 Python 里对夹具行数据求值，**不执行任何生产 SQL**。
用途有两个：

* 审批基准时计算期望值；
* 每次回归都重算一遍，与已审批基准对照，用来发现“基准/定义/夹具”本身的漂移。

如果 oracle 与生产编译器共用实现，回归就失去意义；因此这里刻意不复用
``semantic.MetricStore.compile`` 与其执行结果，只复用已校验的定义对象。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from meterhouse.semantic import Filter, Measure, Metric, MetricStore

from .models import Query


def _match(row: dict[str, Any], flt: Filter) -> bool:
    value = row.get(flt.column)
    op = flt.op
    if op == "is_null":
        return value is None
    if op == "not_null":
        return value is not None
    if value is None:
        # SQL 三值逻辑：除 IS [NOT] NULL 外，NULL 不满足任何比较。
        return False
    if op == "eq":
        return value == flt.value
    if op == "ne":
        return value != flt.value
    if op == "gt":
        return value > flt.value
    if op == "gte":
        return value >= flt.value
    if op == "lt":
        return value < flt.value
    if op == "lte":
        return value <= flt.value
    if op == "in":
        return value in flt.value
    if op == "not_in":
        return value not in flt.value
    raise ValueError(f"unsupported op {op}")


def _apply_filters(rows: Iterable[dict[str, Any]], filters: Iterable[Filter]) -> list[dict[str, Any]]:
    return [row for row in rows if all(_match(row, f) for f in filters)]


def _rank_key(row: dict[str, Any], measure: Measure) -> tuple:
    """max_by 去重的比较键（升序取最大）。

    语义：目标列降序、NULL 最后，tiebreak 同序。转成升序比较键后：
    非空值用 (1, value)（值越大键越大），NULL 用 (0, 0)（永远最小、排在最后）。
    """
    assert measure.dedup and measure.dedup.column
    cols = [measure.dedup.column] + list(measure.dedup.tiebreak)
    key = []
    for col in cols:
        value = row.get(col)
        if value is None:
            key.append((0, 0))
        else:
            key.append((1, value))
    return tuple(key)


def _dedup_rows(rows: list[dict[str, Any]], measure: Measure) -> list[dict[str, Any]]:
    dedup = measure.dedup
    if dedup is None:
        return rows
    if dedup.policy != "max_by":
        raise ValueError(f"unsupported dedup policy {dedup.policy}")
    best: dict[tuple, dict[str, Any]] = {}
    for row in rows:
        key = tuple(row.get(k) for k in dedup.keys)
        current = best.get(key)
        if current is None:
            best[key] = row
            continue
        if _rank_key(row, measure) > _rank_key(current, measure):
            best[key] = row
    return list(best.values())


def _aggregate(rows: list[dict[str, Any]], measure: Measure) -> int | float | None:
    agg = measure.agg
    if agg == "count":
        return len(rows)
    if not measure.column:
        raise ValueError(f"{agg} requires column")
    values = [r.get(measure.column) for r in rows if r.get(measure.column) is not None]
    if agg == "count_distinct":
        return len(set(values))
    if agg == "sum":
        return sum(values) if values else None
    if agg == "avg":
        return sum(values) / len(values) if values else None
    if agg == "min":
        return min(values) if values else None
    if agg == "max":
        return max(values) if values else None
    raise ValueError(f"unsupported aggregate {agg}")


def _bucket(day: date, granularity: str) -> date:
    if granularity == "day":
        return day
    if granularity == "week":
        return day - timedelta(days=day.weekday())
    if granularity == "month":
        return day.replace(day=1)
    raise ValueError(f"unsupported granularity {granularity}")


def _calendar(start: str, end: str, granularity: str) -> list[date]:
    """[start, end) 内的桶，周一对齐、月初对齐。"""
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    cursor = _bucket(start_d, granularity)
    last = _bucket(end_d - timedelta(days=1), granularity)
    out = []
    while cursor <= last:
        out.append(cursor)
        if granularity == "day":
            cursor += timedelta(days=1)
        elif granularity == "week":
            cursor += timedelta(days=7)
        else:
            cursor = date(cursor.year + (cursor.month // 12), cursor.month % 12 + 1, 1)
    return out


@dataclass
class Oracle:
    store: MetricStore
    tz_offset_hours: int = 0

    @classmethod
    def from_store(cls, store: MetricStore) -> "Oracle":
        return cls(store=store, tz_offset_hours=store.tz_offset_hours)

    # ----------------------------------------------------------- 行级准备

    def _prepared(self, metric: Metric, rows: list[dict[str, Any]], query: Query) -> list[dict[str, Any]]:
        filters = list(metric.filters) + [Filter(column=c, op=o, value=v) for c, o, v in query.filters]
        rows = _apply_filters(rows, filters)
        if query.time_range:
            assert metric.time_column
            start_d = date.fromisoformat(query.time_range[0])
            end_d = date.fromisoformat(query.time_range[1])
            rows = [
                r for r in rows
                if start_d <= _local_date(r, metric.time_column, self.tz_offset_hours) < end_d
            ]
        return rows

    def _group(self, metric: Metric, measure: Measure, rows: list[dict[str, Any]], query: Query) -> dict[tuple, list[dict[str, Any]]]:
        filtered = _apply_filters(rows, measure.filters)
        deduped = _dedup_rows(filtered, measure)
        groups: dict[tuple, list[dict[str, Any]]] = {}
        if not query.dimensions and not query.granularity:
            groups[()] = deduped
            return groups
        for row in deduped:
            key: list[Any] = [row.get(d) for d in query.dimensions]
            if query.granularity:
                assert metric.time_column
                day = _local_date(row, metric.time_column, self.tz_offset_hours)
                key.append(_bucket(day, query.granularity))
            groups.setdefault(tuple(key), []).append(row)
        return groups

    # ------------------------------------------------------------- 求值

    def evaluate(self, query: Query, dataset: Any) -> list[dict[str, Any]]:
        metric = self.store.get(query.metric)
        rows = self._prepared(metric, dataset.rows(metric.source), query)

        if metric.kind == "ratio":
            assert metric.numerator is not None and metric.denominator is not None
            den_groups = self._group(metric, metric.denominator, rows, query)
            num_groups = self._group(metric, metric.numerator, rows, query)
            result = []
            for key, den_rows in den_groups.items():
                num_rows = num_groups.get(key, [])
                den = _aggregate(den_rows, metric.denominator)
                num = _aggregate(num_rows, metric.numerator)
                value = metric.empty_value if not den else (num or 0) / den
                result.append(self._row(query, key, value))
            if not den_groups and not query.dimensions and not query.granularity:
                # 无分组聚合恒返回一行；有分组时空结果就是零行。
                result = [self._row(query, (), metric.empty_value)]
        else:
            assert metric.measure is not None
            groups = self._group(metric, metric.measure, rows, query)
            result = []
            for key, group_rows in groups.items():
                value = _aggregate(group_rows, metric.measure)
                if value is None:
                    value = metric.empty_value
                result.append(self._row(query, key, value))
            if not groups and not query.dimensions and not query.granularity:
                result = [self._row(query, (), metric.empty_value)]

        result.sort(key=lambda r: self._sort_key(query, r))
        if query.fill_missing:
            result = self._fill(result, metric, query)
        return result

    @staticmethod
    def _row(query: Query, key: tuple, value: Any) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for i, dim in enumerate(query.dimensions):
            out[dim] = key[i]
        if query.granularity:
            out["date"] = key[len(query.dimensions)]
        out[query.metric] = value
        return out

    @staticmethod
    def _sort_key(query: Query, row: dict[str, Any]) -> tuple:
        cols = list(query.dimensions) + (["date"] if query.granularity else [])
        return tuple((row[c] is None, row[c] if row[c] is not None else "") for c in cols)

    def _fill(self, result: list[dict[str, Any]], metric: Metric, query: Query) -> list[dict[str, Any]]:
        assert query.time_range and query.granularity
        by_date = {row["date"]: row for row in result}
        out = []
        for bucket in _calendar(query.time_range[0], query.time_range[1], query.granularity):
            row = by_date.get(bucket)
            out.append(row if row is not None else {"date": bucket, query.metric: metric.empty_value})
        return out


def _local_date(row: dict[str, Any], column: str, offset_hours: int) -> date:
    value: datetime = row[column]
    return (value + timedelta(hours=offset_hours)).date()
