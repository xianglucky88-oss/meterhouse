"""指标查询编译器：把指标语义 + 查询请求编译为参数化 SQL 计划。

设计约束（对应口径事故的每一类根因）：

1. 标识符与值严格分离
   表名、列名、指标名、维度名只能来自 YAML 白名单，并经过标识符语法校验；
   请求里的过滤值、时间边界、limit 永远作为 DuckDB 绑定参数（?）出现，
   不经过字符串拼接，因此不存在通过参数注入 SQL 的路径。

2. 去重 -> 过滤 -> 聚合的顺序固定
   来源（source）级 dedup 规格先在 CTE 中完成去重，指标过滤只作用在
   去重之后的行上；作者无法在单个接口里绕过这个顺序。

3. 比率指标拆成显式分子/分母
   ratio 类指标分别编译两个聚合表达式，上卷时在目标窗口整体重算，
   编译器不提供"对比率结果再平均/相加"的路径。

4. 可加性是编译期约束
   对 non_additive 指标做跨组上卷时，compile_rollup 会显式拒绝；
   正确路径是用更大的窗口重新编译一条查询（同环比也走这条路径）。

5. 所有拒绝都抛 SemanticError 子类，由 API 层统一映射为 4xx，
   保证"看起来成功但口径错误"的响应发不出去。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

# 当前编译器支持的语义 schema 主版本。
SUPPORTED_SCHEMA_VERSIONS = (2,)

# 标识符只允许字母/数字/下划线，且必须以字母或下划线开头。
# 所有进入 SQL 的标识符（表、列、别名）都要过这道校验。
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# 受支持的时间粒度与对应 trunc 单位。
GRANULARITIES: dict[str, str] = {
    "hour": "hour",
    "day": "day",
    "week": "week",
    "month": "month",
}

# 受支持的过滤算子。值的数量/类型由算子决定，全部走绑定参数。
_OPS_NO_VALUE = frozenset({"is null", "is not null"})
_OPS_SINGLE = frozenset({"=", "!=", "<>", "<", "<=", ">", ">=", "like"})
_OPS_MULTI = frozenset({"in", "not in"})

# 受支持的聚合函数白名单（measure / ratio 组件用）。
_AGG_FUNCS = frozenset({"sum", "avg", "min", "max", "count"})

# limit 防呆范围。
LIMIT_MIN, LIMIT_MAX = 1, 10_000


class SemanticError(Exception):
    """语义层错误基类。"""


class UnknownMetricError(SemanticError, KeyError):
    """指标不存在。同时是 KeyError，兼容历史调用方的 except KeyError。"""


class UnknownDimensionError(SemanticError, ValueError):
    """维度不在指标/来源白名单内。同时是 ValueError，兼容历史断言。"""


class InvalidRequestError(SemanticError, ValueError):
    """请求不合法：粒度、算子、类型、limit、时间范围等。"""


class UnsupportedSchemaError(SemanticError):
    """YAML schema 版本不受支持。"""


def _ident(token: str, what: str) -> str:
    """校验标识符并原样返回；不合法直接拒绝。"""
    if not isinstance(token, str) or not _IDENT_RE.match(token):
        raise InvalidRequestError(f"非法{what}标识符: {token!r}")
    return token


# --------------------------------------------------------------------------- 规格对象


@dataclass(frozen=True)
class AggSpec:
    func: str
    expr: str  # 列名或受信任表达式（仅来自 YAML）


@dataclass(frozen=True)
class Filter:
    """结构化过滤条件。column/op 受白名单约束，value 走绑定参数。"""

    column: str
    op: str
    value: Any = None

    def is_parametric(self) -> bool:
        return self.op not in _OPS_NO_VALUE


@dataclass(frozen=True)
class RatioComponent:
    """比率指标的分子/分母组件：一个聚合 + 可选过滤。"""

    agg: AggSpec | None = None
    distinct: str | None = None
    filters: tuple[Filter, ...] = ()


@dataclass(frozen=True)
class SourceSpec:
    name: str
    table: str
    time_column: str
    columns: tuple[str, ...]
    dimensions: tuple[str, ...]
    dimension_types: dict[str, str]
    dedup_strategy: str  # distinct | none
    biz_offset_hours: int


@dataclass(frozen=True)
class MetricSpec:
    name: str
    label: str
    description: str
    kind: str  # measure | distinct | ratio
    source: str
    additivity: str  # additive | semi_additive | non_additive
    time_column: str
    dimensions: tuple[str, ...]
    filters: tuple[Filter, ...]
    valid_from: int
    agg: AggSpec | None = None
    distinct: str | None = None
    numerator: RatioComponent | None = None
    denominator: RatioComponent | None = None

    @property
    def is_ratio(self) -> bool:
        return self.kind == "ratio"


@dataclass(frozen=True)
class Catalog:
    """不可变的语义目录：来源 + 指标。"""

    schema_version: int
    biz_offset_hours: int
    sources: dict[str, SourceSpec]
    metrics: dict[str, MetricSpec]

    def get_metric(self, name: str) -> MetricSpec:
        if name not in self.metrics:
            raise UnknownMetricError(f"unknown metric: {name}")
        return self.metrics[name]

    def get_source(self, name: str) -> SourceSpec:
        if name not in self.sources:
            raise InvalidRequestError(f"unknown source: {name}")
        return self.sources[name]

    @property
    def compiler(self) -> "Compiler":
        """便捷访问：编译器无状态，每次构造等价且廉价。"""
        return Compiler(self)


# --------------------------------------------------------------------------- 请求


@dataclass(frozen=True)
class QueryRequest:
    """一次指标查询的完整语义请求。"""

    metric: str
    dimensions: tuple[str, ...] = ()
    filters: tuple[Filter, ...] = ()
    granularity: str | None = None
    start: datetime | None = None  # 含，UTC
    end: datetime | None = None  # 不含，UTC
    limit: int | None = None
    compare: str | None = None  # previous_period | previous_year
    offset_hours: int | None = None  # 覆盖目录默认业务时区

    @staticmethod
    def build(
        metric: str,
        *,
        dimensions: list[str] | tuple[str, ...] | None = None,
        filters: list[Filter] | tuple[Filter, ...] | None = None,
        granularity: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        compare: str | None = None,
        offset_hours: int | None = None,
    ) -> "QueryRequest":
        return QueryRequest(
            metric=metric,
            dimensions=tuple(dimensions or ()),
            filters=tuple(filters or ()),
            granularity=granularity,
            start=start,
            end=end,
            limit=limit,
            compare=compare,
            offset_hours=offset_hours,
        )


@dataclass(frozen=True)
class CompiledQuery:
    """一条可执行的参数化 SQL。"""

    sql: str
    params: tuple[Any, ...]
    level: str  # base | compare
    request: QueryRequest
    denominator_column: str | None
    # 窗口存在性查询：判断时间/过滤窗口内是否存在原始行，
    # 用于区分"空窗口"与"聚合零值"（如 count 对空集返回 0）。
    presence_sql: str | None = None
    presence_params: tuple[Any, ...] = ()


@dataclass(frozen=True)
class MetricPlan:
    """一次请求编译出的完整计划（主查询 + 可选同环比查询）。"""

    metric: MetricSpec
    request: QueryRequest
    base: CompiledQuery
    comparisons: tuple[CompiledQuery, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def sql(self) -> str:
        """主查询 SQL（兼容历史调用方）。"""
        return self.base.sql

    def rendered_sql(self, level: str = "base") -> str:
        """调试用：把参数以内联字面量渲染出来（仅用于日志/响应展示，绝不执行）。"""
        target = self.base if level == "base" else next(c for c in self.comparisons if c.level == level)
        return _render_inline(target.sql, target.params)


# --------------------------------------------------------------------------- 编译器


class Compiler:
    """无状态编译器：Catalog + QueryRequest -> MetricPlan。"""

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    # ----- 对外入口 -----

    def compile(self, request: QueryRequest) -> MetricPlan:
        metric = self._validate_request(request)
        base = self._compile_query(metric, request, level="base")
        comparisons: list[CompiledQuery] = []
        warnings: list[str] = []
        if request.compare:
            shifted = self._shift_window(request)
            comparisons.append(self._compile_query(metric, shifted, level=request.compare))
        if metric.is_ratio:
            warnings.append(
                f"{metric.name} 为比率指标：结果不可相加/平均，上卷必须用更大窗口重新编译"
            )
        if metric.additivity == "non_additive" and (request.dimensions or request.granularity):
            warnings.append(
                f"{metric.name} 标记为 non_additive：禁止对分组结果求和，"
                "总量请用无维度查询重新计算"
            )
        return MetricPlan(
            metric=metric,
            request=request,
            base=base,
            comparisons=tuple(comparisons),
            warnings=tuple(warnings),
        )

    def compile_rollup(self, metric_name: str, rows: list[dict[str, Any]]) -> Any:
        """尝试把分组行上卷为总量。

        additive 指标允许求和；semi/non_additive 一律显式拒绝——
        调用方应改用无维度 QueryRequest 重新编译一条总量查询。
        """
        metric = self.catalog.get_metric(metric_name)
        if metric.is_ratio:
            raise InvalidRequestError(
                f"指标 {metric_name} 是比率，不能对比率结果求和或平均；"
                "请用无维度请求重新编译，在目标窗口内重算分子分母"
            )
        if metric.additivity != "additive":
            raise InvalidRequestError(
                f"指标 {metric_name} 的可加性为 {metric.additivity}，"
                "禁止跨组相加；请用无维度请求重新编译（count distinct 在更大窗口重算）"
            )
        if not rows:
            return 0
        return sum(r[metric_name] or 0 for r in rows)

    # ----- 请求校验 -----

    def _validate_request(self, request: QueryRequest) -> MetricSpec:
        metric = self.catalog.get_metric(request.metric)
        source = self.catalog.get_source(metric.source)

        if request.granularity is not None:
            if request.granularity not in GRANULARITIES:
                raise InvalidRequestError(
                    f"不支持的时间粒度 {request.granularity!r}，"
                    f"可选: {sorted(GRANULARITIES)}"
                )
        if request.compare is not None and request.compare not in ("previous_period", "previous_year"):
            raise InvalidRequestError(
                f"不支持的比较范围 {request.compare!r}，"
                "可选: previous_period | previous_year"
            )
        if request.start is not None and request.end is not None and request.end <= request.start:
            raise InvalidRequestError("时间范围非法：end 必须晚于 start（半开区间 [start, end)）")
        if request.limit is not None and not (LIMIT_MIN <= request.limit <= LIMIT_MAX):
            raise InvalidRequestError(f"limit 必须在 [{LIMIT_MIN}, {LIMIT_MAX}] 之间")
        offset = request.offset_hours
        if offset is None:
            offset = source.biz_offset_hours
        if not isinstance(offset, int) or not (-23 <= offset <= 23):
            raise InvalidRequestError(f"业务时区偏移非法: {offset!r}，要求 -23..23 的整数小时")

        # 分组维度必须在指标声明的白名单内。
        bad = [d for d in request.dimensions if d not in metric.dimensions]
        if bad:
            raise UnknownDimensionError(
                f"metric {metric.name} 不支持维度 {bad}，已声明维度: {list(metric.dimensions)}"
            )
        # 过滤列必须在来源注册过（指标过滤 + 请求过滤都受这层约束）。
        for flt in (*metric.filters, *request.filters):
            self._validate_filter(flt, source)
        return metric

    def _validate_filter(self, flt: Filter, source: SourceSpec) -> None:
        # 受信任原生表达式：只能由 YAML 解析构造，请求路径的算子白名单会挡掉它。
        if flt.op == "__raw__":
            return
        if flt.column not in source.columns:
            raise UnknownDimensionError(
                f"来源 {source.name} 不存在过滤列 {flt.column!r}，"
                f"已注册列: {list(source.columns)}"
            )
        op = flt.op.lower()
        if op in _OPS_NO_VALUE:
            if flt.value is not None:
                raise InvalidRequestError(f"算子 {op} 不接受值")
            return
        if op in _OPS_SINGLE:
            self._check_type(flt, source, allow_list=False)
            return
        if op in _OPS_MULTI:
            self._check_type(flt, source, allow_list=True)
            return
        raise InvalidRequestError(
            f"不支持的过滤算子 {flt.op!r}，可选: "
            f"{sorted(_OPS_NO_VALUE | _OPS_SINGLE | _OPS_MULTI)}"
        )

    def _check_type(self, flt: Filter, source: SourceSpec, *, allow_list: bool) -> None:
        col_type = source.dimension_types.get(flt.column, "string")
        values = flt.value if allow_list else [flt.value]
        if allow_list:
            if not isinstance(flt.value, (list, tuple)) or not flt.value:
                raise InvalidRequestError(f"算子 {flt.op} 需要非空列表作为值")
            values = list(flt.value)
        expected: tuple[type, ...]
        if col_type == "number":
            expected = (int, float)
        elif col_type == "boolean":
            expected = (bool,)
        else:
            expected = (str, int, float, bool)
        for v in values:
            if not isinstance(v, expected):
                raise InvalidRequestError(
                    f"过滤 {flt.column} 的值 {v!r} 与声明类型 {col_type} 不匹配"
                )
            if isinstance(v, bool) and col_type != "boolean":
                raise InvalidRequestError(
                    f"过滤 {flt.column} 的值 {v!r} 与声明类型 {col_type} 不匹配"
                )

    # ----- 同环比：平移时间窗口，指标在新窗口整体重算（对非可加指标同样安全） -----

    def _shift_window(self, request: QueryRequest) -> QueryRequest:
        if request.start is None or request.end is None:
            raise InvalidRequestError(
                f"比较范围 {request.compare} 需要同时提供 start/end，"
                "否则无法对齐窗口；禁止对无界序列做逐行比较"
            )
        span = request.end - request.start
        if request.compare == "previous_period":
            delta = -span
        else:  # previous_year
            delta = timedelta(days=-365)
        return QueryRequest(
            metric=request.metric,
            dimensions=request.dimensions,
            filters=request.filters,
            granularity=request.granularity,
            start=request.start + delta,
            end=request.end + delta,
            limit=request.limit,
            compare=None,  # 比较查询本身不再嵌套比较
            offset_hours=request.offset_hours,
        )

    # ----- SQL 生成 -----

    def _compile_query(
        self, metric: MetricSpec, request: QueryRequest, *, level: str
    ) -> CompiledQuery:
        source = self.catalog.get_source(metric.source)
        offset = request.offset_hours if request.offset_hours is not None else source.biz_offset_hours
        params: list[Any] = []

        # 1) 去重 CTE：顺序固定为 去重 -> 过滤 -> 聚合。
        if source.dedup_strategy == "distinct":
            cte = f"WITH src_dedup AS (SELECT DISTINCT * FROM {source.table})"
            from_clause = "src_dedup"
        elif source.dedup_strategy == "none":
            cte = ""
            from_clause = source.table
        else:  # pragma: no cover - 加载期已校验
            raise InvalidRequestError(f"未知去重策略: {source.dedup_strategy}")

        # 2) SELECT 输出列：时间桶 / 维度 / 指标值（比率附带隐藏分母列）。
        out_cols: list[str] = []
        group_exprs: list[str] = []
        if request.granularity:
            bucket = self._time_bucket(source.time_column, request.granularity, offset)
            out_cols.append(f"{bucket} AS time_bucket")
            group_exprs.append(bucket)
        for dim in request.dimensions:
            _ident(dim, "维度")
            out_cols.append(dim)
            group_exprs.append(dim)

        value_sql, denom_sql = self._metric_expr(metric, params)
        denom_col = f"_denom_{metric.name}" if denom_sql is not None else None
        if denom_sql is not None:
            # 分母隐藏列先输出，值表达式通过该列别名引用，分母只计算/绑定一次。
            out_cols.append(f"{denom_sql} AS {denom_col}")
            out_cols.append(f"{value_sql.format(col=denom_col)} AS {metric.name}")
        else:
            out_cols.append(f"{value_sql} AS {metric.name}")

        # 3) WHERE：作者级指标过滤 + 请求级过滤 + 时间窗口，全部参数化。
        #    单独收集一份参数，供存在性查询复用（聚合查询可能还有额外的 limit 参数）。
        where_parts, where_params = self._build_where(metric, source, request)
        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
        params.extend(where_params)

        # 4) GROUP BY / ORDER BY / LIMIT。
        group_sql = ""
        if group_exprs:
            # 用序号位置分组，避免重复表达式；ORDER BY 与输出列顺序一致。
            positions = ", ".join(str(i + 1) for i in range(len(group_exprs)))
            group_sql = f" GROUP BY {positions} ORDER BY {positions}"
        limit_sql = ""
        if request.limit is not None:
            limit_sql = " LIMIT ?"
            params.append(int(request.limit))

        sql = (
            f"{cte} SELECT {', '.join(out_cols)} "
            f"FROM {from_clause}{where_sql}{group_sql}{limit_sql}"
        )
        # 存在性查询与主查询共享 FROM/WHERE 及其绑定参数，只把选择列换成 count(*)。
        presence_sql = " ".join(
            f"{cte} SELECT count(*) AS _present FROM {from_clause}{where_sql}".split()
        )
        return CompiledQuery(
            sql=" ".join(sql.split()),
            params=tuple(params),
            level=level,
            request=request,
            denominator_column=denom_col,
            presence_sql=presence_sql,
            presence_params=tuple(where_params),
        )

    def _build_where(
        self, metric: MetricSpec, source: SourceSpec, request: QueryRequest
    ) -> tuple[list[str], list[Any]]:
        parts: list[str] = []
        params: list[Any] = []
        for flt in metric.filters:
            parts.append(self._render_filter(flt, params))
        for flt in request.filters:
            parts.append(self._render_filter(flt, params))
        if request.start is not None:
            parts.append(f"{source.time_column} >= ?")
            params.append(request.start)
        if request.end is not None:
            parts.append(f"{source.time_column} < ?")
            params.append(request.end)
        return parts, params

    def _time_bucket(self, time_column: str, granularity: str, offset_hours: int) -> str:
        """UTC 时间列先按业务时区偏移再归桶，偏移量是受校验的整数，不做参数占位。"""
        _ident(time_column, "时间列")
        unit = GRANULARITIES[granularity]
        shifted = f"({time_column} + INTERVAL {int(offset_hours)} HOUR)"
        return f"date_trunc('{unit}', {shifted})"

    def _metric_expr(self, metric: MetricSpec, params: list[Any]) -> tuple[str, str | None]:
        if metric.kind == "measure":
            assert metric.agg is not None
            return self._agg_expr(metric.agg, extra_filters=(), params=params), None
        if metric.kind == "distinct":
            _ident(metric.distinct, "去重列")
            return f"count(DISTINCT {metric.distinct})", None
        if metric.kind == "ratio":
            assert metric.numerator and metric.denominator
            num = self._component_expr(metric.numerator, params)
            den = self._component_expr(metric.denominator, params)
            # 分母只计算一次并作为隐藏列输出（列名在 _compile_query 中确定）；
            # 值表达式引用隐藏列别名做零分母判断，避免参数化表达式重复绑定。
            ratio = (
                f"CASE WHEN {{col}} IS NULL OR {{col}} = 0 THEN NULL "
                f"ELSE 1.0 * ({num}) / {{col}} END"
            )
            return ratio, den
        raise InvalidRequestError(f"未知指标类型: {metric.kind}")  # pragma: no cover

    def _component_expr(self, comp: RatioComponent, params: list[Any]) -> str:
        # 组件过滤只渲染一次：聚合分支交给 _agg_expr 挂在聚合函数上，
        # 去重分支直接挂 FILTER；禁止在 coalesce 等外包装之外再套 FILTER。
        if comp.distinct is not None:
            _ident(comp.distinct, "去重列")
            base = f"count(DISTINCT {comp.distinct})"
            if comp.filters:
                conds = " AND ".join(self._render_filter(f, params) for f in comp.filters)
                return f"{base} FILTER (WHERE {conds})"
            return base
        assert comp.agg is not None
        return self._agg_expr(comp.agg, extra_filters=comp.filters, params=params)

    def _agg_expr(
        self, agg: AggSpec, *, extra_filters: tuple[Filter, ...], params: list[Any]
    ) -> str:
        func = agg.func.lower()
        if func not in _AGG_FUNCS:
            raise InvalidRequestError(f"不支持的聚合函数: {agg.func!r}")
        # expr 来自 YAML 受信任配置；列形态过标识符校验，函数表达式形态（如 coalesce(...)）
        # 只允许出现在 YAML 中，请求路径无法注入。
        if _IDENT_RE.match(agg.expr):
            _ident(agg.expr, "聚合列")
            expr = agg.expr
        else:
            expr = agg.expr
        # FILTER 必须挂在聚合函数上（DuckDB 语法：sum(x) FILTER (WHERE ...)）；
        # coalesce 空值包装只能在最外层，且仅对裸列求和使用，避免破坏其他表达式。
        inner = f"{func}({expr})"
        if extra_filters:
            conds = " AND ".join(self._render_filter(f, params) for f in extra_filters)
            inner += f" FILTER (WHERE {conds})"
        if func == "sum" and _IDENT_RE.match(agg.expr):
            return f"coalesce({inner}, 0)"
        return inner
    def _render_filter(self, flt: Filter, params: list[Any]) -> str:
        # 受信任原生表达式直接内联（仅 YAML 作者可写，API 请求构造不出该算子）。
        if flt.op == "__raw__":
            return str(flt.value)
        _ident(flt.column, "过滤列")
        op = flt.op.lower()
        if op in _OPS_NO_VALUE:
            return f"{flt.column} {op.upper()}"
        if op in _OPS_SINGLE:
            params.append(flt.value)
            return f"{flt.column} {op.upper()} ?"
        # in / not in：占位符数量随列表长度生成，值逐个绑定。
        placeholders = ", ".join("?" for _ in flt.value)
        params.extend(flt.value)
        return f"{flt.column} {op.upper()} ({placeholders})"


# --------------------------------------------------------------------------- 调试渲染


def _render_inline(sql: str, params: tuple[Any, ...]) -> str:
    """把绑定参数内联成字面量，仅用于展示。

    与执行路径完全分离：执行始终用参数化 SQL；这里的字符串不会被送回引擎。
    """
    out: list[str] = []
    i = 0
    for ch in sql:
        if ch == "?":
            v = params[i]
            i += 1
            if isinstance(v, bool):
                out.append("TRUE" if v else "FALSE")
            elif isinstance(v, (int, float)):
                out.append(str(v))
            elif v is None:
                out.append("NULL")
            else:
                out.append("'" + str(v).replace("'", "''") + "'")
        else:
            out.append(ch)
    return "".join(out)
