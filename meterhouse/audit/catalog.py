"""数据集目录：导出契约、列白名单与敏感字段最小化策略。

外部审计交付物中的每一列都必须在此显式登记，未登记的列不会离开数据库；
敏感标识符不做明文导出，改用「任务盐 + HMAC-SHA256」生成稳定假名，
同一任务内同一供应商在两张表中的假名一致，可做关联分析但无法反推原值。
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass, field
from typing import Any

# 最小化策略：
#   keep      原样输出（仅限非敏感列）
#   hmac      HMAC-SHA256 假名化，输出 hex 截断
#   constant  以固定占位符替换（列存在但不含真实值）
POLICY_KEEP = "keep"
POLICY_HMAC = "hmac"
POLICY_CONSTANT = "constant"

# 假名长度（hex 字符数）；16 字符 = 64 bit，足够审计区分且不可逆
PSEUDONYM_HEX_LEN = 16

# 敏感列的占位值
REDACTED_PLACEHOLDER = "REDACTED"


@dataclass(frozen=True)
class ColumnSpec:
    """一列的交付契约。"""

    name: str
    policy: str = POLICY_KEEP
    # constant 策略的占位值
    placeholder: str = REDACTED_PLACEHOLDER

    @property
    def sensitive(self) -> bool:
        return self.policy != POLICY_KEEP


@dataclass(frozen=True)
class DatasetSpec:
    """一个可导出数据集的完整定义。

    ``build_sql`` 返回把源表冻结为快照的 SQL：
    必须完成时间范围过滤、去重与全序排序，并产出 ``_row_no``（从 1 开始）。
    全序排序保证键集分页可重放、不漏不重。
    """

    name: str
    source_table: str
    time_column: str
    columns: tuple[ColumnSpec, ...]
    # 源表的天然唯一键（无天然键时为空，快照保留全部原始行）
    natural_key: tuple[str, ...] = ()
    description: str = ""

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> ColumnSpec:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"column {name!r} not in export catalog for dataset {self.name!r}")

    def build_sql(self, start_utc: str, end_utc: str) -> str:
        """生成快照冻结 SQL。时间边界为左闭右开 [start, end)。"""
        raise NotImplementedError

    def cross_check_sql(self) -> str:
        """对冻结后的快照表做独立聚合，用于与流式扫描结果交叉核对。"""
        raise NotImplementedError


@dataclass(frozen=True)
class SupplierStatsSpec(DatasetSpec):
    """供应商小时级活跃快照。无天然唯一键，原始行全部保留。"""

    def build_sql(self, start_utc: str, end_utc: str) -> str:
        # rowid 作为最后的稳定决胜键，保证快照构建可重放
        return f"""
        SELECT
            row_number() OVER (
                ORDER BY stat_time, supplier_id, region, category, is_active, _rowid
            ) AS _row_no,
            supplier_id, stat_time, is_active, region, category
        FROM (
            SELECT *, rowid AS _rowid
            FROM {self.source_table}
            WHERE stat_time >= TIMESTAMP '{start_utc}'
              AND stat_time <  TIMESTAMP '{end_utc}'
        )
        ORDER BY _row_no
        """

    def cross_check_sql(self) -> str:
        return f"""
        SELECT count(*) AS row_count,
               count(DISTINCT supplier_id) AS distinct_suppliers,
               min(stat_time) AS min_time,
               max(stat_time) AS max_time
        FROM {{snapshot}}
        """


@dataclass(frozen=True)
class OrderFactsSpec(DatasetSpec):
    """支付事实。源表存在上游重推的重复 order_id，快照阶段按订单去重。

    去重规则与 ground_truth 的 GMV 口径一致：同一 order_id 取金额最大的一行；
    金额相同则取 created_at 最新、rowid 最大的一行，保证全序确定。
    仅导出 status='paid' 的支付事实。
    """

    def build_sql(self, start_utc: str, end_utc: str) -> str:
        return f"""
        WITH ranked AS (
            SELECT order_id, supplier_id, created_at, amount_cents, status, region,
                   rowid AS _rowid,
                   row_number() OVER (
                       PARTITION BY order_id
                       ORDER BY amount_cents DESC, created_at DESC, rowid DESC
                   ) AS rn
            FROM {self.source_table}
            WHERE status = 'paid'
              AND created_at >= TIMESTAMP '{start_utc}'
              AND created_at <  TIMESTAMP '{end_utc}'
        )
        SELECT
            row_number() OVER (
                ORDER BY created_at, order_id, region, amount_cents, _rowid
            ) AS _row_no,
            order_id, supplier_id, created_at, amount_cents, status, region
        FROM ranked
        WHERE rn = 1
        ORDER BY _row_no
        """

    def cross_check_sql(self) -> str:
        return f"""
        SELECT count(*) AS row_count,
               count(DISTINCT order_id) AS distinct_orders,
               coalesce(sum(amount_cents), 0) AS total_amount_cents,
               min(created_at) AS min_time,
               max(created_at) AS max_time
        FROM {{snapshot}}
        """


@dataclass(frozen=True)
class Catalog:
    """全部可导出数据集的注册表。"""

    datasets: dict[str, DatasetSpec] = field(default_factory=dict)

    def get(self, name: str) -> DatasetSpec:
        if name not in self.datasets:
            raise KeyError(f"unknown export dataset: {name!r}")
        return self.datasets[name]

    def names(self) -> list[str]:
        return sorted(self.datasets)


def default_catalog() -> Catalog:
    """内置目录。调整最小化范围只需改这里，不影响扫描逻辑。"""
    datasets = {
        "supplier_snapshots": SupplierStatsSpec(
            name="supplier_snapshots",
            source_table="supplier_stats",
            time_column="stat_time",
            description="供应商小时级活跃快照（原始快照，全量行）",
            columns=(
                ColumnSpec("supplier_id", POLICY_HMAC),
                ColumnSpec("stat_time", POLICY_KEEP),
                ColumnSpec("is_active", POLICY_KEEP),
                ColumnSpec("region", POLICY_KEEP),
                ColumnSpec("category", POLICY_KEEP),
            ),
        ),
        "payment_facts": OrderFactsSpec(
            name="payment_facts",
            source_table="order_facts",
            time_column="created_at",
            natural_key=("order_id",),
            description="支付事实（按 order_id 去重，仅 paid）",
            columns=(
                ColumnSpec("order_id", POLICY_HMAC),
                ColumnSpec("supplier_id", POLICY_HMAC),
                ColumnSpec("created_at", POLICY_KEEP),
                ColumnSpec("amount_cents", POLICY_KEEP),
                # 状态列保留结构但最小化：快照内恒为 paid，不含额外信息
                ColumnSpec("status", POLICY_CONSTANT),
                ColumnSpec("region", POLICY_KEEP),
            ),
        ),
    }
    return Catalog(datasets=datasets)


class Minimizer:
    """按目录策略把一行原始记录转换为交付记录。

    HMAC 使用任务级随机盐，盐不进入交付物与 manifest，只保存在任务目录的
    受限权限文件中；同一任务内对同一逻辑标识符产生相同假名。
    """

    def __init__(self, spec: DatasetSpec, salt: bytes):
        self._spec = spec
        self._salt = salt

    def pseudonym(self, value: Any) -> str:
        digest = hmac.new(self._salt, str(value).encode("utf-8"), "sha256")
        return digest.hexdigest()[:PSEUDONYM_HEX_LEN]

    def project(self, raw_row: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for col in self._spec.columns:
            if col.policy == POLICY_KEEP:
                out[col.name] = raw_row[col.name]
            elif col.policy == POLICY_HMAC:
                out[col.name] = self.pseudonym(raw_row[col.name])
            elif col.policy == POLICY_CONSTANT:
                out[col.name] = col.placeholder
            else:  # pragma: no cover - 目录在加载时即固定
                raise ValueError(f"unknown minimization policy: {col.policy}")
        return out
