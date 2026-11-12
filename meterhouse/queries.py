"""遗留 SQL：散落在三个接口里的手写查询。

这**不是**反例教学——这是当前线上真实跑的代码。
三份"活跃供应商数"口径不一致，就是从这里来的。
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# 口径 A：/api/v1/dashboard/summary
#   按 supplier_id 去重，跨整个时间范围
# ---------------------------------------------------------------------------
ACTIVE_SUPPLIERS_A = """
SELECT count(DISTINCT supplier_id) AS active_suppliers
FROM supplier_stats
WHERE is_active
"""

# ---------------------------------------------------------------------------
# 口径 B：/api/v1/dashboard/trend
#   按 (supplier_id, date) 去重后求平均，但 date 用的是 UTC
# ---------------------------------------------------------------------------
ACTIVE_SUPPLIERS_B = """
SELECT avg(cnt) AS active_suppliers
FROM (
    SELECT CAST(stat_time AS DATE) AS d, count(DISTINCT supplier_id) AS cnt
    FROM supplier_stats
    WHERE is_active
    GROUP BY 1
) t
"""

# ---------------------------------------------------------------------------
# 口径 C：/api/v1/reports/region
#   按 (supplier_id, date, region) 去重 —— 供应商换过区域会被算两次
#   而且忘了 is_active 过滤
# ---------------------------------------------------------------------------
ACTIVE_SUPPLIERS_C = """
SELECT region, count(DISTINCT supplier_id || '|' || CAST(stat_time AS DATE)) AS active_suppliers
FROM supplier_stats
GROUP BY region
"""

# ---------------------------------------------------------------------------
# 其他指标
# ---------------------------------------------------------------------------

GMV_CENTS = """
SELECT coalesce(sum(amount_cents), 0) AS gmv_cents
FROM order_facts
WHERE status = 'paid'
"""

# 注意：这里没有按 order_id 去重，重复推送的订单会被重复计入
ORDERS_TOTAL = """
SELECT count(*) AS orders_total FROM order_facts
"""

ORDERS_BY_DATE = """
SELECT CAST(created_at AS DATE) AS d, count(*) AS orders
FROM order_facts
WHERE status = 'paid'
GROUP BY 1
ORDER BY 1
"""

SUPPLIER_COUNT_BY_REGION = """
SELECT region, count(DISTINCT supplier_id) AS suppliers
FROM supplier_stats
GROUP BY region
ORDER BY region
"""

CONVERSION_RATE = """
SELECT
    CASE WHEN count(*) = 0 THEN 0
         ELSE 1.0 * count(*) FILTER (WHERE status = 'paid') / count(*)
    END AS paid_rate
FROM order_facts
"""

AVG_ORDER_CENTS = """
SELECT coalesce(avg(amount_cents), 0) AS avg_order_cents
FROM order_facts
WHERE status = 'paid'
"""

QUERIES = {
    "active_suppliers_a": ACTIVE_SUPPLIERS_A,
    "active_suppliers_b": ACTIVE_SUPPLIERS_B,
    "active_suppliers_c": ACTIVE_SUPPLIERS_C,
    "gmv_cents": GMV_CENTS,
    "orders_total": ORDERS_TOTAL,
    "orders_by_date": ORDERS_BY_DATE,
    "supplier_count_by_region": SUPPLIER_COUNT_BY_REGION,
    "conversion_rate": CONVERSION_RATE,
    "avg_order_cents": AVG_ORDER_CENTS,
}
