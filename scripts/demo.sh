#!/bin/sh
# demo.sh —— 迁移演示序列。
#
#   1. 重建数据（DuckDB）
#   2. 语义层口径断言（DuckDB）
#   3. 同步明细到 ClickHouse，校验逐行一致
#   4. 两引擎逐指标对比
#   5. 逐接口切流：off → shadow → primary
#   6. 停掉 ClickHouse，展示回退与 503 语义
set -eu
cd "$(dirname "$0")/.."

API="${API:-http://localhost:9400}"
say() { printf '\n=== %s ===\n' "$1"; }
jq_() { python3 -m json.tool; }

say "0. 重建数据"
python3 -m meterhouse seed

say "1. 语义层口径（DuckDB）"
python3 -m meterhouse verify

say "2. 同步明细到 ClickHouse"
python3 -m meterhouse sync

say "3. 两引擎明细逐行一致"
python3 -m meterhouse check-parity

say "4. 两引擎逐指标对比"
python3 -m meterhouse parity

say "5. 切流状态"
curl -fsS "$API/api/v1/routes" | jq_ .

say "6. 接口切到 shadow（数字仍来自 DuckDB，同时比对 ClickHouse）"
curl -fsS -X POST "$API/api/v1/routes/dashboard.summary" \
  -H 'content-type: application/json' \
  -d '{"mode":"shadow","reason":"demo"}' >/dev/null
curl -fsS "$API/api/v1/dashboard/summary" | jq_ .
curl -fsS "$API/api/v1/status" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('shadow comparison:', d['comparison_counts'])
"

say "7. 接口切到 primary（数字来自 ClickHouse）"
curl -fsS -X POST "$API/api/v1/routes/dashboard.summary" \
  -H 'content-type: application/json' \
  -d '{"mode":"primary","reason":"demo"}' >/dev/null
curl -fsS "$API/api/v1/dashboard/summary" | jq_ .

say "8. 引擎故障：停掉 ClickHouse"
echo "（演示需手动停掉 CLICKHOUSE，或设置 MH_DEMO_SKIP_OUTAGE=1 跳过）"
if [ "${MH_DEMO_SKIP_OUTAGE:-0}" != "1" ]; then
  echo "停止 ClickHouse 后，summary 应回退 DuckDB 并标记 degraded=true；"
  echo "若关闭 fallback，则应返回 503 而不是 0。"
fi
