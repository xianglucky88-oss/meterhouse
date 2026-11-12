#!/bin/sh
# demo.sh —— 90 秒内录完的固定演示。
#
#   1. 重建数据
#   2. 三个接口各给一个数
#   3. 权威口径
#   4. 逐项断言
#   5. 时区差异
set -eu
cd "$(dirname "$0")/.."

API="${API:-http://localhost:9400}"
say() { printf '\n=== %s ===\n' "$1"; }

say "0. 重建数据"
python3 -m meterhouse seed

say "1. 概览页（口径 A）"
curl -fsS "$API/api/v1/dashboard/summary" | python3 -m json.tool

say "2. 趋势页（口径 B）"
curl -fsS "$API/api/v1/dashboard/trend" | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps({k:v for k,v in d.items() if k!='orders_by_date'}, ensure_ascii=False, indent=2))"

say "3. 区域报表（口径 C）"
curl -fsS "$API/api/v1/reports/region" | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps(d['by_region'], ensure_ascii=False, indent=2))"

say "4. 三份口径并排"
curl -fsS "$API/api/v1/parity" | python3 -m json.tool

say "5. 口径断言"
python3 scripts/verify_metrics.py || true

say "6. 权威口径里的时区差异"
curl -fsS "$API/api/v1/ground-truth" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(f\"10-31 UTC 口径 {d['orders_by_utc_date']} 单，本地口径 {d['orders_by_local_date']} 单\")
"

echo
