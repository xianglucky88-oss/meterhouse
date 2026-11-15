#!/bin/sh
# demo.sh —— 语义层演示序列。
#
#   1. 重建数据
#   2. 三个业务接口（全部经语义编译器取数，口径一致）
#   3. v2 统一查询：维度 + 粒度 + 过滤 + 同环比
#   4. 非可加/比率上卷被显式拒绝
#   5. 权威口径与硬门禁断言
set -eu
cd "$(dirname "$0")/.."

API="${API:-http://localhost:9400}"
say() { printf '\n=== %s ===\n' "$1"; }

say "0. 重建数据"
python3 -m meterhouse seed

say "1. 概览页（语义层无维度重算）"
curl -fsS "$API/api/v1/dashboard/summary" | python3 -m json.tool

say "2. 趋势页（业务时区日序列，非可加指标不给误导性均值）"
curl -fsS "$API/api/v1/dashboard/trend" | python3 -c "
import json,sys
d=json.load(sys.stdin)
d['active_suppliers_series']=d['active_suppliers_series'][:3]+['...']
print(json.dumps(d, ensure_ascii=False, indent=2))"

say "3. 区域报表（区域上卷被拒绝，总量单独重算）"
curl -fsS "$API/api/v1/parity" | python3 -m json.tool

say "4. v2 统一查询：GMV 按区域 + 按日，带状态过滤"
curl -fsS "$API/api/v2/metrics/gmv_cents?dimensions=region&filters=status:=:paid" \
  | python3 -m json.tool

say "5. 同环比：平移窗口整体重算"
curl -fsS "$API/api/v2/metrics/gmv_cents/compare?compare=previous_period&start=2026-10-01T00:00:00&end=2026-10-08T00:00:00" \
  | python3 -m json.tool

say "6. 非可加上卷显式拒绝"
curl -s -o /tmp/rollup.json -w "HTTP %{http_code}\n" "$API/api/v2/metrics/active_suppliers/rollup?dimensions=region"
python3 -m json.tool /tmp/rollup.json

say "7. 未知维度被拒绝（400，而不是返回错误口径的 200）"
curl -s -o /tmp/bad.json -w "HTTP %{http_code}\n" "$API/api/v2/metrics/gmv_cents?dimensions=evil_dim"
python3 -m json.tool /tmp/bad.json

say "8. 硬门禁断言"
python3 scripts/verify_metrics.py
