SHELL := /bin/sh
PY ?= python3
API ?= http://localhost:9400

.PHONY: help
help:
	@echo "make seed         生成示例数据（DuckDB）"
	@echo "make verify       口径 + 迁移就绪度断言"
	@echo "make parity       两引擎逐指标对比"
	@echo "make sync         明细同步到 ClickHouse"
	@echo "make check-parity 校验两引擎明细逐行一致"
	@echo "make demo         固定演示序列"
	@echo "make api          起服务（9400）"
	@echo "make test         单元测试（不含 ClickHouse）"
	@echo "make test-all     单元测试 + ClickHouse 端到端"
	@echo "make cutover-shadow  所有接口切到 shadow"
	@echo "make cutover-primary 所有接口切到 primary（保留回退）"

.PHONY: seed
seed:
	$(PY) -m meterhouse seed

.PHONY: verify
verify:
	$(PY) scripts/verify_metrics.py

.PHONY: parity
parity:
	$(PY) -m meterhouse parity

.PHONY: sync
sync:
	$(PY) -m meterhouse sync

.PHONY: check-parity
check-parity:
	$(PY) -m meterhouse check-parity

.PHONY: demo
demo:
	sh scripts/demo.sh

.PHONY: api
api:
	$(PY) -m uvicorn meterhouse.api:app --port 9400 --reload

.PHONY: test
test:
	$(PY) -m pytest -q

.PHONY: test-all
test-all:
	$(PY) -m pytest -q

.PHONY: cutover-shadow
cutover-shadow:
	@for i in dashboard.summary dashboard.trend reports.region metrics.query exports.daily; do \
		$(PY) -m meterhouse cutover $$i --mode shadow --actor make --reason "观察差异" >/dev/null; \
	done
	@echo "all interfaces -> shadow"

.PHONY: cutover-primary
cutover-primary:
	@for i in dashboard.summary dashboard.trend reports.region metrics.query exports.daily; do \
		$(PY) -m meterhouse cutover $$i --mode primary --fallback --actor make --reason "切流" >/dev/null; \
	done
	@echo "all interfaces -> primary (fallback on)"

.PHONY: clean
clean:
	rm -rf data .pytest_cache
