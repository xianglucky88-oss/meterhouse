SHELL := /bin/sh
PY ?= python3
API ?= http://localhost:9400

.PHONY: help
help:
	@echo "make seed     生成示例数据"
	@echo "make verify   口径一致性断言"
	@echo "make demo     固定演示序列"
	@echo "make api      起服务（9400）"
	@echo "make test     单元测试"
	@echo "make parity   打印三份口径对比"

.PHONY: seed
seed:
	$(PY) -m meterhouse seed

.PHONY: verify
verify:
	$(PY) scripts/verify_metrics.py

.PHONY: parity
parity:
	$(PY) -m meterhouse parity

.PHONY: demo
demo:
	sh scripts/demo.sh

.PHONY: api
api:
	$(PY) -m uvicorn meterhouse.api:app --port 9400 --reload

.PHONY: test
test:
	$(PY) -m pytest -q

.PHONY: clean
clean:
	rm -rf data .pytest_cache
