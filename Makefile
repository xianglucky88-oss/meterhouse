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
	@echo "make regress  指标语义回归（对照已审批基准）"
	@echo "make regress-approve  生成基准变更预览（dry-run，不写盘）"
	@echo "make regress-datasets 打印夹具哈希"

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

.PHONY: regress
regress:
	$(PY) -m meterhouse.regression run --jobs 4

.PHONY: regress-diff
regress-diff:
	$(PY) -m meterhouse.regression diff

.PHONY: regress-approve
regress-approve:
	@echo "生成基准并预览变化，不写盘。写盘需人工执行："
	@echo "  python -m meterhouse.regression approve --approver <人> --ticket <单号> --reason <理由> --approve"
	$(PY) -m meterhouse.regression approve --approver "$(APPROVER)" --ticket "$(TICKET)" --reason "$(REASON)"

.PHONY: regress-datasets
regress-datasets:
	$(PY) -m meterhouse.regression datasets

.PHONY: clean
clean:
	rm -rf data .pytest_cache
