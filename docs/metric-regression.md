# 指标语义回归体系

本文件说明 `meterhouse/regression/` 的口径契约、基准审批流程与门禁规则。代码是
唯一事实来源；这里只写代码里无法自解释的约定。

## 1. 为什么要独立于生产实现

回归基准不能由被考核的查询实现自己生产，否则永远自洽。整个体系分三层：

| 层 | 实现 | 职责 |
|---|---|---|
| 声明 | `fixtures/datasets.yaml`、`fixtures/scenarios.yaml` | 数据形状、查询、断言；**不含期望数字** |
| 参考 | `regression/oracle.py` + `fixtures/expected_truths.yaml` | 纯 Python 对行数据求值；手算真值锚定参考层自身 |
| 候选 | `regression/candidates.py` | 被测实现：`semantic`（当前生产路径）或 `legacy`（改造前手写 SQL） |

候选与参考的唯一共同输入是指标定义（`metrics/metrics.yaml`）与夹具。oracle 不读
编译器生成的 SQL，因此编译器写错时两边会分叉——这正是要捕获的信号。

但 oracle 也由定义派生：如果定义本身写错（例如 `dedup` 放在指标级而非 `measure` 内），
候选和 oracle 会一起错、回归全绿。为此有两道防线：

1. **严格模式**：定义里的未知字段直接报错，裸 `expr` / 字符串筛选也不接受；
2. **手算真值**：`fixtures/expected_truths.yaml` 的数字逐行手算，与定义、编译器、
   oracle 都无关，由 `tests/test_reference_truths.py` 校验。它不符时先修参考层，
   不能改真值去迁就实现。

## 2. 指标定义契约

`metrics/metrics.yaml` 用结构化字段代替 SQL 字符串：

```yaml
kind: simple | distinct | ratio
measure:            # simple/distinct 的聚合分支
  agg: sum | count | count_distinct | min | max | avg
  column: ...
  filters: [...]    # 该分支自己的筛选
  dedup:            # 事实去重（重复推送）
    keys: [order_id]
    policy: max_by
    column: amount_cents
    tiebreak: [ingest_seq]
numerator / denominator: Measure     # ratio 的分子分母，各自可带 dedup
filters: [...]       # 指标级筛选
dimensions: [...]    # 允许的分组维度（白名单）
time_column: ...     # 时间粒度与时间范围依赖它
additivity: additive | semi_additive | non_additive
empty_value: 0 | null
definition_version: n
```

不得再用裸 `expr`（编译器会拒绝加载）。`label`/`description` 不影响口径，其余语义
字段参与 `semantic_hash`。

## 3. 基准文件与审批

基准在 `baseline/metric_reference.json`，是纳入版本控制的可审查参考数据：

* `datasets`: 每个夹具的内容哈希；
* `metrics`: 每个指标的定义版本与 `semantic_hash`；
* `scenarios`: 场景指纹 + 每个查询的参考行数据；
* `approval`: 审批人、工单、理由、审批时间。

**写盘必须显式审批：**

```bash
# 预览变化（dry-run，不写盘）
python -m meterhouse.regression approve \
  --approver alice --ticket MET-123 --reason "口径 v2 上线"

# 确认后写盘（必须显式 --approve）
python -m meterhouse.regression approve \
  --approver alice --ticket MET-123 --reason "口径 v2 上线" --approve
```

`write_baseline` 在 `approve=False` 时直接抛 `PermissionError`；缺少审批人/工单/理由
同样拒绝写盘。任何“更新期望结果让测试变绿”的路径都不存在。

## 4. 差异归因

每次执行按固定优先级归因（顺序不可调换）：

1. `unapproved_baseline` — 该查询没有已审批基准；阻断。
2. `shape_mismatch` — 列缺失、分组键重复；阻断。
3. `definition_change` — 指标定义指纹变了；旧审批失效，必须重新审批；阻断。
4. `reference_drift` — 定义没变，但 oracle 重算与已审批基准不一致（夹具内容变了
   或参考实现变了）；阻断。
5. `storage_precision` — 差异在声明容差内（如 Decimal 与 double）；不阻断，记录为 note。
6. `candidate_regression` — 定义没变而结果越界；阻断。
7. `match` — 完全一致。

两个关键点：

* **定义变更优先于参考漂移。** oracle 由定义派生，定义一改 oracle 必然漂移；
  若先判参考漂移，改定义就能借“参考漂移”逃避审批。
* **定义一变，即使数字恰好相同也归为 `definition_change`**，避免“改定义 +
  更新期望值”把语义变更伪装成无差异。

场景里可以声明容差：

```yaml
checks:
  - kind: value
    name: ...
    query: ...
    tolerance: {relative: 0.001}      # 相对容差
    # 或 {absolute: 1}、{quantize: 2}（按 2 位小数相等）
```

## 5. 场景检查类型

| kind | 作用 |
|---|---|
| `value` | 对照已审批基准；数字来自基准，不写在场景里 |
| `invariant` | 内置不变量：`additive_rollup`（各期之和 = 全期）、`null_group_preserves_total`（空值分组仍计入总数） |
| `non_additive` | 去重指标沿时间上卷必须**严格**不可加（各期之和 > 全期） |
| `ratio_recompute` | 逐组校验 `比率 == 分子/分母`，并确认夹具能区分“算术平均”与加权重算 |
| `null_policy` | 可空维度的 NULL 是独立分组，且与 `is_null` 筛选结果一致 |
| `sequence` | 结果按维度/日期确定升序，不依赖存储返回顺序 |
| `cardinality` | 结果行数与参考一致（维度组合爆炸/漏分组） |

`sampling` 是场景级配置：对大基数数据集做确定性抽样，用比例置信区间判定。

## 6. 抽样可信度

* 抽样用 `sha256(salt|key)` 排序取前 n 个，**与输入顺序无关、跨进程可复现**，
  不使用 `random`（避免 PYTHONHASHSEED 导致不可复现）。
* 判定用 Wilson score 区间与容差带（不用 Wald 区间：p=0/1 时 Wald 宽度为 0，
  会给出“样本全通过 = 绝对确定”的假象）：
  * 区间完全落在带内 → `pass`
  * 区间完全落在带外 → `fail`（阻断）
  * 跨越边界 → `inconclusive`（`warn`，不阻断）
* `sample_size_for_zero_failures(tolerance)` 按“零失败”验收口径求最小样本量，
  与 `decide` 共用 Wilson 实现，避免公式与判定自相矛盾。失败率上界 3%、95%
  置信时需 125 个样本；场景取 400 留余量，样本不足只会得到 `inconclusive`。

## 7. 失败最小化

`--minimize` 对失败场景做 ddmin：

* 收缩事实行（先订单、后快照、再订单），保留“候选与 oracle 仍不一致”的最小子集；
* **不以静态基准作为失败判据**——删行后基准不再对应当前数据，任何收缩都会
  “保持失败”，最小化就没意义。改为对当前子集重算 oracle。
* 输出最小行数与代表行，便于业务方定位是哪个重复事实/时区边界。

## 8. 并行隔离

* 每个场景在独立进程执行（`ProcessPoolExecutor`），各自使用独立 DuckDB 临时库；
* 候选实例可能持有数据库连接，因此进程间只传候选**名字**，不传实例；
* 夹具装载走 CSV `COPY`，避免万行 `executemany` 拖慢并行与最小化重放；
* 报告按场景名排序，串行与并行的结果必须逐字节一致（有测试覆盖）。

## 9. 夹具

* `fixtures/datasets.yaml` 以显式行数据为主：时区边界（15:59/16:00 UTC）、重复推送
  （同 `order_id` 不同金额/`ingest_seq`）、空 `category`、只有退款订单的空数据集、跨
  月活跃的供应商、比率极不均衡的区域；
* `generator: large_cardinality` 仅用于大基数场景，参数与种子固定；
* `derived` 数据集（如反向插入）由声明式规则生成，结果可复现；
* 夹具内容哈希写入基准，数据一变即 `reference_drift`。

## 10. 命令

```bash
python -m meterhouse.regression run --jobs 4          # 门禁
python -m meterhouse.regression run --candidate legacy --minimize   # 验证体系能抓错
python -m meterhouse.regression diff                   # 预览基准变化
python -m meterhouse.regression datasets               # 夹具哈希
python -m meterhouse.regression metrics                # 定义指纹
```

CI 在 `pytest` 后执行 `run`（消费已审批基准，不写盘）与 `diff`（基准漂移检查）。
基准漂移或候选回归都会让流水线失败；只有经过 `approve` 流程更新基准才能继续。
