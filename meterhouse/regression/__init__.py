"""指标语义回归体系（独立于生产查询实现）。

模块划分
--------
models      声明式场景 / 数据 / 查询 / 检查的数据模型与规范化哈希
fixtures    确定性数据生成（含空值、重复事实、乱序插入、大基数）
oracle      纯 Python 的参考实现，不执行任何生产 SQL
candidates  候选执行器（语义层编译、手写 SQL、抽样执行）
compare     结果规范化、容差判断与差异归因
baseline    基准清单与审批链（基准变更必须显式审批）
sampling    抽样可信度（置信区间 + 最小可检测差异）
minimize    失败用例 delta-debugging 最小化
runner      并行隔离执行与报告聚合
cli         命令行入口
"""
