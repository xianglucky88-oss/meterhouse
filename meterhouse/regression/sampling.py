"""抽样可信度。

大基数场景全量 oracle 求值代价高，因此允许“抽样执行候选 + 全量/抽样参考”。
关键约束：抽样必须给出可信判定，而不是拿一个随机数糊弄门禁。

* ``calculate_sample_size``：按比例指标的 Wald 置信区间与目标半宽推算样本量；
* ``bound_sample_error``：对给定样本给出比例估计与置信区间；
* ``decide``：把“候选值是否落在参考区间内”分为 pass / fail / inconclusive，
  inconclusive 不计为失败但会进入报告，避免环境波动造成误报；
* 采样本身由 ``deterministic_hash_sample`` 决定，可复现。
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Any, Callable, Iterable

# 95% / 99% 双侧正态分位数（Wilson 区间用）
Z_95 = 1.959963984540054
Z_99 = 2.5758293035489004


def deterministic_hash_sample(items: Iterable[Any], n: int, salt: str = "meterhouse") -> list[Any]:
    """按 sha256(salt|item) 排序取前 n 个：与输入顺序无关、跨进程可复现。

    不用 random.shuffle，因为 Python hash 随机化与 PYTHONHASHSEED 会破坏复现。
    """
    def key(item: Any) -> str:
        return hashlib.sha256(f"{salt}|{item}".encode("utf-8")).hexdigest()

    return sorted(items, key=key)[:n]


def sample_size_for_proportion(p: float, desired_half_width: float, z: float = Z_95, finite_population: int | None = None) -> int:
    """比例估计所需样本量（Cochran 公式，可带有限总体校正）。

    注意：p 接近 0/1 时该公式退化为很小的样本量，不适用于“允许零失败”的验收
    口径；那种场景用 ``sample_size_for_zero_failures``。
    """
    if not 0 < desired_half_width < 1:
        raise ValueError("desired_half_width must be in (0, 1)")
    p = min(max(p, 1e-6), 1 - 1e-6)
    n = (z * z * p * (1 - p)) / (desired_half_width ** 2)
    n = math.ceil(n)
    if finite_population:
        n = math.ceil(n / (1 + (n - 1) / finite_population))
    return max(n, 2)


def sample_size_for_zero_failures(tolerance: float, confidence: float = 0.95) -> int:
    """在“样本零失败”的前提下，判定失败率上界 <= tolerance 所需的样本量。

    直接复用 ``ProportionEstimate`` 的 Wilson 下界做数值求解，保证与
    ``decide`` 的判定口径一致（不会出现“按公式算够了、实际仍判 inconclusive”）。
    """
    if not 0 < tolerance < 1:
        raise ValueError("tolerance must be in (0, 1)")
    n = 2
    while ProportionEstimate(n, n, confidence).interval[0] < 1 - tolerance:
        n = int(n * 1.5) + 1
        if n > 10_000_000:
            raise ValueError("tolerance too small to reach with zero failures")
    lo, hi = max(2, n // 2), n
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if ProportionEstimate(mid, mid, confidence).interval[0] >= 1 - tolerance:
            hi = mid
        else:
            lo = mid
    return hi


@dataclass
class ProportionEstimate:
    successes: int
    n: int
    confidence: float = 0.95

    @property
    def p(self) -> float:
        return self.successes / self.n if self.n else 0.0

    @property
    def _z(self) -> float:
        return Z_95 if self.confidence == 0.95 else Z_99

    @property
    def interval(self) -> tuple[float, float]:
        """Wilson score 区间。

        不用 Wald 区间：p=0 或 p=1 时 Wald 宽度为 0，会给出“样本全通过 =
        绝对确定”的假象，恰好是抽样门禁最不该有的行为。
        """
        if self.n == 0:
            return (0.0, 1.0)
        z = self._z
        p = self.p
        denom = 1 + z * z / self.n
        center = (p + z * z / (2 * self.n)) / denom
        margin = z * math.sqrt(max(p * (1 - p) / self.n + z * z / (4 * self.n * self.n), 0.0)) / denom
        return (max(0.0, center - margin), min(1.0, center + margin))

    @property
    def half_width(self) -> float:
        low, high = self.interval
        return (high - low) / 2

    def as_dict(self) -> dict[str, Any]:
        return {
            "successes": self.successes,
            "n": self.n,
            "p": self.p,
            "confidence": self.confidence,
            "half_width": self.half_width,
            "interval": list(self.interval),
            "method": "wilson",
        }


@dataclass
class SampleVerdict:
    status: str  # pass | fail | inconclusive
    estimate: ProportionEstimate
    reference: float
    tolerance: float
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "estimate": self.estimate.as_dict(),
            "reference": self.reference,
            "tolerance": self.tolerance,
            "reason": self.reason,
        }


def decide(estimate: ProportionEstimate, reference: float, tolerance: float) -> SampleVerdict:
    """用置信区间与容差判定。

    * 区间完全落在容差带内 -> pass
    * 区间完全在容差带外 -> fail
    * 区间跨越容差边界 -> inconclusive（样本量不足，不能据此阻断）
    这样与抽样波动相关的波动不会造成误报，也不会把真实偏差放过去。
    """
    if not 0 <= reference <= 1:
        raise ValueError("reference must be a probability in [0, 1]")
    low, high = estimate.interval
    band_low = reference - tolerance
    band_high = reference + tolerance
    if low >= band_low and high <= band_high:
        return SampleVerdict("pass", estimate, reference, tolerance, "confidence interval inside tolerance band")
    if high < band_low or low > band_high:
        return SampleVerdict("fail", estimate, reference, tolerance, "confidence interval outside tolerance band")
    return SampleVerdict(
        "inconclusive",
        estimate,
        reference,
        tolerance,
        "confidence interval crosses the tolerance boundary; increase sample size or tighten data",
    )


def evaluate_proportion_sample(
    items: list[Any],
    checks: Callable[[Any], tuple[int, int]],
    reference: float,
    tolerance: float,
) -> SampleVerdict:
    """对抽样条目执行 check，汇总成比例并判定。

    ``checks`` 返回 (successes, total) 二元组，可一次统计多条断言。
    """
    successes = 0
    total = 0
    for item in items:
        s, t = checks(item)
        successes += s
        total += t
    return decide(ProportionEstimate(successes, total), reference, tolerance)
