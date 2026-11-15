#!/usr/bin/env python3
"""CI 用：检查当前指标定义/夹具/场景是否已偏离已审批基准。

任何偏离都说明需要走 approve 流程，流水线必须失败，而不是自动接受新基准。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import yaml  # noqa: E402

from meterhouse.regression.baseline import Approval, Baseline, build_baseline, diff_baselines  # noqa: E402
from meterhouse.regression.fixtures import FixtureLibrary  # noqa: E402
from meterhouse.regression.models import ScenarioSet  # noqa: E402
from meterhouse.regression.oracle import Oracle  # noqa: E402
from meterhouse.semantic import MetricStore  # noqa: E402


def main() -> int:
    store = MetricStore.load(ROOT / "metrics" / "metrics.yaml")
    scenarios = ScenarioSet.from_raw(
        yaml.safe_load((ROOT / "meterhouse/regression/fixtures/scenarios.yaml").read_text(encoding="utf-8"))
    )
    fixtures = FixtureLibrary.load(ROOT / "meterhouse/regression/fixtures/datasets.yaml")
    baseline_path = ROOT / "baseline" / "metric_reference.json"
    old = Baseline.load(baseline_path)
    preview = build_baseline(scenarios, Oracle.from_store(store), fixtures, store,
                             Approval(approver="ci", ticket="ci", reason="drift check"))
    changes = diff_baselines(old, preview)
    print(json.dumps(changes, ensure_ascii=False, indent=2))
    has_drift = bool(
        changes.get("is_new")
        or changes.get("scenario_changes")
        or changes.get("dataset_changes")
        or changes.get("metric_changes")
    )
    if has_drift:
        print(
            "\nbaseline drifted: metric/fixture/scenario changed. "
            "Review the diff, then re-approve with: "
            "python -m meterhouse.regression approve --approver ... --ticket ... --reason ... --approve",
            file=sys.stderr,
        )
        return 1
    print("baseline is in sync with definitions, fixtures and scenarios")
    return 0


if __name__ == "__main__":
    sys.exit(main())
