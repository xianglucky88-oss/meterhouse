"""回归 CLI。

    python -m meterhouse.regression run            # 对照已审批基准
    python -m meterhouse.regression run --jobs 4 --minimize
    python -m meterhouse.regression approve --approver alice --ticket MET-123 --reason "..."
    python -m meterhouse.regression diff            # 候选基准相对已审批基准的变化
    python -m meterhouse.regression datasets        # 打印数据集哈希
    python -m meterhouse.regression metrics         # 打印指标定义指纹
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from meterhouse.semantic import MetricStore

from .baseline import Approval, Baseline, build_baseline, diff_baselines, write_baseline
from .fixtures import FixtureLibrary
from .models import ScenarioSet
from .oracle import Oracle
from .runner import run_all

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_METRICS = ROOT / "metrics" / "metrics.yaml"
DEFAULT_SCENARIOS = ROOT / "meterhouse" / "regression" / "fixtures" / "scenarios.yaml"
DEFAULT_DATASETS = ROOT / "meterhouse" / "regression" / "fixtures" / "datasets.yaml"
DEFAULT_BASELINE = ROOT / "baseline" / "metric_reference.json"


def _load(args: argparse.Namespace) -> tuple[MetricStore, ScenarioSet, FixtureLibrary, Baseline | None]:
    store = MetricStore.load(args.metrics)
    scenarios = ScenarioSet.from_raw(_read_yaml(args.scenarios))
    scenarios.validate_against(store)
    fixtures = FixtureLibrary.load(args.datasets)
    _check_fixture_refs(scenarios, fixtures)
    baseline = Baseline.load(args.baseline)
    return store, scenarios, fixtures, baseline


def _check_fixture_refs(scenarios: ScenarioSet, fixtures: FixtureLibrary) -> None:
    """场景引用的数据集必须存在；拼错文件名要在执行前报错，而不是运行中途 KeyError。"""
    missing = sorted({s.dataset for s in scenarios.scenarios if s.dataset not in fixtures.datasets})
    if missing:
        raise KeyError(f"scenarios reference unknown datasets: {missing}")


def _read_yaml(path: str | Path) -> dict:
    import yaml

    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def cmd_run(args: argparse.Namespace) -> int:
    store, scenarios, fixtures, baseline = _load(args)
    problems = baseline.verify_integrity() if baseline else ["baseline file is missing"]
    if problems:
        print("baseline is not valid:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        print("run `python -m meterhouse.regression approve ...` after review", file=sys.stderr)
        if args.require_baseline:
            return 2
    report = run_all(
        scenarios,
        fixtures,
        store,
        baseline,
        candidate_name=args.candidate,
        jobs=args.jobs,
        minimize=args.minimize,
        minimize_attempts=args.minimize_attempts,
        only=args.only or None,
    )
    payload = report.as_dict()
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        _print_report(payload)
    if args.output:
        Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary = payload["summary"]
    return 0 if not summary["failed"] else 1


def _print_report(payload: dict) -> None:
    summary = payload["summary"]
    print(f"candidate: {summary['candidate']}  baseline: {summary['baseline'] or '(none)'}")
    print(f"scenarios: {sum(summary['counts'].values())}  counts: {summary['counts']}")
    print(f"verdicts: {summary['verdicts']}")
    for scenario in payload["scenarios"]:
        mark = "PASS" if scenario["status"] == "pass" else "FAIL"
        print(f"  [{mark}] {scenario['scenario']}")
        if scenario.get("error"):
            print(f"      error: {scenario['error'].splitlines()[0]}")
        for q in scenario["queries"]:
            verdict = q["attribution"].get("verdict")
            detail = ""
            if verdict in ("candidate_regression", "shape_mismatch") and q.get("comparison"):
                diffs = q["comparison"]["diffs"]
                if diffs:
                    detail = f" e.g. {diffs[0]}"
            if verdict != "match":
                print(f"      - {verdict}: {q['query']['metric']}{detail}")
        for inv in scenario["invariants"]:
            if not inv["ok"]:
                print(f"      ! {inv['name']}: {inv['detail']}")
            elif inv.get("sampling", {}).get("status") == "inconclusive":
                print(f"      ~ {inv['name']}: {inv['detail']}")
        if scenario.get("minimized"):
            m = scenario["minimized"]
            print(f"      minimized: {m['order_rows']} order rows, {m['supplier_rows']} supplier rows (attempts={m['attempts']})")


def cmd_approve(args: argparse.Namespace) -> int:
    store, scenarios, fixtures, _ = _load(args)
    approval = Approval.from_raw(
        {"approver": args.approver, "ticket": args.ticket, "reason": args.reason, "commit": args.commit or ""}
    )
    oracle = Oracle.from_store(store)
    new_baseline = build_baseline(scenarios, oracle, fixtures, store, approval)
    old = Baseline.load(args.baseline)
    changes = diff_baselines(old, new_baseline)
    print(json.dumps({"changes": changes, "approval": approval.as_dict()}, ensure_ascii=False, indent=2))
    if not args.approve:
        print("\ndry-run: pass --approve to write the new baseline", file=sys.stderr)
        return 1
    write_baseline(new_baseline, args.baseline, approve=True)
    print(f"baseline written: {args.baseline}")
    return 0


def cmd_diff(args: argparse.Namespace) -> int:
    store, scenarios, fixtures, old = _load(args)
    approval = Approval(approver="(preview)", ticket="(preview)", reason="preview only")
    oracle = Oracle.from_store(store)
    new = build_baseline(scenarios, oracle, fixtures, store, approval)
    changes = diff_baselines(old, new)
    print(json.dumps(changes, ensure_ascii=False, indent=2))
    return 0


def cmd_datasets(args: argparse.Namespace) -> int:
    fixtures = FixtureLibrary.load(args.datasets)
    print(json.dumps(fixtures.hashes(), ensure_ascii=False, indent=2))
    return 0


def cmd_metrics(args: argparse.Namespace) -> int:
    store = MetricStore.load(args.metrics)
    print(json.dumps(store.fingerprints(), ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="meterhouse.regression")
    ap.add_argument("--metrics", default=str(DEFAULT_METRICS))
    ap.add_argument("--scenarios", default=str(DEFAULT_SCENARIOS))
    ap.add_argument("--datasets", default=str(DEFAULT_DATASETS))
    ap.add_argument("--baseline", default=str(DEFAULT_BASELINE))
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="执行回归")
    run.add_argument("--candidate", default="semantic")
    run.add_argument("--jobs", type=int, default=None, help="并行进程数；1 表示串行")
    run.add_argument("--only", action="append", help="只跑指定场景或分组，可重复")
    run.add_argument("--minimize", action="store_true")
    run.add_argument("--minimize-attempts", type=int, default=40)
    run.add_argument("--json", action="store_true")
    run.add_argument("--output")
    run.add_argument("--require-baseline", action="store_true", default=True)
    run.set_defaults(func=cmd_run)

    approve = sub.add_parser("approve", help="生成并（显式）写入新基准")
    approve.add_argument("--approver", required=True)
    approve.add_argument("--ticket", required=True)
    approve.add_argument("--reason", required=True)
    approve.add_argument("--commit")
    approve.add_argument("--approve", action="store_true", help="真正写盘；缺省为 dry-run")
    approve.set_defaults(func=cmd_approve)

    diff = sub.add_parser("diff", help="预览基准变化")
    diff.set_defaults(func=cmd_diff)

    datasets = sub.add_parser("datasets", help="打印数据集哈希")
    datasets.set_defaults(func=cmd_datasets)

    metrics = sub.add_parser("metrics", help="打印指标定义指纹")
    metrics.set_defaults(func=cmd_metrics)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
