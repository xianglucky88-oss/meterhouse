"""基准清单与审批链。

设计原则
--------
* 基准文件是 version-controlled 的参考数据，记录每个场景/查询/检查的期望值、
  数据集哈希、指标定义指纹与审批元信息。
* **不允许**通过“更新期望结果”掩盖问题：写入新基准必须提供审批记录
  （审批人、工单、理由）且必须显式传 ``--approve``。
* 每次回归都把 oracle 重算结果与基准对照；不一致即 reference_drift，
  即使候选实现恰好“看起来正常”也不能通过。
* 基准的完整性由 ``BaselineVerifier`` 校验：签名、数据集哈希、定义指纹。

文件格式（JSON，稳定可 diff）::

    {
      "schema": 1,
      "approved_at": "...",
      "approval": {"approver": "...", "ticket": "...", "reason": "...", "commit": "..."},
      "datasets": {"name": "<hash>"},
      "metrics": {"name": {"definition_version": n, "semantic_hash": "..."}},
      "scenarios": {
        "<group>/<id>": {
          "fingerprint": "...",
          "dataset": "name",
          "queries": {"<query fingerprint>": {"key": "...", "rows": [...]}}
        }
      }
    }
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import Query, Scenario

SCHEMA = 1


@dataclass
class Approval:
    approver: str
    ticket: str
    reason: str
    commit: str = ""
    approved_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "Approval":
        missing = [k for k in ("approver", "ticket", "reason") if not str(raw.get(k, "")).strip()]
        if missing:
            raise ValueError(f"approval missing required fields: {missing}")
        return cls(
            approver=raw["approver"],
            ticket=raw["ticket"],
            reason=raw["reason"],
            commit=raw.get("commit", ""),
            approved_at=raw.get("approved_at") or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "approver": self.approver,
            "ticket": self.ticket,
            "reason": self.reason,
            "commit": self.commit,
            "approved_at": self.approved_at,
        }


@dataclass
class QueryReference:
    """一个查询的参考结果（规范化行数据）。"""

    query_key: str
    rows: list[dict[str, Any]]
    key_columns: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {"key": self.query_key, "rows": self.rows, "key_columns": self.key_columns}


@dataclass
class ScenarioReference:
    fingerprint: str
    dataset: str
    dataset_hash: str
    queries: dict[str, QueryReference] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "dataset": self.dataset,
            "dataset_hash": self.dataset_hash,
            "queries": {k: v.as_dict() for k, v in self.queries.items()},
        }


@dataclass
class Baseline:
    path: Path
    approval: Approval | None
    datasets: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    scenarios: dict[str, ScenarioReference] = field(default_factory=dict)
    approved_at: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path | str) -> "Baseline | None":
        p = Path(path)
        if not p.exists():
            return None
        raw = json.loads(p.read_text(encoding="utf-8"))
        if raw.get("schema") != SCHEMA:
            raise ValueError(f"{p}: unsupported baseline schema {raw.get('schema')}")
        approval = Approval.from_raw(raw["approval"]) if raw.get("approval") else None
        scenarios = {}
        for name, ref in raw.get("scenarios", {}).items():
            queries = {}
            for qkey, qref in ref.get("queries", {}).items():
                queries[qkey] = QueryReference(
                    query_key=qref["key"],
                    rows=qref["rows"],
                    key_columns=qref.get("key_columns", []),
                )
            scenarios[name] = ScenarioReference(
                fingerprint=ref.get("fingerprint", ""),
                dataset=ref.get("dataset", ""),
                dataset_hash=ref.get("dataset_hash", ""),
                queries=queries,
            )
        return cls(
            path=p,
            approval=approval,
            datasets=dict(raw.get("datasets", {})),
            metrics=dict(raw.get("metrics", {})),
            scenarios=scenarios,
            approved_at=raw.get("approved_at", ""),
            raw=raw,
        )

    # ----------------------------------------------------------- 查询基准

    def scenario(self, scenario: Scenario) -> ScenarioReference | None:
        return self.scenarios.get(scenario.full_name)

    def query_ref(self, scenario: Scenario, query: Query) -> QueryReference | None:
        ref = self.scenario(scenario)
        if ref is None:
            return None
        return ref.queries.get(query.fingerprint)

    def dataset_hash(self, dataset_name: str) -> str | None:
        return self.datasets.get(dataset_name)

    def metric(self, name: str) -> dict[str, Any] | None:
        return self.metrics.get(name)

    # ------------------------------------------------------------ 校验

    def verify_integrity(self) -> list[str]:
        problems = []
        if self.approval is None:
            problems.append("baseline has no approval record")
        if not self.approved_at:
            problems.append("baseline has no approved_at")
        if not self.scenarios:
            problems.append("baseline contains no scenarios")
        for name, ref in self.scenarios.items():
            if not ref.fingerprint:
                problems.append(f"{name}: missing scenario fingerprint")
            if not ref.dataset_hash:
                problems.append(f"{name}: missing dataset hash")
        return problems


# --------------------------------------------------------------------------
# 生成基准
# --------------------------------------------------------------------------


def build_baseline(
    scenario_set: Any,
    oracle: Any,
    fixtures: Any,
    store: Any,
    approval: Approval,
) -> Baseline:
    """用 oracle 为所有场景/查询生成基准。

    只有显式提供审批时才允许写盘（见 ``write_baseline``）。
    """
    datasets = fixtures.hashes()
    metrics = store.fingerprints()
    scenarios: dict[str, ScenarioReference] = {}
    for scenario in scenario_set.scenarios:
        dataset = fixtures.get(scenario.dataset)
        refs: dict[str, QueryReference] = {}
        for query in scenario.all_queries():
            rows = oracle.evaluate(query, dataset)
            from .compare import normalize_rows

            key_cols = list(query.dimensions) + (["date"] if query.granularity else [])
            refs[query.fingerprint] = QueryReference(
                query_key=query.fingerprint,
                rows=normalize_rows(rows),
                key_columns=key_cols,
            )
        scenarios[scenario.full_name] = ScenarioReference(
            fingerprint=scenario.fingerprint,
            dataset=scenario.dataset,
            dataset_hash=dataset.hash(),
            queries=refs,
        )
    return Baseline(
        path=Path(""),
        approval=approval,
        datasets=datasets,
        metrics=metrics,
        scenarios=scenarios,
        approved_at=approval.approved_at,
        raw={},
    )


def write_baseline(baseline: Baseline, path: Path | str, *, approve: bool) -> Path:
    """写盘前必须显式审批，禁止静默覆盖基准。"""
    if not approve:
        raise PermissionError(
            "refusing to write baseline without explicit --approve; "
            "baseline changes require review and an approval record"
        )
    if baseline.approval is None:
        raise PermissionError("baseline write requires an approval record")
    payload = {
        "schema": SCHEMA,
        "approved_at": baseline.approved_at,
        "approval": baseline.approval.as_dict(),
        "datasets": baseline.datasets,
        "metrics": baseline.metrics,
        "scenarios": {k: v.as_dict() for k, v in sorted(baseline.scenarios.items())},
    }
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return p


def diff_baselines(old: Baseline | None, new: Baseline) -> dict[str, Any]:
    """展示基准变化，供审批人复核；不做任何自动接受。"""
    changed: list[dict[str, Any]] = []
    if old is None:
        return {"is_new": True, "scenarios": sorted(new.scenarios)}
    for name in sorted(set(old.scenarios) | set(new.scenarios)):
        o = old.scenarios.get(name)
        n = new.scenarios.get(name)
        if o is None:
            changed.append({"scenario": name, "change": "added"})
        elif n is None:
            changed.append({"scenario": name, "change": "removed"})
        elif o.fingerprint != n.fingerprint:
            changed.append({"scenario": name, "change": "scenario_fingerprint"})
        else:
            for qkey in sorted(set(o.queries) | set(n.queries)):
                oq = o.queries.get(qkey)
                nq = n.queries.get(qkey)
                if oq is None or nq is None or oq.rows != nq.rows:
                    changed.append({"scenario": name, "query": qkey, "change": "reference_rows"})
    dataset_changes = {k: {"from": old.datasets.get(k), "to": v} for k, v in new.datasets.items() if old.datasets.get(k) != v}
    metric_changes = {}
    for k, v in new.metrics.items():
        if old.metrics.get(k) != v:
            metric_changes[k] = {"from": old.metrics.get(k), "to": v}
    return {
        "is_new": False,
        "scenario_changes": changed,
        "dataset_changes": dataset_changes,
        "metric_changes": metric_changes,
    }
