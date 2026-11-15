"""审计导出的 HTTP 接口。

路由与交互查询接口隔离挂载：导出任务在后台单线程工作器中执行，
HTTP 请求只负责创建/查询/下载，请求断开不影响任务推进。

下载支持 ``Range`` 断点续传，返回与 manifest 一致的 SHA-256 与行级证明，
审计方可独立重算哈希链核对完整性。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from ..config import settings
from .catalog import default_catalog
from .export import (
    COMPLETED,
    JobManager,
    JobNotFound,
    JobNotReady,
    SnapshotExpired,
)
from .snapshot import SnapshotManager

router = APIRouter(prefix="/api/v1/audit", tags=["audit-export"])

_state: dict[str, Any] = {}


def _managers() -> tuple[SnapshotManager, JobManager]:
    """惰性初始化：不访问审计接口就不会创建任何导出连接或目录。"""
    if "jobs" not in _state:
        settings.ensure()
        catalog = default_catalog()
        root = Path(settings.audit_root)
        snap_mgr = SnapshotManager(
            root=root / "snapshots",
            catalog=catalog,
            db_path=settings.db_path,
        )
        job_mgr = JobManager(
            root=root,
            snapshot_manager=snap_mgr,
            catalog=catalog,
        )
        job_mgr.start_worker()
        _state["snapshots"] = snap_mgr
        _state["jobs"] = job_mgr
        _state["catalog"] = catalog
    return _state["snapshots"], _state["jobs"]


def shutdown_worker() -> None:
    if "jobs" in _state:
        _state["jobs"].stop_worker()


# ---------------------------------------------------------------- 目录


@router.get("/datasets")
def list_datasets() -> dict[str, Any]:
    _, jobs = _managers()
    catalog = _state["catalog"]
    return {
        "datasets": [
            {
                "name": spec.name,
                "description": spec.description,
                "source_table": spec.source_table,
                "time_column": spec.time_column,
                "natural_key": list(spec.natural_key),
                "columns": [
                    {
                        "name": col.name,
                        "policy": col.policy,
                        "sensitive": col.sensitive,
                    }
                    for col in spec.columns
                ],
            }
            for name in catalog.names()
            for spec in [catalog.get(name)]
        ]
    }


# ---------------------------------------------------------------- 任务


@router.post("/exports")
def create_export(
    start_utc: str = Query(..., description="起始时间（UTC，含），如 2026-09-01 00:00:00"),
    end_utc: str = Query(..., description="结束时间（UTC，不含）"),
    datasets: str | None = Query(None, description="逗号分隔，默认全部目录数据集"),
    segment_size: int | None = Query(None, ge=1),
) -> dict[str, Any]:
    _, jobs = _managers()
    requested = [d.strip() for d in datasets.split(",") if d.strip()] if datasets else None
    try:
        manifest = jobs.create_job(start_utc, end_utc, requested, segment_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _public_manifest(manifest)


@router.get("/exports")
def list_exports() -> dict[str, Any]:
    _, jobs = _managers()
    return {"jobs": [_public_manifest(m) for m in jobs.list_jobs()]}


@router.get("/exports/{job_id}")
def get_export(job_id: str) -> dict[str, Any]:
    _, jobs = _managers()
    try:
        manifest = jobs.load_manifest(job_id)
    except JobNotFound as exc:
        raise HTTPException(status_code=404, detail="export job not found") from exc
    except SnapshotExpired as exc:  # pragma: no cover - 过期在执行期标记
        raise HTTPException(status_code=410, detail=str(exc)) from exc
    return _public_manifest(manifest)


@router.post("/exports/{job_id}/cancel")
def cancel_export(job_id: str) -> dict[str, Any]:
    _, jobs = _managers()
    try:
        manifest = jobs.request_cancel(job_id)
    except JobNotFound as exc:
        raise HTTPException(status_code=404, detail="export job not found") from exc
    return _public_manifest(manifest)


@router.post("/exports/{job_id}/resume")
def resume_export(job_id: str) -> dict[str, Any]:
    _, jobs = _managers()
    try:
        manifest = jobs.requeue(job_id)
    except JobNotFound as exc:
        raise HTTPException(status_code=404, detail="export job not found") from exc
    except SnapshotExpired as exc:
        raise HTTPException(status_code=410, detail=str(exc)) from exc
    return _public_manifest(manifest)


# ---------------------------------------------------------------- 下载与校验


@router.get("/exports/{job_id}/download")
def download_export(job_id: str):
    _, jobs = _managers()
    try:
        path, manifest = jobs.output_path(job_id)
    except JobNotFound as exc:
        raise HTTPException(status_code=404, detail="export job not found") from exc
    except JobNotReady as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    output = manifest["output"]
    headers = {
        "X-Export-Root-Hash": manifest["root_hash"],
        "X-Export-Sha256": output["sha256"],
        "X-Export-Row-Count": str(output["row_count"]),
        "X-Export-Dataset-Order": ",".join(output["dataset_order"]),
        "Accept-Ranges": "bytes",
    }
    # FileResponse 原生处理 Range：客户端中断后可从任意字节偏移续传，
    # 返回 206 Partial Content，无需重跑任务
    return FileResponse(
        path,
        media_type=output["media_type"],
        filename=output["filename"],
        headers=headers,
        stat_result=os.stat(path),
    )


@router.get("/exports/{job_id}/proof")
def export_proof(job_id: str) -> dict[str, Any]:
    """返回可独立重放的完整性证明。"""
    _, jobs = _managers()
    try:
        manifest = jobs.load_manifest(job_id)
    except JobNotFound as exc:
        raise HTTPException(status_code=404, detail="export job not found") from exc
    if manifest["status"] != COMPLETED:
        raise HTTPException(status_code=409, detail=f"job is {manifest['status']}")
    return {
        "job_id": job_id,
        "root_hash": manifest["root_hash"],
        "output": manifest["output"],
        "snapshots": [
            {
                "dataset": name,
                "snapshot_id": snap_id,
                "snapshot": _snapshot_view(jobs, snap_id),
                "segments": manifest["datasets"][name]["segments"],
                "chain_head": manifest["datasets"][name]["chain_head"],
                "stream_check": manifest["datasets"][name]["stream_check"],
                "snapshot_check": manifest["datasets"][name]["snapshot_check"],
                "cross_check_matched": manifest["datasets"][name]["cross_check_matched"],
            }
            for name, snap_id in manifest["snapshots"].items()
        ],
    }


# ---------------------------------------------------------------- 辅助


def _snapshot_view(jobs: JobManager, snapshot_id: str) -> dict[str, Any]:
    info = jobs.snapshots.get_registered(snapshot_id)
    return {
        "snapshot_id": info.snapshot_id,
        "dataset": info.dataset,
        "start_utc": info.start_utc,
        "end_utc": info.end_utc,
        "schema_version": info.schema_version,
        "row_count": info.row_count,
        "parquet_sha256": info.parquet_sha256,
        "parquet_size": info.parquet_size,
        "source_watermark": info.source_watermark,
        "created_at": info.created_at,
        "expires_at": info.expires_at,
        "expired": info.is_expired(),
    }


def _public_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """对外视图：剥离内部路径细节，保留进度与证明所需信息。"""
    return {
        "job_id": manifest["job_id"],
        "status": manifest["status"],
        "request": manifest["request"],
        "snapshots": manifest["snapshots"],
        "datasets": {
            name: {
                "state": state["state"],
                "segments_completed": len(state["segments"]),
                "rows_written": state["row_count"],
                "expected_row_count": state["expected_row_count"],
                "next_after_row_no": state["next_after_row_no"],
                "cross_check_matched": state["cross_check_matched"],
            }
            for name, state in manifest["datasets"].items()
        },
        "output": manifest["output"],
        "root_hash": manifest["root_hash"],
        "error": manifest["error"],
        "heartbeat_at": manifest["heartbeat_at"],
        "created_at": manifest["created_at"],
        "updated_at": manifest["updated_at"],
        "completed_at": manifest["completed_at"],
        "expires_at": manifest["expires_at"],
    }
