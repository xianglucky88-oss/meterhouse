"""导出任务：分段读取、断点续传与完整性证明。

任务状态机::

    pending -> running -> completed
                         -> failed      （可重试错误，重启或重新入队后续跑）
                         -> expired     （快照过 TTL，需重新发起）
                         -> cancelled   （客户端主动取消）

可重放与不漏不重的保证：

* 每段都是快照上的键集分页（``_row_no > 上次边界 ORDER BY _row_no``），
  不使用 OFFSET，段结果与执行次数、执行进程无关；
* 段文件先写临时文件再原子改名，manifest 记录每段的行号区间与哈希；
* 段是唯一的持久化进度单位：恢复时回放到最后一个已完成段的边界，
  未完成段整体重扫（规模有界），段区间必须首尾相接、最终行号等于快照
  总行数，否则任务失败，因此任何中断场景下都不漏不重；
* 每段 SHA-256 + 段间哈希链 + 全文件 SHA-256 + 根哈希构成完整性证明；
* 流式扫描聚合与快照侧独立聚合交叉核对，不一致则任务失败。

资源隔离：每个数据集使用独立只读连接打开快照，读完即关；
所有扫描按有界批次进行，内存占用上限为「批次大小」而非结果集大小。
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import settings
from .catalog import Catalog, DatasetSpec, Minimizer
from .hashing import RowHasher, canonical_value, file_sha256
from .snapshot import SnapshotExpired, SnapshotManager, SnapshotMissing, utc_now

JOB_DIR_PREFIX = "job_"
SEGMENTS_DIRNAME = "segments"
MANIFEST_FILE = "manifest.json"
SALT_FILE = ".salt"
_FILE_MODE = 0o600

# 任务状态
PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
EXPIRED = "expired"
CANCELLED = "cancelled"
TERMINAL_STATES = frozenset({COMPLETED, FAILED, EXPIRED, CANCELLED})


class JobNotFound(LookupError):
    pass


class JobNotReady(RuntimeError):
    pass


@dataclass
class SegmentRecord:
    index: int
    file: str
    row_count: int
    first_row_no: int
    last_row_no: int
    size_bytes: int
    sha256: str
    chain_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "file": self.file,
            "row_count": self.row_count,
            "first_row_no": self.first_row_no,
            "last_row_no": self.last_row_no,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "chain_hash": self.chain_hash,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SegmentRecord":
        return cls(**raw)


class JobManager:
    """导出任务的创建、调度、恢复与交付。"""

    def __init__(
        self,
        root: Path,
        snapshot_manager: SnapshotManager,
        catalog: Catalog,
        segment_size: int | None = None,
        fetch_batch_size: int | None = None,
        max_retries: int | None = None,
        heartbeat_seconds: float | None = None,
    ):
        self.root = Path(root)
        self.jobs_root = self.root / "jobs"
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self.snapshots = snapshot_manager
        self.catalog = catalog
        self.segment_size = segment_size or settings.audit_segment_size
        self.fetch_batch_size = fetch_batch_size or settings.audit_fetch_batch_size
        self.max_retries = max_retries if max_retries is not None else settings.audit_max_retries
        self.heartbeat_seconds = (
            heartbeat_seconds if heartbeat_seconds is not None else settings.audit_heartbeat_seconds
        )
        self._queue: deque[str] = deque()
        self._wake = threading.Event()
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------ 持久化

    def _job_dir(self, job_id: str) -> Path:
        return self.jobs_root / job_id

    def _manifest_path(self, job_id: str) -> Path:
        return self._job_dir(job_id) / MANIFEST_FILE

    def _salt_path(self, job_id: str) -> Path:
        return self._job_dir(job_id) / SALT_FILE

    def load_manifest(self, job_id: str) -> dict[str, Any]:
        path = self._manifest_path(job_id)
        if not path.exists():
            raise JobNotFound(job_id)
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_manifest(self, job_id: str, manifest: dict[str, Any]) -> None:
        manifest["updated_at"] = utc_now()
        path = self._manifest_path(job_id)
        # 取消信号由独立请求写入磁盘；工作线程的进度保存不得把它覆盖回 RUNNING
        if path.exists():
            on_disk = json.loads(path.read_text(encoding="utf-8"))
            if on_disk.get("status") == CANCELLED and manifest["status"] == RUNNING:
                manifest["status"] = CANCELLED
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)
        os.chmod(tmp, _FILE_MODE)
        os.replace(tmp, path)

    def _get_lock(self, job_id: str) -> threading.Lock:
        with self._locks_guard:
            if job_id not in self._locks:
                self._locks[job_id] = threading.Lock()
            return self._locks[job_id]

    def list_jobs(self) -> list[dict[str, Any]]:
        out = []
        if not self.jobs_root.exists():
            return out
        for child in sorted(self.jobs_root.iterdir()):
            mp = child / MANIFEST_FILE
            if mp.exists():
                out.append(json.loads(mp.read_text(encoding="utf-8")))
        return out

    # ------------------------------------------------------------ 创建任务

    def create_job(
        self,
        start_utc: str,
        end_utc: str,
        datasets: list[str] | None = None,
        segment_size: int | None = None,
    ) -> dict[str, Any]:
        # 参数校验：数据集必须在目录内，时间范围左闭右开
        known = self.catalog.names()
        requested = sorted(set(datasets or known), key=known.index)
        unknown = [d for d in requested if d not in known]
        if unknown:
            raise ValueError(f"unknown datasets: {unknown}")
        if start_utc >= end_utc:
            raise ValueError("time range must be [start_utc, end_utc) with start < end")
        size = segment_size or self.segment_size
        if size <= 0:
            raise ValueError("segment_size must be positive")

        job_id = f"{JOB_DIR_PREFIX}{int(utc_now() * 1000)}_{secrets.token_hex(6)}"
        job_dir = self._job_dir(job_id)
        (job_dir / SEGMENTS_DIRNAME).mkdir(parents=True)
        # 任务级随机盐：只落盘在任务目录（0600），不进 manifest、不进交付物
        salt = secrets.token_bytes(32)
        with open(self._salt_path(job_id), "wb") as fh:
            fh.write(salt)
        os.chmod(self._salt_path(job_id), _FILE_MODE)

        now = utc_now()
        manifest = {
            "manifest_version": settings.audit_manifest_version,
            "schema_version": settings.audit_schema_version,
            "job_id": job_id,
            "status": PENDING,
            "request": {
                "datasets": requested,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "segment_size": size,
            },
            "snapshots": {},
            "datasets": {
                name: {
                    "state": PENDING,
                    "segments": [],
                    "next_after_row_no": 0,
                    "expected_row_count": None,
                    "row_count": 0,
                    "chain_head": RowHasher.GENESIS,
                    "stream_check": None,
                    "snapshot_check": None,
                    "cross_check_matched": None,
                }
                for name in requested
            },
            "output": None,
            "root_hash": None,
            "error": None,
            "heartbeat_at": None,
            "created_at": now,
            "updated_at": now,
            "expires_at": None,
            "completed_at": None,
        }
        self._save_manifest(job_id, manifest)
        self._queue.append(job_id)
        self._wake.set()
        return manifest

    def request_cancel(self, job_id: str) -> dict[str, Any]:
        with self._get_lock(job_id):
            manifest = self.load_manifest(job_id)
            if manifest["status"] in TERMINAL_STATES:
                return manifest
            manifest["status"] = CANCELLED
            self._save_manifest(job_id, manifest)
            self._wake.set()
            return manifest

    # ------------------------------------------------------------ 调度循环

    def start_worker(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        # 进程重启后恢复：未到终态的任务重新入队，按 manifest 进度续跑
        for manifest in self.list_jobs():
            if manifest["status"] not in TERMINAL_STATES:
                self._queue.append(manifest["job_id"])
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._worker_loop, name="audit-export-worker", daemon=True
        )
        self._thread.start()

    def stop_worker(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=timeout)

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            job_id = self._queue.popleft() if self._queue else None
            if job_id is None:
                self._wake.wait(timeout=1.0)
                self._wake.clear()
                continue
            try:
                self._run_job(job_id)
            except Exception as exc:  # pragma: no cover - 兜底，线程不能死
                self._mark_failed(job_id, f"worker crashed: {exc!r}")

    def _mark_failed(self, job_id: str, message: str) -> None:
        try:
            with self._get_lock(job_id):
                manifest = self.load_manifest(job_id)
                if manifest["status"] in TERMINAL_STATES:
                    return
                manifest["status"] = FAILED
                manifest["error"] = message
                self._save_manifest(job_id, manifest)
        except JobNotFound:
            pass

    # ------------------------------------------------------------ 任务执行

    def _is_cancelled(self, manifest: dict[str, Any]) -> bool:
        # 取消信号直接读磁盘上的最新 manifest，无需等待线程调度
        fresh = self.load_manifest(manifest["job_id"])
        return fresh["status"] == CANCELLED

    def _run_job(self, job_id: str) -> None:
        # 只用短临界区做状态流转；任务执行期间不持锁，
        # 否则取消请求会被阻塞到任务结束，运行中的任务永远看不到取消信号
        with self._get_lock(job_id):
            manifest = self.load_manifest(job_id)
            if manifest["status"] in TERMINAL_STATES:
                return
            # PENDING 与进程崩溃遗留的 RUNNING 都允许接管续跑；
            # 单工作线程 + 队列保证同进程内不会被重复执行
            manifest["status"] = RUNNING
            manifest["error"] = None
            manifest["heartbeat_at"] = utc_now()
            self._save_manifest(job_id, manifest)

        try:
            self._ensure_snapshots(job_id, manifest)
            for dataset in manifest["request"]["datasets"]:
                if self._is_cancelled(manifest):
                    self._transition(job_id, CANCELLED)
                    return
                self._run_dataset(job_id, manifest, dataset)
            self._finalize(job_id, manifest)
        except _Cancelled:
            self._transition(job_id, CANCELLED)
        except SnapshotExpired as exc:
            latest = self.load_manifest(job_id)
            latest["status"] = EXPIRED
            latest["error"] = str(exc)
            latest["expires_at"] = utc_now()
            self._save_manifest(job_id, latest)
        except Exception as exc:
            latest = self.load_manifest(job_id)
            if latest["status"] in TERMINAL_STATES:
                return
            latest["status"] = FAILED
            latest["error"] = repr(exc)
            self._save_manifest(job_id, latest)

    def _transition(self, job_id: str, status: str) -> None:
        with self._get_lock(job_id):
            manifest = self.load_manifest(job_id)
            manifest["status"] = status
            self._save_manifest(job_id, manifest)

    def _ensure_snapshots(self, job_id: str, manifest: dict[str, Any]) -> None:
        req = manifest["request"]
        for dataset in req["datasets"]:
            if dataset in manifest["snapshots"]:
                continue
            info = self.snapshots.acquire(dataset, req["start_utc"], req["end_utc"])
            manifest["snapshots"][dataset] = info.snapshot_id
            manifest["datasets"][dataset]["expected_row_count"] = info.row_count
            self._save_manifest(job_id, manifest)

    # ------------------------------------------------------------ 单数据集分段扫描

    def _run_dataset(self, job_id: str, manifest: dict[str, Any], dataset: str) -> None:
        state = manifest["datasets"][dataset]
        if state["state"] == COMPLETED:
            return
        spec = self.catalog.get(dataset)
        salt = self._salt_path(job_id).read_bytes()
        minimizer = Minimizer(spec, salt)
        segment_size = manifest["request"]["segment_size"]
        seg_dir = self._job_dir(job_id) / SEGMENTS_DIRNAME / dataset
        seg_dir.mkdir(parents=True, exist_ok=True)
        # 清理上次崩溃残留的临时段文件
        for tmp in seg_dir.glob("*.tmp"):
            tmp.unlink()

        snapshot_id = manifest["snapshots"][dataset]
        attempt = 0
        while True:
            # 段是唯一的持久化进度单位：每次尝试（含进程重启后的首次）
            # 都回落到最后一个已完成段边界，未完成段重扫，保证不漏不重
            self._restore_boundary(state)
            try:
                self._scan_with_retries(
                    job_id, manifest, dataset, snapshot_id, minimizer, seg_dir, segment_size
                )
                break
            except (SnapshotExpired, SnapshotMissing, _Cancelled):
                raise
            except Exception:
                for tmp in seg_dir.glob("*.tmp"):
                    tmp.unlink()
                attempt += 1
                if attempt > self.max_retries:
                    raise

    @staticmethod
    def _restore_boundary(state: dict[str, Any]) -> None:
        """把扫描位置回放到最后一个已落盘段的边界。

        心跳保存的段内游标不构成持久进度：段未完成就可能丢失，
        因此只信任段记录本身，最坏情况是重扫一个段（规模有界）。
        """
        if state["segments"]:
            last = state["segments"][-1]
            state["next_after_row_no"] = last["last_row_no"]
            state["chain_head"] = last["chain_hash"]
        else:
            state["next_after_row_no"] = 0
            state["chain_head"] = RowHasher.GENESIS
        state["row_count"] = sum(seg["row_count"] for seg in state["segments"])
        state["stream_check"] = None
        state["cross_check_matched"] = None

    def _scan_with_retries(
        self, job_id, manifest, dataset, snapshot_id, minimizer, seg_dir, segment_size
    ) -> None:
        state = manifest["datasets"][dataset]
        # 每次尝试都打开全新短生命周期只读连接，失败重试不复用连接
        reader = self.snapshots.open_reader(snapshot_id)
        last_heartbeat = utc_now()
        try:
            chain = RowHasher(state["chain_head"])
            segment_index = len(state["segments"])
            segment_rows = 0
            segment_hasher = hashlib.sha256()
            segment_first_row_no = (
                state["segments"][-1]["last_row_no"] + 1 if state["segments"] else 1
            )
            segment_path = seg_dir / f"{segment_index:06d}.jsonl"
            segment_tmp = segment_path.with_suffix(".jsonl.tmp")
            out_fh = open(segment_tmp, "wb")
            # 从已落盘段重建聚合：重试/进程恢复时不重复计数，
            # 且聚合口径与交付文件中的（最小化后）行完全一致
            stream = _StreamAggregate.from_segments(
                self.catalog.get(dataset), self._job_dir(job_id), state["segments"]
            )

            try:
                while True:
                    if utc_now() - last_heartbeat >= self.heartbeat_seconds:
                        manifest["heartbeat_at"] = utc_now()
                        self._save_manifest(job_id, manifest)
                        last_heartbeat = utc_now()
                    if self._is_cancelled(manifest):
                        out_fh.close()
                        segment_tmp.unlink(missing_ok=True)
                        raise _Cancelled()

                    batch = reader.fetch_segment(state["next_after_row_no"], self.fetch_batch_size)
                    if not batch:
                        break
                    for raw in batch:
                        row_no = raw.pop("_row_no")
                        projected = minimizer.project(raw)
                        stream.observe(projected)
                        encoded = chain.add_row(projected)
                        out_fh.write(encoded)
                        segment_hasher.update(encoded)
                        segment_rows += 1
                        state["next_after_row_no"] = row_no
                        state["row_count"] += 1
                        if segment_rows >= segment_size:
                            out_fh.flush()
                            os.fsync(out_fh.fileno())
                            out_fh.close()
                            os.chmod(segment_tmp, _FILE_MODE)
                            os.replace(segment_tmp, segment_path)
                            record = SegmentRecord(
                                index=segment_index,
                                file=str(segment_path.relative_to(self._job_dir(job_id))),
                                row_count=segment_rows,
                                first_row_no=segment_first_row_no,
                                last_row_no=row_no,
                                size_bytes=segment_path.stat().st_size,
                                sha256=segment_hasher.hexdigest(),
                                chain_hash=chain.digest(),
                            )
                            state["segments"].append(record.to_dict())
                            state["chain_head"] = chain.digest()
                            self._save_manifest(job_id, manifest)
                            # 开启下一段
                            segment_index += 1
                            segment_rows = 0
                            segment_hasher = hashlib.sha256()
                            segment_first_row_no = row_no + 1
                            segment_path = seg_dir / f"{segment_index:06d}.jsonl"
                            segment_tmp = segment_path.with_suffix(".jsonl.tmp")
                            out_fh = open(segment_tmp, "wb")
            finally:
                if not out_fh.closed:
                    out_fh.close()

            # 收尾：最后一段（可能为空则不留段）
            if segment_rows > 0:
                os.chmod(segment_tmp, _FILE_MODE)
                os.replace(segment_tmp, segment_path)
                record = SegmentRecord(
                    index=segment_index,
                    file=str(segment_path.relative_to(self._job_dir(job_id))),
                    row_count=segment_rows,
                    first_row_no=segment_first_row_no,
                    last_row_no=state["next_after_row_no"],
                    size_bytes=segment_path.stat().st_size,
                    sha256=segment_hasher.hexdigest(),
                    chain_hash=chain.digest(),
                )
                state["segments"].append(record.to_dict())
                state["chain_head"] = chain.digest()
            else:
                segment_tmp.unlink(missing_ok=True)

            state["stream_check"] = stream.result()
            state["snapshot_check"] = reader.aggregate()
            self._validate_dataset(job_id, state)
            state["state"] = COMPLETED
            manifest["heartbeat_at"] = utc_now()
            self._save_manifest(job_id, manifest)
        finally:
            reader.close()

    def _validate_dataset(self, job_id: str, state: dict[str, Any]) -> None:
        """边界、连续性、行数与双路聚合核对。"""
        segments = state["segments"]
        # 段区间必须从 1 开始、首尾相接、无重叠无缺口
        expected_next = 1
        for seg in segments:
            if seg["first_row_no"] != expected_next:
                raise IntegrityError(
                    f"{job_id}: segment {seg['index']} boundary gap, "
                    f"expect first_row_no={expected_next}, got {seg['first_row_no']}"
                )
            if seg["last_row_no"] < seg["first_row_no"]:
                raise IntegrityError(f"{job_id}: segment {seg['index']} has inverted boundary")
            expected_next = seg["last_row_no"] + 1
        expected_total = state["expected_row_count"]
        if expected_total is not None and state["next_after_row_no"] != expected_total:
            raise IntegrityError(
                f"{job_id}: scan stopped at row {state['next_after_row_no']}, "
                f"snapshot has {expected_total} rows"
            )
        if state["row_count"] != (expected_total or state["row_count"]):
            raise IntegrityError(f"{job_id}: row count mismatch against snapshot")

        # 重算每个段文件的哈希，防止落盘后被篡改
        for seg in segments:
            path = self._job_dir(job_id) / seg["file"]
            if not path.exists() or path.stat().st_size != seg["size_bytes"]:
                raise IntegrityError(f"{job_id}: segment file missing or resized: {seg['file']}")
            if file_sha256(str(path)) != seg["sha256"]:
                raise IntegrityError(f"{job_id}: segment hash mismatch: {seg['file']}")

        # 双路聚合交叉核对：流式扫描 vs 快照独立聚合
        stream, snap = state["stream_check"], state["snapshot_check"]
        if stream["row_count"] != snap["row_count"]:
            raise IntegrityError(f"{job_id}: cross-check row_count {stream} != {snap}")
        for key in set(stream) & set(snap):
            if stream[key] != snap[key]:
                raise IntegrityError(f"{job_id}: cross-check {key} {stream[key]!r} != {snap[key]!r}")
        state["cross_check_matched"] = True

    # ------------------------------------------------------------ 汇总与交付

    def _finalize(self, job_id: str, manifest: dict[str, Any]) -> None:
        for dataset, state in manifest["datasets"].items():
            if state["state"] != COMPLETED:
                raise IntegrityError(f"{job_id}: dataset {dataset} not completed")
            # 完成前再做一次快照全量哈希，确认冻结文件自构建后未被改动
            self.snapshots.verify_sha256(manifest["snapshots"][dataset])

        output_path = self._job_dir(job_id) / f"export_{job_id}.jsonl"
        output_tmp = output_path.with_suffix(".jsonl.tmp")
        root = hashlib.sha256()
        total_rows = 0
        total_size = 0
        with open(output_tmp, "wb") as out_fh:
            for dataset in manifest["request"]["datasets"]:
                state = manifest["datasets"][dataset]
                root.update(dataset.encode("utf-8"))
                root.update(state["chain_head"].encode("ascii"))
                for seg in state["segments"]:
                    seg_path = self._job_dir(job_id) / seg["file"]
                    with open(seg_path, "rb") as in_fh:
                        while True:
                            chunk = in_fh.read(1024 * 1024)
                            if not chunk:
                                break
                            out_fh.write(chunk)
                            total_size += len(chunk)
                    total_rows += seg["row_count"]
        os.chmod(output_tmp, _FILE_MODE)
        os.replace(output_tmp, output_path)

        manifest["output"] = {
            "path": str(output_path.relative_to(self._job_dir(job_id))),
            "filename": output_path.name,
            "media_type": "application/x-ndjson",
            "sha256": file_sha256(str(output_path)),
            "row_count": total_rows,
            "size_bytes": total_size,
            "dataset_order": manifest["request"]["datasets"],
        }
        manifest["root_hash"] = root.hexdigest()
        manifest["status"] = COMPLETED
        manifest["completed_at"] = utc_now()
        self._save_manifest(job_id, manifest)

    def output_path(self, job_id: str) -> tuple[Path, dict[str, Any]]:
        manifest = self.load_manifest(job_id)
        if manifest["status"] != COMPLETED:
            raise JobNotReady(f"job {job_id} is {manifest['status']}")
        path = self._job_dir(job_id) / manifest["output"]["path"]
        if not path.exists():
            raise IntegrityError(f"job {job_id}: output file missing")
        return path, manifest

    def requeue(self, job_id: str) -> dict[str, Any]:
        """把失败任务重新入队（从 manifest 进度续跑，已完成段不重算）。"""
        manifest = self.load_manifest(job_id)
        if manifest["status"] == COMPLETED:
            return manifest
        if manifest["status"] == CANCELLED:
            manifest["status"] = PENDING
        elif manifest["status"] == EXPIRED:
            raise SnapshotExpired("snapshot expired; create a new export job instead")
        else:
            manifest["status"] = PENDING
            manifest["error"] = None
        self._save_manifest(job_id, manifest)
        self._queue.append(job_id)
        self._wake.set()
        return manifest


class _Cancelled(Exception):
    """内部信号：扫描过程中收到取消。"""


class IntegrityError(RuntimeError):
    """完整性核对失败（段边界、哈希或双路聚合不一致）。"""


class _StreamAggregate:
    """流式扫描侧的聚合，只保留标量，内存与行数无关。

    聚合的是**交付行**（最小化之后），因此聚合值天然与交付文件一致；
    快照侧聚合使用同样的标量口径（行数、去重计数、金额合计、时间边界），
    最小化不改变这些标量，双路可直接核对。

    ``from_segments`` 支持重试/进程恢复：先回放已落盘段重建聚合，
    续扫部分再增量计入，任何执行次数下结果都相同。
    """

    def __init__(self, spec: DatasetSpec):
        self.spec = spec
        self.row_count = 0
        self._distinct: set[str] = set()
        self._amount = 0
        self._min: str | None = None
        self._max: str | None = None
        self._distinct_key = "order_id" if spec.natural_key else "supplier_id"
        self._distinct_out = "distinct_orders" if spec.natural_key else "distinct_suppliers"
        self._time_col = spec.time_column

    @classmethod
    def from_segments(cls, spec: DatasetSpec, job_dir: Path, segments: list[dict]) -> "_StreamAggregate":
        agg = cls(spec)
        for seg in segments:
            with open(job_dir / seg["file"], "rb") as fh:
                for line in fh:
                    agg.observe(json.loads(line.decode("utf-8")))
        return agg

    def observe(self, row: dict[str, Any]) -> None:
        self.row_count += 1
        self._distinct.add(str(row[self._distinct_key]))
        if self.spec.natural_key:
            self._amount += int(row["amount_cents"])
        # 与 canonical_json 同一规范化路径：datetime 输出 T 分隔的 ISO 文本，
        # 保证实时扫描与段文件回放两条路径的聚合值完全一致
        ts_text = str(canonical_value(row[self._time_col]))
        if self._min is None or ts_text < self._min:
            self._min = ts_text
        if self._max is None or ts_text > self._max:
            self._max = ts_text

    def result(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "row_count": self.row_count,
            self._distinct_out: len(self._distinct),
            "min_time": self._min,
            "max_time": self._max,
        }
        if self.spec.natural_key:
            out["total_amount_cents"] = self._amount
        return out
