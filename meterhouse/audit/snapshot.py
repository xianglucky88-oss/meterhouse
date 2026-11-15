"""稳定快照层。

导出不是直接扫在线表，而是先把查询范围的数据冻结为不可变 Parquet 快照：

* 快照构建是一次短事务式的只读拷贝，构建完成后文件不再变化，
  导出期间底层表怎么改都不会让同一任务出现矛盾视图；
* 快照带 TTL，打开前校验文件状态与过期时间，过期显式失败；
* 注册表（``registry.json``）记录全部快照及其指纹，支持进程重启后恢复；
* 相同（数据集, 时间范围, schema 版本, 源数据水位）的请求复用快照，
  构建过程按指纹加锁，避免并发重复冻结。

资源隔离：快照构建与后续分段读取均使用独立的短生命周期 DuckDB 连接，
不借用交互查询的连接对象；连接设置有界内存上限，构建结束立即关闭。
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from ..config import settings
from .catalog import Catalog, DatasetSpec

REGISTRY_FILE = "registry.json"
SNAPSHOT_DIR_PREFIX = "snap_"
# 快照文件与注册表不含交付假名，但属于审计中间产物，收紧权限
_FILE_MODE = 0o600


def utc_now() -> float:
    return time.time()


@dataclass
class SnapshotInfo:
    """注册表中的一条快照记录。"""

    snapshot_id: str
    dataset: str
    start_utc: str
    end_utc: str
    schema_version: int
    # 构建时刻源表水位（行数 + 最大时间戳），用于判断能否复用
    source_watermark: dict[str, Any]
    parquet_path: str
    parquet_size: int
    parquet_sha256: str
    row_count: int
    # 构建 SQL 文本本身纳入指纹，目录逻辑变更后旧快照不会被误复用
    build_sql: str
    created_at: float
    expires_at: float

    def fingerprint(self) -> str:
        """复用指纹：范围、schema 与构建逻辑一致才视为同一份快照。"""
        parts = [
            self.dataset,
            self.start_utc,
            self.end_utc,
            str(self.schema_version),
            self.build_sql,
            json.dumps(self.source_watermark, sort_keys=True, ensure_ascii=False),
        ]
        import hashlib

        return hashlib.sha256("\u001f".join(parts).encode("utf-8")).hexdigest()

    def is_expired(self, now: float | None = None) -> bool:
        return (now or utc_now()) >= self.expires_at

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SnapshotInfo":
        return cls(**raw)


class SnapshotExpired(RuntimeError):
    """快照已过 TTL，任务不可继续，需要重新发起导出。"""


class SnapshotMissing(RuntimeError):
    """快照文件或注册表记录丢失。"""


class SnapshotManager:
    """快照的构建、注册、复用与过期管理。"""

    def __init__(
        self,
        root: Path,
        catalog: Catalog,
        db_path: Path,
        ttl_seconds: int | None = None,
        memory_limit: str | None = None,
        threads: int | None = None,
    ):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.catalog = catalog
        self.db_path = Path(db_path)
        self.ttl_seconds = ttl_seconds if ttl_seconds is not None else settings.audit_snapshot_ttl_seconds
        self.memory_limit = memory_limit or settings.audit_duckdb_memory_limit
        self.threads = threads if threads is not None else settings.audit_duckdb_threads
        self._registry_path = self.root / REGISTRY_FILE
        self._registry: dict[str, SnapshotInfo] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._registry_lock = threading.Lock()
        self._load_registry()
        # 进程启动时顺带回收上次运行遗留的过期快照
        self.purge_expired()

    # ------------------------------------------------------------ 注册表

    def _load_registry(self) -> None:
        if not self._registry_path.exists():
            return
        raw = json.loads(self._registry_path.read_text(encoding="utf-8"))
        self._registry = {
            sid: SnapshotInfo.from_dict(item) for sid, item in raw.get("snapshots", {}).items()
        }

    def _persist_registry_locked(self) -> None:
        payload = {
            "version": 1,
            "updated_at": utc_now(),
            "snapshots": {sid: info.to_dict() for sid, info in sorted(self._registry.items())},
        }
        tmp = self._registry_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        os.chmod(tmp, _FILE_MODE)
        os.replace(tmp, self._registry_path)

    def get_registered(self, snapshot_id: str) -> SnapshotInfo:
        info = self._registry.get(snapshot_id)
        if info is None:
            raise SnapshotMissing(f"snapshot {snapshot_id} not found in registry")
        return info

    def list_snapshots(self) -> list[SnapshotInfo]:
        return [self._registry[k] for k in sorted(self._registry)]

    # ------------------------------------------------------------ 连接

    def _new_connection(self, read_only: bool) -> duckdb.DuckDBPyConnection:
        """导出专用连接：有界内存、限线程、短生命周期，用完即关。

        与交互查询的全局连接完全独立，不占用也不污染其连接状态。
        """
        con = duckdb.connect(str(self.db_path), read_only=read_only)
        con.execute(f"SET memory_limit = '{self.memory_limit}'")
        con.execute(f"SET threads = {self.threads}")
        return con

    # ------------------------------------------------------------ 水位与复用

    def _source_watermark(self, con: duckdb.DuckDBPyConnection, spec: DatasetSpec) -> dict[str, Any]:
        row = con.execute(
            f"SELECT count(*), coalesce(max({spec.time_column}), NULL) FROM {spec.source_table}"
        ).fetchone()
        return {"row_count": int(row[0]), "max_time": row[1].isoformat() if row[1] else None}

    def _fingerprint_for(
        self, spec: DatasetSpec, start_utc: str, end_utc: str, watermark: dict[str, Any], sql: str
    ) -> str:
        probe = SnapshotInfo(
            snapshot_id="",
            dataset=spec.name,
            start_utc=start_utc,
            end_utc=end_utc,
            schema_version=settings.audit_schema_version,
            source_watermark=watermark,
            parquet_path="",
            parquet_size=0,
            parquet_sha256="",
            row_count=0,
            build_sql=sql,
            created_at=0.0,
            expires_at=0.0,
        )
        return probe.fingerprint()

    def acquire(
        self, dataset: str, start_utc: str, end_utc: str
    ) -> SnapshotInfo:
        """获取一份可用快照：命中未过期快照则复用，否则构建新快照。"""
        spec = self.catalog.get(dataset)
        sql = spec.build_sql(start_utc, end_utc)

        # 水位探测用独立只读连接，快速关闭
        probe_con = self._new_connection(read_only=True)
        try:
            watermark = self._source_watermark(probe_con, spec)
        finally:
            probe_con.close()

        fp = self._fingerprint_for(spec, start_utc, end_utc, watermark, sql)
        lock = self._get_lock(fp)
        with lock:
            with self._registry_lock:
                for info in self._registry.values():
                    if info.fingerprint() == fp and not info.is_expired():
                        if self._intact(info):
                            return info
            return self._build(spec, start_utc, end_utc, sql, watermark)

    def _get_lock(self, fingerprint: str) -> threading.Lock:
        with self._registry_lock:
            if fingerprint not in self._locks:
                self._locks[fingerprint] = threading.Lock()
            return self._locks[fingerprint]

    # ------------------------------------------------------------ 构建

    def _build(
        self,
        spec: DatasetSpec,
        start_utc: str,
        end_utc: str,
        sql: str,
        watermark: dict[str, Any],
    ) -> SnapshotInfo:
        snapshot_id = f"{SNAPSHOT_DIR_PREFIX}{spec.name}_{int(utc_now() * 1000)}_{os.urandom(4).hex()}"
        snap_dir = self.root / snapshot_id
        snap_dir.mkdir(parents=True)
        parquet_path = snap_dir / f"{spec.name}.parquet"

        # 构建使用独立连接：COPY 是流式落盘，不把结果集拉进 Python 内存
        con = self._new_connection(read_only=True)
        try:
            con.execute(
                f"COPY ({sql}) TO '{parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            row_count = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_path.as_posix()}')").fetchone()[0]
        finally:
            con.close()

        from .hashing import file_sha256

        digest = file_sha256(str(parquet_path))
        os.chmod(parquet_path, _FILE_MODE)
        now = utc_now()
        info = SnapshotInfo(
            snapshot_id=snapshot_id,
            dataset=spec.name,
            start_utc=start_utc,
            end_utc=end_utc,
            schema_version=settings.audit_schema_version,
            source_watermark=watermark,
            parquet_path=str(parquet_path),
            parquet_size=parquet_path.stat().st_size,
            parquet_sha256=digest,
            row_count=int(row_count),
            build_sql=sql,
            created_at=now,
            expires_at=now + self.ttl_seconds,
        )
        with self._registry_lock:
            self._registry[snapshot_id] = info
            self._persist_registry_locked()
        return info

    # ------------------------------------------------------------ 校验与打开

    def _intact(self, info: SnapshotInfo) -> bool:
        path = Path(info.parquet_path)
        if not path.exists():
            return False
        if path.stat().st_size != info.parquet_size:
            return False
        return True

    def open_reader(self, snapshot_id: str) -> "SnapshotReader":
        """打开快照供分段读取；过期或文件被篡改时显式失败。"""
        info = self.get_registered(snapshot_id)
        if info.is_expired():
            raise SnapshotExpired(
                f"snapshot {snapshot_id} expired at {info.expires_at:.0f}; "
                "请重新发起导出任务"
            )
        path = Path(info.parquet_path)
        if not path.exists():
            raise SnapshotMissing(f"parquet file missing for snapshot {snapshot_id}")
        if path.stat().st_size != info.parquet_size:
            raise SnapshotMissing(f"snapshot {snapshot_id} size mismatch; file may be corrupted")
        con = self._new_connection(read_only=True)
        try:
            actual = con.execute(
                f"SELECT count(*) FROM read_parquet('{path.as_posix()}')"
            ).fetchone()[0]
            if int(actual) != info.row_count:
                raise SnapshotMissing(f"snapshot {snapshot_id} row count mismatch")
        except Exception:
            con.close()
            raise
        return SnapshotReader(info, con, self.catalog.get(info.dataset))

    def verify_sha256(self, snapshot_id: str) -> str:
        """全量重算 Parquet 哈希（任务完成时调用，平时只做轻量状态校验）。"""
        from .hashing import file_sha256

        info = self.get_registered(snapshot_id)
        actual = file_sha256(info.parquet_path)
        if actual != info.parquet_sha256:
            raise SnapshotMissing(f"snapshot {snapshot_id} sha256 mismatch")
        return actual

    def purge_expired(self) -> int:
        """清理过期快照的文件与注册表记录，返回清理条数。"""
        removed = 0
        with self._registry_lock:
            dead = [sid for sid, info in self._registry.items() if info.is_expired()]
            for sid in dead:
                info = self._registry.pop(sid)
                path = Path(info.parquet_path)
                if path.exists():
                    path.unlink()
                parent = path.parent
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                removed += 1
            if dead:
                self._persist_registry_locked()
        return removed


class SnapshotReader:
    """快照上的只读分段游标，关闭即释放独立连接。"""

    def __init__(
        self,
        info: SnapshotInfo,
        con: duckdb.DuckDBPyConnection,
        spec: DatasetSpec,
    ):
        self.info = info
        self._con = con
        self.spec = spec

    @property
    def parquet_ref(self) -> str:
        return f"read_parquet('{Path(self.info.parquet_path).as_posix()}')"

    def fetch_segment(self, after_row_no: int, limit: int) -> list[dict[str, Any]]:
        """键集分页：取 _row_no > after_row_no 的前 limit 行，全序确定。

        不使用 OFFSET：每段成本与范围起点无关，重放任意分段结果一致。
        """
        if limit <= 0:
            raise ValueError("segment limit must be positive")
        cols = ", ".join(("_row_no",) + self.spec.column_names)
        sql = (
            f"SELECT {cols} FROM {self.parquet_ref} "
            f"WHERE _row_no > {int(after_row_no)} "
            f"ORDER BY _row_no LIMIT {int(limit)}"
        )
        rows = self._con.execute(sql).fetchall()
        names = [d[0] for d in self._con.description]
        return [dict(zip(names, r)) for r in rows]

    def aggregate(self) -> dict[str, Any]:
        """快照侧独立聚合（交叉核对的基准值）。"""
        sql = self.spec.cross_check_sql().format(snapshot=self.parquet_ref)
        rows = self._con.execute(sql).fetchall()
        names = [d[0] for d in self._con.description]
        out = dict(zip(names, rows[0]))
        return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in out.items()}

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> "SnapshotReader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
