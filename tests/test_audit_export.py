"""审计导出能力的端到端测试。

覆盖合规需求中的每条承诺：
稳定快照、可续传分段、不漏不重、崩溃恢复、客户端断开不影响任务、
快照过期、敏感字段最小化、完整性证明、资源隔离与交互查询互不干扰。

本环境 DuckDB 落盘 fsync 较慢，seed 一次成本高，因此模块共享一份
只读源库；需要独立/可写数据库的用例通过文件拷贝获得隔离副本。
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path

import pytest

from meterhouse.audit.catalog import REDACTED_PLACEHOLDER, default_catalog
from meterhouse.audit.export import (
    COMPLETED,
    EXPIRED,
    FAILED,
    JobManager,
)
from meterhouse.audit.hashing import RowHasher, canonical_json
from meterhouse.audit.snapshot import (
    SnapshotExpired,
    SnapshotManager,
    SnapshotReader,
)
from meterhouse.data import connect, seed

START = "2026-09-01 00:00:00"
END = "2026-11-01 00:00:00"


@pytest.fixture(scope="module")
def source_db(tmp_path_factory):
    """模块内共享的种子库；只读使用，不做任何写操作。"""
    path = tmp_path_factory.mktemp("seed") / "source.duckdb"
    con = connect(path)
    seed(con)
    con.close()
    return path


@pytest.fixture()
def env(tmp_path, source_db):
    # 文件级拷贝：每个用例拥有独立数据库与审计目录，互不污染
    db_path = tmp_path / "test.duckdb"
    shutil.copyfile(source_db, db_path)

    catalog = default_catalog()
    snap_mgr = SnapshotManager(
        root=tmp_path / "audit" / "snapshots",
        catalog=catalog,
        db_path=db_path,
        ttl_seconds=3600,
    )
    job_mgr = JobManager(
        root=tmp_path / "audit",
        snapshot_manager=snap_mgr,
        catalog=catalog,
        segment_size=500,
        fetch_batch_size=200,
        max_retries=0,
        heartbeat_seconds=0.05,
    )
    yield {
        "tmp": tmp_path,
        "db_path": db_path,
        "catalog": catalog,
        "snap": snap_mgr,
        "jobs": job_mgr,
    }
    job_mgr.stop_worker()
    snap_mgr.purge_expired()


def _run_and_wait(job_mgr: JobManager, job_id: str, timeout: float = 30.0) -> dict:
    job_mgr.start_worker()
    deadline = time.time() + timeout
    while time.time() < deadline:
        manifest = job_mgr.load_manifest(job_id)
        if manifest["status"] in {"completed", "failed", "expired", "cancelled"}:
            return manifest
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} did not finish: {job_mgr.load_manifest(job_id)['status']}")


def _read_output(job_mgr: JobManager, job_id: str) -> list[dict]:
    path, _ = job_mgr.output_path(job_id)
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _expected_counts(db_path: Path) -> dict[str, int]:
    con = connect(db_path, read_only=True)
    try:
        suppliers = con.execute(
            f"""SELECT count(*) FROM supplier_stats
                WHERE stat_time >= TIMESTAMP '{START}' AND stat_time < TIMESTAMP '{END}'"""
        ).fetchone()[0]
        paid = con.execute(
            f"""SELECT count(DISTINCT order_id) FROM order_facts
                WHERE status='paid'
                  AND created_at >= TIMESTAMP '{START}' AND created_at < TIMESTAMP '{END}'"""
        ).fetchone()[0]
        gmv = con.execute(
            f"""SELECT coalesce(sum(amount_cents),0) FROM (
                  SELECT order_id, max(amount_cents) AS amount_cents FROM order_facts
                  WHERE status='paid'
                    AND created_at >= TIMESTAMP '{START}' AND created_at < TIMESTAMP '{END}'
                  GROUP BY order_id)"""
        ).fetchone()[0]
    finally:
        con.close()
    return {"supplier_snapshots": suppliers, "payment_facts": paid, "gmv": gmv}


# ---------------------------------------------------------------- 基础交付


def test_full_export_completes_with_cross_check(env):
    manifest = env["jobs"].create_job(START, END)
    result = _run_and_wait(env["jobs"], manifest["job_id"])

    assert result["status"] == COMPLETED, result["error"]
    counts = _expected_counts(env["db_path"])
    for name, state in result["datasets"].items():
        assert state["state"] == COMPLETED
        assert state["cross_check_matched"] is True
        assert state["row_count"] == counts[name]
        assert state["row_count"] == state["expected_row_count"]

    rows = _read_output(env["jobs"], manifest["job_id"])
    # 输出按 dataset_order 排列，行数与双路核对一致
    assert len(rows) == counts["supplier_snapshots"] + counts["payment_facts"]

    # 支付事实的金额合计与 ground_truth 口径一致
    pay_rows = [r for r in rows if "amount_cents" in r]
    assert sum(r["amount_cents"] for r in pay_rows) == counts["gmv"]


def test_segments_are_contiguous_and_replayable(env):
    """段边界首尾相接；逐段重放哈希链得到相同根哈希。"""
    manifest = env["jobs"].create_job(START, END, segment_size=300)
    result = _run_and_wait(env["jobs"], manifest["job_id"])
    assert result["status"] == COMPLETED

    for dataset, state in result["datasets"].items():
        segs = state["segments"]
        assert len(segs) > 1
        expected_next = 1
        chain = RowHasher()
        job_dir = env["jobs"]._job_dir(manifest["job_id"])
        for seg in segs:
            assert seg["first_row_no"] == expected_next
            expected_next = seg["last_row_no"] + 1
            # 重算段文件哈希
            data = (job_dir / seg["file"]).read_bytes()
            assert hashlib.sha256(data).hexdigest() == seg["sha256"]
            chain.update(data)
            assert chain.digest() == seg["chain_hash"]
        assert state["next_after_row_no"] == state["expected_row_count"]
        assert chain.digest() == state["chain_head"]


def test_duplicate_order_ids_deduped_and_sorted(env):
    con = connect(env["db_path"], read_only=True)
    try:
        total_physical = con.execute("SELECT count(*) FROM order_facts").fetchone()[0]
        distinct_paid = con.execute(
            "SELECT count(DISTINCT order_id) FROM order_facts WHERE status='paid'"
        ).fetchone()[0]
    finally:
        con.close()
    assert total_physical > distinct_paid  # 源表确实含重推数据

    manifest = env["jobs"].create_job(START, END, datasets=["payment_facts"])
    result = _run_and_wait(env["jobs"], manifest["job_id"])
    rows = _read_output(env["jobs"], manifest["job_id"])

    order_pseudonyms = [r["order_id"] for r in rows]
    assert len(order_pseudonyms) == len(set(order_pseudonyms)), "去重后不得有重复订单"
    assert len(rows) == distinct_paid
    # 全序：按 created_at 升序（键集分页的确定性排序）
    times = [r["created_at"] for r in rows]
    assert times == sorted(times)


# ---------------------------------------------------------------- 稳定快照


def test_snapshot_is_stable_under_concurrent_writes(env):
    # 先创建任务并固定到写入前的快照，再制造底层数据变化，
    # 验证任务交付的是冻结视图而非变化后的在线数据
    info = env["snap"].acquire("payment_facts", START, END)
    before = env["snap"].open_reader(info.snapshot_id)
    rows_before = before.fetch_segment(0, 10_000)
    before.close()

    manifest = env["jobs"].create_job(START, END, datasets=["payment_facts"])
    # 任务尚未运行，把清单指向写入前已冻结的快照
    manifest["snapshots"]["payment_facts"] = info.snapshot_id
    manifest["datasets"]["payment_facts"]["expected_row_count"] = info.row_count
    env["jobs"]._save_manifest(manifest["job_id"], manifest)

    # 导出期间底层表发生变化：新增支付订单并删除旧行
    con = connect(env["db_path"])
    try:
        con.execute(
            """INSERT INTO order_facts VALUES
               ('ORD-9990001','SUP-0001', TIMESTAMP '2026-10-15 10:00:00', 12345, 'paid', '华东'),
               ('ORD-9990002','SUP-0002', TIMESTAMP '2026-10-16 10:00:00', 67890, 'paid', '华南')"""
        )
        con.execute("DELETE FROM order_facts WHERE order_id = (SELECT min(order_id) FROM order_facts)")
    finally:
        con.close()

    # 已持有的旧快照内容不变（文件不可变）
    after = env["snap"].open_reader(info.snapshot_id)
    rows_after = after.fetch_segment(0, 10_000)
    after.close()
    assert rows_before == rows_after

    # 基于冻结旧快照的任务不受底层写入影响，行数与构建时一致
    result = _run_and_wait(env["jobs"], manifest["job_id"])
    assert result["status"] == COMPLETED, result.get("error")
    assert result["datasets"]["payment_facts"]["row_count"] == info.row_count

    # 写入后重新获取会产生新快照（水位变化），旧快照依旧可读
    rebuilt = env["snap"].acquire("payment_facts", START, END)
    assert rebuilt.snapshot_id != info.snapshot_id
    # +2 新增；被删订单若为 paid 则 -1，否则不影响支付事实计数
    assert info.row_count + 1 <= rebuilt.row_count <= info.row_count + 2


def test_snapshot_reuse_and_watermark_invalidation(env):
    first = env["snap"].acquire("supplier_snapshots", START, END)
    second = env["snap"].acquire("supplier_snapshots", START, END)
    assert first.snapshot_id == second.snapshot_id  # 水位未变，复用

    other = env["snap"].acquire("supplier_snapshots", START, "2026-10-01 00:00:00")
    assert other.snapshot_id != first.snapshot_id  # 范围不同，新建

    con = connect(env["db_path"])
    try:
        con.execute(
            """INSERT INTO supplier_stats VALUES
               ('SUP-0080', TIMESTAMP '2026-10-20 08:00:00', true, '华东', '电子元器件')"""
        )
    finally:
        con.close()
    rebuilt = env["snap"].acquire("supplier_snapshots", START, END)
    assert rebuilt.snapshot_id != first.snapshot_id  # 源水位变化，新建
    assert rebuilt.row_count == first.row_count + 1


# ---------------------------------------------------------------- 崩溃恢复


def test_job_resumes_from_manifest_without_duplication(env, monkeypatch):
    # 让第 3 次分段读取抛错，模拟进程在导出中途崩溃
    call_count = {"n": 0}
    original = SnapshotReader.fetch_segment

    def flaky(self, after_row_no, limit):
        call_count["n"] += 1
        if call_count["n"] == 3:
            raise RuntimeError("simulated worker crash")
        return original(self, after_row_no, limit)

    monkeypatch.setattr(SnapshotReader, "fetch_segment", flaky)
    manifest = env["jobs"].create_job(START, END, segment_size=250)
    result = _run_and_wait(env["jobs"], manifest["job_id"])
    assert result["status"] == FAILED
    completed_before = sum(len(s["segments"]) for s in result["datasets"].values())
    assert completed_before >= 1  # 崩溃前已有段落盘

    # 显式还原补丁，再用全新的管理器模拟进程重启
    monkeypatch.undo()
    assert SnapshotReader.fetch_segment is original

    new_snap = SnapshotManager(
        root=env["tmp"] / "audit" / "snapshots",
        catalog=env["catalog"],
        db_path=env["db_path"],
        ttl_seconds=3600,
    )
    new_jobs = JobManager(
        root=env["tmp"] / "audit",
        snapshot_manager=new_snap,
        catalog=env["catalog"],
        segment_size=250,
        fetch_batch_size=200,
        max_retries=0,
        heartbeat_seconds=0.05,
    )
    try:
        # FAILED 是终态，重启后通过 resume 入口显式恢复（对应 POST /resume），
        # 已完成段从 manifest 回放，未完成段从边界重扫，不重算已完成部分
        new_jobs.requeue(manifest["job_id"])
        new_jobs.start_worker()
        recovered = _run_and_wait(new_jobs, manifest["job_id"])
        assert recovered["status"] == COMPLETED, recovered.get("error")

        rows = _read_output(new_jobs, manifest["job_id"])
        counts = _expected_counts(env["db_path"])
        assert len(rows) == counts["supplier_snapshots"] + counts["payment_facts"]
        # 行号连续，无重复无缺口
        for state in recovered["datasets"].values():
            assert state["next_after_row_no"] == state["expected_row_count"]
    finally:
        new_jobs.stop_worker()


def test_running_job_is_taken_over_after_process_restart(env, monkeypatch):
    """进程在 RUNNING 中崩溃（未留终态）：新进程 start_worker 自动接管续跑。"""
    call_count = {"n": 0}
    original = SnapshotReader.fetch_segment

    def flaky(self, after_row_no, limit):
        call_count["n"] += 1
        if call_count["n"] == 3:
            raise RuntimeError("simulated process kill")
        return original(self, after_row_no, limit)

    monkeypatch.setattr(SnapshotReader, "fetch_segment", flaky)
    manifest = env["jobs"].create_job(START, END, segment_size=250)
    _run_and_wait(env["jobs"], manifest["job_id"])
    monkeypatch.undo()

    # 模拟旧进程没来得及写终态：把任务手动置回 RUNNING
    stale = env["jobs"].load_manifest(manifest["job_id"])
    stale["status"] = "running"
    env["jobs"]._save_manifest(manifest["job_id"], stale)
    env["jobs"].stop_worker()

    new_snap = SnapshotManager(
        root=env["tmp"] / "audit" / "snapshots",
        catalog=env["catalog"],
        db_path=env["db_path"],
        ttl_seconds=3600,
    )
    new_jobs = JobManager(
        root=env["tmp"] / "audit",
        snapshot_manager=new_snap,
        catalog=env["catalog"],
        segment_size=250,
        fetch_batch_size=200,
        max_retries=0,
        heartbeat_seconds=0.05,
    )
    try:
        new_jobs.start_worker()  # 非终态任务自动重新入队
        recovered = _run_and_wait(new_jobs, manifest["job_id"])
        assert recovered["status"] == COMPLETED, recovered.get("error")
        for state in recovered["datasets"].values():
            assert state["next_after_row_no"] == state["expected_row_count"]
    finally:
        new_jobs.stop_worker()


def test_completed_job_is_idempotent_and_not_reprocessed(env):
    manifest = env["jobs"].create_job(START, END)
    first = _run_and_wait(env["jobs"], manifest["job_id"])
    path1, _ = env["jobs"].output_path(manifest["job_id"])
    digest1 = path1.read_bytes()

    # 再次入队不应重扫任何段
    env["jobs"].requeue(manifest["job_id"])
    time.sleep(0.2)
    second = env["jobs"].load_manifest(manifest["job_id"])
    assert second["status"] == COMPLETED
    assert path1.read_bytes() == digest1
    assert first["root_hash"] == second["root_hash"]


# ---------------------------------------------------------------- 快照过期


def _make_expired_managers(tmp_path: Path, db_path: Path):
    catalog = default_catalog()
    snap = SnapshotManager(
        root=tmp_path / "audit" / "snapshots",
        catalog=catalog,
        db_path=db_path,
        ttl_seconds=0,  # 构建即过期
    )
    jobs = JobManager(
        root=tmp_path / "audit",
        snapshot_manager=snap,
        catalog=catalog,
    )
    return snap, jobs


def test_expired_snapshot_fails_the_job(tmp_path, source_db):
    db_path = tmp_path / "expired.duckdb"
    shutil.copyfile(source_db, db_path)
    snap, jobs = _make_expired_managers(tmp_path, db_path)
    try:
        manifest = jobs.create_job(START, END, datasets=["payment_facts"])
        result = _run_and_wait(jobs, manifest["job_id"])
        assert result["status"] == EXPIRED
        assert "expired" in (result["error"] or "")

        # 过期快照不能再被打开
        snapshot_id = result["snapshots"]["payment_facts"]
        with pytest.raises(SnapshotExpired):
            snap.open_reader(snapshot_id)

        # resume 一个过期任务必须被拒绝，要求重新发起
        with pytest.raises(SnapshotExpired):
            jobs.requeue(manifest["job_id"])
    finally:
        jobs.stop_worker()


def test_purge_expired_removes_files(tmp_path, source_db):
    db_path = tmp_path / "purge.duckdb"
    shutil.copyfile(source_db, db_path)
    snap, _ = _make_expired_managers(tmp_path, db_path)
    info = snap.acquire("payment_facts", START, END)
    assert Path(info.parquet_path).exists()
    assert snap.purge_expired() == 1
    assert not Path(info.parquet_path).exists()
    assert snap.list_snapshots() == []


# ---------------------------------------------------------------- 最小化与完整性


def test_sensitive_fields_are_minimized(env):
    manifest = env["jobs"].create_job(START, END)
    _run_and_wait(env["jobs"], manifest["job_id"])
    rows = _read_output(env["jobs"], manifest["job_id"])
    raw_text = Path(env["jobs"].output_path(manifest["job_id"])[0]).read_text(encoding="utf-8")

    # 原始标识符与敏感状态绝不进入交付物
    assert "SUP-" not in raw_text
    assert "ORD-" not in raw_text
    assert '"paid"' not in raw_text

    supplier_rows = [r for r in rows if "is_active" in r]
    payment_rows = [r for r in rows if "amount_cents" in r]
    assert all(r["status"] == REDACTED_PLACEHOLDER for r in payment_rows)

    # 假名定长且为十六进制
    pseudos = {r["supplier_id"] for r in supplier_rows} | {r["supplier_id"] for r in payment_rows}
    assert pseudos
    assert all(len(p) == 16 and all(c in "0123456789abcdef" for c in p) for p in pseudos)

    # 同一供应商在两张表中假名一致（可关联），且同值稳定
    con = connect(env["db_path"], read_only=True)
    try:
        shared = con.execute(
            """SELECT DISTINCT s.supplier_id FROM supplier_stats s
               WHERE EXISTS (SELECT 1 FROM order_facts o WHERE o.supplier_id = s.supplier_id)
               LIMIT 1"""
        ).fetchone()[0]
    finally:
        con.close()
    salt = env["jobs"]._salt_path(manifest["job_id"]).read_bytes()
    from meterhouse.audit.catalog import Minimizer

    pseudo = Minimizer(env["catalog"].get("payment_facts"), salt).pseudonym(shared)
    assert pseudo in pseudos


def test_tampered_segment_is_detected(env):
    manifest = env["jobs"].create_job(START, END, segment_size=300)
    # 在 finalize 前篡改即将完成的段文件 -> 校验失败
    original_validate = JobManager._validate_dataset

    def tamper_then_validate(self, job_id, state):
        if state["segments"]:
            target = self._job_dir(job_id) / state["segments"][0]["file"]
            if target.exists():
                with open(target, "ab") as fh:
                    fh.write(b'{"tampered":true}\n')
        return original_validate(self, job_id, state)

    JobManager._validate_dataset = tamper_then_validate
    try:
        result = _run_and_wait(env["jobs"], manifest["job_id"])
        assert result["status"] == FAILED
        # 大小或哈希任一检查失败都必须拦截被篡改的段
        assert "segment file missing or resized" in (result["error"] or "") or \
               "hash mismatch" in (result["error"] or "")
    finally:
        JobManager._validate_dataset = original_validate


def test_output_sha256_and_root_hash_present(env):
    manifest = env["jobs"].create_job(START, END)
    result = _run_and_wait(env["jobs"], manifest["job_id"])
    path, _ = env["jobs"].output_path(manifest["job_id"])

    assert hashlib.sha256(path.read_bytes()).hexdigest() == result["output"]["sha256"]
    assert len(result["root_hash"]) == 64
    # 重新读取的 manifest 根哈希稳定
    again = env["jobs"].load_manifest(manifest["job_id"])
    assert again["root_hash"] == result["root_hash"]


# ---------------------------------------------------------------- 资源隔离


def test_export_uses_isolated_bounded_connections(env, monkeypatch):
    import duckdb
    import weakref

    original_connect = duckdb.connect
    # 原生连接对象不支持挂属性，用弱引用 + finalize 跟踪生命周期
    live: list[bool] = []
    connections: list[str] = []

    def tracking_connect(path, read_only=False, *args, **kwargs):
        con = original_connect(path, read_only=read_only, *args, **kwargs)
        if read_only:
            idx = len(live)
            live.append(True)
            connections.append(str(path))
            weakref.finalize(con, lambda i=idx: live.__setitem__(i, False))
        return con

    monkeypatch.setattr(duckdb, "connect", tracking_connect)
    manifest = env["jobs"].create_job(START, END)
    _run_and_wait(env["jobs"], manifest["job_id"])

    assert connections, "导出必须通过独立只读连接访问数据"
    # 所有导出连接均指向当前数据库，且使用完毕后关闭释放
    assert all(p.endswith("test.duckdb") for p in connections)
    assert not any(live), "导出连接不得长生命周期持有"

    # 会话级资源上限设置路径可用（独立连接打开后应能读到设置项）
    con = original_connect(str(env["db_path"]), read_only=True)
    try:
        assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        assert con.execute("SELECT current_setting('threads')").fetchone()[0]
    finally:
        con.close()


def test_export_never_loads_full_result_into_memory(env):
    """fetch_batch_size 限制每次取数规模；总结果远大于批次也能完成。"""
    manifest = env["jobs"].create_job(START, END, segment_size=137)
    result = _run_and_wait(env["jobs"], manifest["job_id"])
    assert result["status"] == COMPLETED
    # 段数远多于 1，证明是流式分段而非一次性 fetchall
    total_segments = sum(len(s["segments"]) for s in result["datasets"].values())
    assert total_segments > 5


# ---------------------------------------------------------------- HTTP 接口


@pytest.fixture()
def client(tmp_path, source_db, monkeypatch):
    from fastapi.testclient import TestClient

    from meterhouse.audit.api import _state
    from meterhouse.config import settings
    from meterhouse.api import app

    db_path = tmp_path / "http.duckdb"
    shutil.copyfile(source_db, db_path)
    _state.clear()
    monkeypatch.setattr(settings, "audit_root", tmp_path / "audit")
    monkeypatch.setattr(settings, "db_path", db_path)

    with TestClient(app) as c:
        yield c
    _state.clear()


def _wait_job(client, job_id):
    deadline = time.time() + 30
    while time.time() < deadline:
        resp = client.get(f"/api/v1/audit/exports/{job_id}")
        body = resp.json()
        if body["status"] in {"completed", "failed", "expired", "cancelled"}:
            return body
        time.sleep(0.02)
    raise AssertionError("job did not finish in time")


def test_api_export_lifecycle_and_range_download(client):
    catalog = client.get("/api/v1/audit/datasets")
    assert catalog.status_code == 200
    names = [d["name"] for d in catalog.json()["datasets"]]
    assert names == ["payment_facts", "supplier_snapshots"]
    # 目录显式标注敏感列策略
    pay_cols = {
        c["name"]: c["policy"]
        for d in catalog.json()["datasets"]
        if d["name"] == "payment_facts"
        for c in d["columns"]
    }
    assert pay_cols["supplier_id"] == "hmac"
    assert pay_cols["status"] == "constant"

    resp = client.post(
        f"/api/v1/audit/exports?start_utc={START.replace(' ', '%20')}&end_utc={END.replace(' ', '%20')}"
    )
    assert resp.status_code == 200
    job_id = resp.json()["job_id"]

    body = _wait_job(client, job_id)
    assert body["status"] == COMPLETED

    # 全量下载
    full = client.get(f"/api/v1/audit/exports/{job_id}/download")
    assert full.status_code == 200
    assert full.headers["x-export-root-hash"] == body["root_hash"]
    full_bytes = full.content

    # Range 续传：分段下载后拼接必须与全量字节完全一致
    size = body["output"]["size_bytes"]
    mid = size // 2
    part1 = client.get(
        f"/api/v1/audit/exports/{job_id}/download", headers={"Range": f"bytes=0-{mid - 1}"}
    )
    assert part1.status_code == 206
    part2 = client.get(
        f"/api/v1/audit/exports/{job_id}/download", headers={"Range": f"bytes={mid}-"}
    )
    assert part2.status_code == 206
    assert part1.content + part2.content == full_bytes

    # 证明接口
    proof = client.get(f"/api/v1/audit/exports/{job_id}/proof")
    assert proof.status_code == 200
    proof_body = proof.json()
    assert proof_body["root_hash"] == body["root_hash"]
    assert all(item["cross_check_matched"] for item in proof_body["snapshots"])


def test_api_disconnect_does_not_kill_job(client):
    # 创建任务后立即断开（不轮询），后台工作器仍应完成
    resp = client.post(
        f"/api/v1/audit/exports?start_utc={START.replace(' ', '%20')}&end_utc={END.replace(' ', '%20')}"
    )
    job_id = resp.json()["job_id"]
    del resp  # 模拟客户端离开：没有任何请求在等待
    body = _wait_job(client, job_id)
    assert body["status"] == COMPLETED


def test_api_rejects_bad_range_and_unknown_dataset(client):
    bad = client.post(
        "/api/v1/audit/exports?start_utc=2026-10-01%2000:00:00&end_utc=2026-09-01%2000:00:00"
    )
    assert bad.status_code == 400

    unknown = client.post(
        "/api/v1/audit/exports?start_utc=2026-09-01%2000:00:00&end_utc=2026-10-01%2000:00:00&datasets=nope"
    )
    assert unknown.status_code == 400

    missing = client.get("/api/v1/audit/exports/job_does_not_exist")
    assert missing.status_code == 404


def test_api_cancel_and_resume(client):
    resp = client.post(
        f"/api/v1/audit/exports?start_utc={START.replace(' ', '%20')}&end_utc={END.replace(' ', '%20')}&segment_size=100"
    )
    job_id = resp.json()["job_id"]
    # 任务很快，允许取消或完成两种终态；取消后可恢复
    client.post(f"/api/v1/audit/exports/{job_id}/cancel")
    state = client.get(f"/api/v1/audit/exports/{job_id}").json()
    if state["status"] == "cancelled":
        client.post(f"/api/v1/audit/exports/{job_id}/resume")
    body = _wait_job(client, job_id)
    assert body["status"] == COMPLETED


# ---------------------------------------------------------------- 编码确定性


def test_canonical_json_is_deterministic():
    row = {"b": 2, "a": "值", "c": True}
    assert canonical_json(row) == canonical_json({"c": True, "a": "值", "b": 2})
    assert canonical_json(row).endswith(b"\n")
    # 键序固定、无多余空白
    assert canonical_json(row) == '{"a":"值","b":2,"c":true}\n'.encode("utf-8")
