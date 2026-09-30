"""整批放行并发与边界正确性验证（直连数据库，不走 HTTP）。

覆盖：
 1. 基本整批放行：多仪器多条目原子发布，按输入顺序返回标识/所据版本/快照
 2. 幂等：相同内容重试原样返回首次结果且不新增报告；同号不同内容 409
 3. 任一条目阻断（禁用优先/无校准/区间外/未通过）-> 422 带条目位置，整批回滚
 4. 失败不占批次号：阻断/编号冲突后同批次号可再用
 5. 批内编号重复 -> 422 BATCH_DUPLICATE_REFERENCE（取锁前预检）
 6. 编号与已发布报告冲突 -> 409 REPORT_REFERENCE_EXISTS 带条目位置，整批回滚
 7. 并发同批次号重试（相同内容）：全部得到首次结果，仅一批报告落库
 8. 并发同批次号不同内容：恰一个成功，其余 BATCH_CONTENT_MISMATCH
 9. 并发“批次 vs 禁用登记/撤销/校准更正/单份发布”：无半批、无过期判定
10. 并发两批次（不同仪器）相同编号：恰一个成功，另一个 REPORT_REFERENCE_EXISTS
11. 确定性互斥：禁用持锁未提交时，批次在取锁阶段阻塞，提交后必被阻断
12. 单份接口行为不变；空条目/空批次号被模型拒绝
"""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pydantic
from fastapi import HTTPException

import app.db as db
from app.models import (
    BatchReportEntry,
    BatchReportSubmit,
    CalibrationCorrect,
    CalibrationCreate,
    DisablementCreate,
    ReportSubmit,
)
from app.repository import (
    batch_request_hash,
    correct_calibration,
    create_calibration,
    create_disablement,
    instrument_of_calibration,
    prelock_batch,
    prelock_event_then_instrument,
    prelock_instrument,
    publish_batch,
    publish_report,
    validate_batch_payload,
    _lock_key,
)
from app.transactions import run_tx

failures: list[str] = []

db.open_pool()


def check(name, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


def reset():
    with db.pool.connection() as c:
        c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    db.init_db()


def dt(y, m, d, hh=0):
    return datetime(y, m, d, hh, tzinfo=timezone.utc)


def entry(instrument, y, m, d, reference=None, hh=0):
    return BatchReportEntry(instrument=instrument, tested_at=dt(y, m, d, hh),
                            reference=reference)


def tx_publish_batch(payload):
    """与路由一致：先批内预检，再带批次锁+仪器锁进事务。"""
    validate_batch_payload(payload)
    return run_tx(lambda c: publish_batch(c, payload),
                  prelock=prelock_batch(payload.batch_no,
                                        [e.instrument for e in payload.entries]))


def tx_publish(payload):
    return run_tx(lambda c: publish_report(c, payload),
                  prelock=prelock_instrument(payload.instrument))


def tx_create_calibration(payload):
    return run_tx(lambda c: create_calibration(c, payload),
                  prelock=prelock_instrument(payload.instrument))


def tx_create_disablement(payload):
    return run_tx(lambda c: create_disablement(c, payload),
                  prelock=prelock_event_then_instrument(payload.event_no,
                                                        payload.instrument))


def tx_correct(payload):
    with db.pool.connection() as probe:
        instrument = instrument_of_calibration(probe, payload.corrects_id)
    assert instrument is not None
    return run_tx(lambda c: correct_calibration(c, payload),
                  prelock=prelock_instrument(instrument))


def code_of(exc):
    return exc.detail["code"]


def report_count(instrument=None):
    with db.pool.connection() as c:
        if instrument:
            return c.execute("SELECT count(*) n FROM reports WHERE instrument=%s",
                             (instrument,)).fetchone()["n"]
        return c.execute("SELECT count(*) n FROM reports").fetchone()["n"]


def batch_count():
    with db.pool.connection() as c:
        return c.execute("SELECT count(*) n FROM report_batches").fetchone()["n"]


# ---------- 1. 基本整批放行：多仪器原子发布，按输入顺序返回 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B1-A", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
tx_create_calibration(CalibrationCreate(
    instrument="B1-B", valid_from=dt(2026, 1, 1), valid_to=None, passed=True))

payload = BatchReportSubmit(batch_no="LOT-1", entries=[
    entry("B1-A", 2026, 3, 1, "R-A-1"),
    entry("B1-B", 2026, 3, 2, "R-B-1"),
    entry("B1-A", 2026, 3, 3),  # 无编号条目允许
])
resp = tx_publish_batch(payload)
check("整批放行成功", resp["released"] is True and resp["batch_no"] == "LOT-1")
check("按输入顺序返回 3 条，位置/编号对应",
      [r["entry_index"] for r in resp["reports"]] == [0, 1, 2]
      and resp["reports"][0]["reference"] == "R-A-1"
      and resp["reports"][1]["reference"] == "R-B-1"
      and resp["reports"][2]["reference"] is None, str(resp["reports"]))
check("每条含所据校准版本与发布快照",
      all(r["basis_calibration"]["passed"] is True
          and r["snapshot"]["released"] is True
          and r["snapshot"]["basis_calibration"]["id"] == r["basis_calibration"]["id"]
          for r in resp["reports"]))
with db.pool.connection() as c:
    rows = c.execute(
        "SELECT reference, batch_no FROM reports ORDER BY id").fetchall()
check("报告落库且关联批次号", len(rows) == 3
      and all(r["batch_no"] == "LOT-1" for r in rows), str(rows))
check("批次行落库且存响应", batch_count() == 1)


# ---------- 2. 幂等：相同内容重试 -> 首次结果；不同内容 -> 409 ----------
replay = tx_publish_batch(payload)
check("相同内容重试：原样返回首次结果（含相同 report_id）", replay == resp)
check("重试未新增报告/批次", report_count() == 3 and batch_count() == 1)

changed = BatchReportSubmit(batch_no="LOT-1", entries=[
    entry("B1-A", 2026, 3, 1, "R-A-1"),
    entry("B1-B", 2026, 3, 2, "R-B-1"),
    entry("B1-A", 2026, 3, 4),  # 与首次不同
])
try:
    tx_publish_batch(changed)
    check("同号不同内容拒绝", False, "未抛错")
except HTTPException as e:
    check("同号不同内容 -> 409 BATCH_CONTENT_MISMATCH",
          e.status_code == 409 and code_of(e) == "BATCH_CONTENT_MISMATCH", str(e.detail))
check("拒绝后仍未新增报告", report_count() == 3 and batch_count() == 1)


# ---------- 3. 任一条目阻断 -> 422 带位置，整批回滚，不占批次号 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B3-A", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
tx_create_disablement(DisablementCreate(
    event_no="B3-EVT", instrument="B3-A",
    disabled_from=dt(2026, 6, 1), disabled_to=dt(2026, 7, 1), reason="泵故障"))

blocked_payload = BatchReportSubmit(batch_no="LOT-3", entries=[
    entry("B3-A", 2026, 3, 1, "ok-1"),          # 可放行
    entry("B3-A", 2026, 6, 15, "hit-disable"),  # 命中禁用（禁用优先）
    entry("B3-A", 2026, 3, 2, "ok-2"),          # 可放行
])
try:
    tx_publish_batch(blocked_payload)
    check("禁用阻断整批", False, "未抛错")
except HTTPException as e:
    check("命中禁用条目 -> 422 INSTRUMENT_DISABLED 且带条目位置 1",
          e.status_code == 422 and code_of(e) == "INSTRUMENT_DISABLED"
          and e.detail["entry_index"] == 1
          and e.detail["basis_disablement"]["event_no"] == "B3-EVT", str(e.detail))
check("阻断后整批无报告、无批次行（不占批次号）",
      report_count() == 0 and batch_count() == 0)

# 其余阻断原因沿用单份语义并带位置
def expect_block(entries, code, index, name):
    p = BatchReportSubmit(batch_no=f"LOT-{name}", entries=entries)
    try:
        tx_publish_batch(p)
        check(name, False, "未抛错")
    except HTTPException as e:
        check(name, e.status_code == 422 and code_of(e) == code
              and e.detail["entry_index"] == index, str(e.detail))

expect_block([entry("B3-A", 2026, 3, 1), entry("B3-UNKNOWN", 2026, 3, 1)],
             "NO_CALIBRATION", 1, "无校准条目阻断")
expect_block([entry("B3-A", 2028, 1, 1)], "OUTSIDE_VALID_PERIOD", 0, "区间外条目阻断")
tx_create_calibration(CalibrationCreate(
    instrument="B3-C", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=False))
expect_block([entry("B3-A", 2026, 3, 1), entry("B3-C", 2026, 3, 1)],
             "CALIBRATION_NOT_PASSED", 1, "未通过条目阻断")
check("多次失败后仍无批次行", batch_count() == 0)

# 失败不占批次号：同号修正内容后成功
fixed = BatchReportSubmit(batch_no="LOT-3", entries=[
    entry("B3-A", 2026, 3, 1, "ok-1"),
    entry("B3-A", 2026, 3, 2, "ok-2"),
])
resp3 = tx_publish_batch(fixed)
check("阻断失败后同批次号修正重提 -> 成功", resp3["released"] is True
      and report_count("B3-A") == 2)


# ---------- 5. 批内编号重复 -> 422（取锁前预检） ----------
dup = BatchReportSubmit(batch_no="LOT-5", entries=[
    entry("B3-A", 2026, 3, 1, "dup-ref"),
    entry("B3-A", 2026, 3, 2),
    entry("B3-A", 2026, 3, 3, "dup-ref"),
])
try:
    tx_publish_batch(dup)
    check("批内重复编号拒绝", False, "未抛错")
except HTTPException as e:
    check("批内重复编号 -> 422 BATCH_DUPLICATE_REFERENCE 带两个位置",
          e.status_code == 422 and code_of(e) == "BATCH_DUPLICATE_REFERENCE"
          and e.detail["entry_indices"] == [0, 2], str(e.detail))
check("批内重复不产生报告/批次", report_count("B3-A") == 2 and batch_count() == 1)


# ---------- 6. 编号与已发布报告冲突 -> 409 带位置，整批回滚 ----------
clash = BatchReportSubmit(batch_no="LOT-6", entries=[
    entry("B3-A", 2026, 3, 5, "fresh-1"),
    entry("B3-A", 2026, 3, 6, "ok-1"),  # 与第 3 节已发布编号冲突
])
try:
    tx_publish_batch(clash)
    check("编号冲突拒绝", False, "未抛错")
except HTTPException as e:
    check("与已发布编号冲突 -> 409 REPORT_REFERENCE_EXISTS 带位置 1",
          e.status_code == 409 and code_of(e) == "REPORT_REFERENCE_EXISTS"
          and e.detail["entry_index"] == 1, str(e.detail))
check("冲突整批回滚（fresh-1 未落库），批次号未占用",
      report_count("B3-A") == 2 and batch_count() == 1)


# ---------- 7. 并发同批次号相同内容重试：全部得到首次结果 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B7", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
same = BatchReportSubmit(batch_no="LOT-7", entries=[
    entry("B7", 2026, 3, 1, "s-1"), entry("B7", 2026, 3, 2, "s-2")])
N = 12
barrier = threading.Barrier(N)


def race_same(i):
    barrier.wait()
    try:
        return ("ok", tx_publish_batch(same))
    except HTTPException as e:
        return (code_of(e), None)


with ThreadPoolExecutor(N) as ex:
    results = list(ex.map(race_same, range(N)))
oks = [r for r in results if r[0] == "ok"]
check("并发12次同号同内容：全部成功返回", len(oks) == N, str([r[0] for r in results]))
first = oks[0][1]
check("全部返回首次结果（相同 report_id 集合）",
      all(r[1] == first for r in oks))
check("仅一批报告落库（2 条）", report_count("B7") == 2 and batch_count() == 1)


# ---------- 8. 并发同批次号不同内容：恰一个成功 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B8", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
barrier = threading.Barrier(N)


def race_diff(i):
    barrier.wait()
    p = BatchReportSubmit(batch_no="LOT-8", entries=[
        entry("B8", 2026, 3, 1, f"diff-{i}")])
    try:
        return ("ok", tx_publish_batch(p))
    except HTTPException as e:
        return (code_of(e), None)


with ThreadPoolExecutor(N) as ex:
    results = list(ex.map(race_diff, range(N)))
ok_n = sum(1 for r in results if r[0] == "ok")
mm_n = sum(1 for r in results if r[0] == "BATCH_CONTENT_MISMATCH")
check("并发12次同号不同内容：恰1个成功，其余内容冲突",
      ok_n == 1 and mm_n == N - 1, str([r[0] for r in results]))
check("仅胜者的 1 条报告落库", report_count("B8") == 1 and batch_count() == 1)


# ---------- 9a. 并发“批次 vs 禁用登记”：可串行，无半批 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B9", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
M = 8
barrier = threading.Barrier(M + 1)


def race_batch9(i):
    barrier.wait()
    p = BatchReportSubmit(batch_no=f"LOT-9-{i}", entries=[
        entry("B9", 2026, 6, 10, f"b9-{i}-a"),
        entry("B9", 2026, 6, 15, f"b9-{i}-b"),
    ])
    try:
        return ("released", tx_publish_batch(p))
    except HTTPException as e:
        return (code_of(e), None)


def race_disable9():
    barrier.wait()
    try:
        tx_create_disablement(DisablementCreate(
            event_no="B9-EVT", instrument="B9",
            disabled_from=dt(2026, 6, 12), disabled_to=dt(2026, 7, 1),
            reason="追溯故障"))
        return "registered"
    except HTTPException as e:
        return code_of(e)


with ThreadPoolExecutor(M + 1) as ex:
    bf = [ex.submit(race_batch9, i) for i in range(M)]
    df = ex.submit(race_disable9)
    bres = [f.result() for f in bf]
    dres = df.result()
check("禁用登记最终生效", dres == "registered", str(dres))
codes = {r[0] for r in bres}
check("竞态批次结果仅为 放行(登记前)/禁用阻断(登记后)",
      codes <= {"released", "INSTRUMENT_DISABLED"}, str(codes))
# 无半批：每个批次要么 2 条要么 0 条
with db.pool.connection() as c:
    per_batch = c.execute(
        "SELECT batch_no, count(*) n FROM reports GROUP BY batch_no").fetchall()
check("无半批（每批要么 2 条要么不存在）",
      all(r["n"] == 2 for r in per_batch), str(per_batch))
# 阻断的批次不占号：被阻断批次号重提（检测点避开禁用区）可成功
blocked_lots = [f"LOT-9-{i}" for i, r in enumerate(bres) if r[0] == "INSTRUMENT_DISABLED"]
if blocked_lots:
    retry = BatchReportSubmit(batch_no=blocked_lots[0], entries=[
        entry("B9", 2026, 3, 1, "retry-1")])
    rr = tx_publish_batch(retry)
    check("被阻断批次号可再用", rr["released"] is True)
# 尘埃落定后：覆盖禁用区的批次必阻断
settle = BatchReportSubmit(batch_no="LOT-9-settle", entries=[
    entry("B9", 2026, 6, 15, "settle-1")])
try:
    tx_publish_batch(settle)
    check("竞态结束后覆盖禁用区的批次必阻断", False, "未抛错")
except HTTPException as e:
    check("竞态结束后覆盖禁用区的批次必阻断",
          code_of(e) == "INSTRUMENT_DISABLED", str(e.detail))


# ---------- 9b. 并发“批次 vs 校准更正”：不依据过期校准放行 ----------
reset()
v1 = tx_create_calibration(CalibrationCreate(
    instrument="B9B", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
barrier = threading.Barrier(M + 1)


def race_batch9b(i):
    barrier.wait()
    p = BatchReportSubmit(batch_no=f"LOT-9B-{i}", entries=[
        entry("B9B", 2026, 6, 10, f"b9b-{i}")])
    try:
        r = tx_publish_batch(p)
        return ("released", r["reports"][0]["basis_calibration"]["version"])
    except HTTPException as e:
        return (code_of(e), None)


def race_correct9b():
    barrier.wait()
    try:
        tx_correct(CalibrationCorrect(
            corrects_id=v1["id"], version=2,
            valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=False))
        return "corrected"
    except HTTPException as e:
        return code_of(e)


with ThreadPoolExecutor(M + 1) as ex:
    bf = [ex.submit(race_batch9b, i) for i in range(M)]
    cf = ex.submit(race_correct9b)
    bres = [f.result() for f in bf]
    cres = cf.result()
check("更正最终生效", cres == "corrected", str(cres))
check("批次结果仅为 放行(更正前)/未通过阻断(更正后)",
      {r[0] for r in bres} <= {"released", "CALIBRATION_NOT_PASSED"}, str(bres))
with db.pool.connection() as c:
    bad = c.execute("""
        SELECT r.id FROM reports r
        JOIN calibration_versions cv ON cv.id = r.basis_calibration_id
        JOIN calibration_versions v2 ON v2.chain_id = cv.chain_id AND v2.version = 2
        WHERE cv.version = 1 AND r.created_at > v2.created_at
    """).fetchall()
check("无“依据过期 v1”的迟到放行", not bad, str(bad))


# ---------- 9c. 并发“批次 vs 同仪器单份发布”：互斥串行 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B9C", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
barrier = threading.Barrier(M * 2)


def race_batch9c(i):
    barrier.wait()
    p = BatchReportSubmit(batch_no=f"LOT-9C-{i}", entries=[
        entry("B9C", 2026, 6, 10, f"b9c-{i}")])
    try:
        tx_publish_batch(p)
        return "batch-ok"
    except HTTPException as e:
        return code_of(e)


def race_single9c(i):
    barrier.wait()
    try:
        tx_publish(ReportSubmit(instrument="B9C", tested_at=dt(2026, 6, 11),
                                reference=f"single-{i}"))
        return "single-ok"
    except HTTPException as e:
        return code_of(e)


with ThreadPoolExecutor(M * 2) as ex:
    futs = [ex.submit(race_batch9c, i) for i in range(M)]
    futs += [ex.submit(race_single9c, i) for i in range(M)]
    res = [f.result() for f in futs]
check("批次与单份并发全部成功（仪器锁串行，互不干扰）",
      all(r in ("batch-ok", "single-ok") for r in res), str(res))
check("全部落库（批 8 + 单 8）", report_count("B9C") == M * 2)


# ---------- 10. 并发两批次（不同仪器）相同编号：恰一个成功 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B10-A", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
tx_create_calibration(CalibrationCreate(
    instrument="B10-B", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
K = 6
barrier = threading.Barrier(K * 2)


def race_clash(i, instrument):
    barrier.wait()
    p = BatchReportSubmit(batch_no=f"LOT-10-{instrument}-{i}", entries=[
        entry(instrument, 2026, 3, 1, "shared-ref"),
        entry(instrument, 2026, 3, 2, f"own-{instrument}-{i}"),
    ])
    try:
        tx_publish_batch(p)
        return "ok"
    except HTTPException as e:
        return code_of(e)


with ThreadPoolExecutor(K * 2) as ex:
    futs = [ex.submit(race_clash, i, "B10-A") for i in range(K)]
    futs += [ex.submit(race_clash, i, "B10-B") for i in range(K)]
    res = [f.result() for f in futs]
ok_n = res.count("ok")
check("跨仪器同编号并发：恰1个成功，其余编号冲突",
      ok_n == 1 and all(r in ("ok", "REPORT_REFERENCE_EXISTS") for r in res),
      str(res))
check("失败者整批回滚（仅胜者 2 条落库）", report_count() == 2,
      str(report_count()))
check("失败者批次号未占用（仅 1 个批次行）", batch_count() == 1)


# ---------- 11. 确定性互斥：禁用持锁未提交，批次在取锁阶段阻塞，提交后必阻断 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B11", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
c1 = db.pool.getconn()
holder = {"done": False}
t = None
try:
    c1.autocommit = True
    c1.execute("SELECT pg_advisory_lock(%s)", (_lock_key("instrument\x00B11"),))
    c1.autocommit = False
    c1.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
    c1.execute(
        "INSERT INTO instrument_disablements (event_no, instrument, disabled_from,"
        " disabled_to, reason) VALUES ('B11-EVT','B11',%s,%s,'待提交故障')",
        (dt(2026, 6, 1), dt(2026, 7, 1)))

    def concurrent_batch():
        p = BatchReportSubmit(batch_no="LOT-11", entries=[
            entry("B11", 2026, 6, 15, "held-1")])
        try:
            tx_publish_batch(p)
            holder["result"] = "released"
        except HTTPException as e:
            holder["result"] = code_of(e)
        finally:
            holder["done"] = True

    t = threading.Thread(target=concurrent_batch)
    t.start()
    t.join(timeout=2.0)
    check("禁用持锁未提交时，批次在取锁阶段被阻塞（未提前判定）",
          not holder["done"], f"done={holder.get('done')}")
    c1.execute("COMMIT")
    c1.autocommit = True
    c1.execute("SELECT pg_advisory_unlock_all()")
    t.join(timeout=10.0)
    t = None
    check("禁用提交并放锁后，批次被唤醒并阻断（不使用过期状态）",
          holder["done"] and holder["result"] == "INSTRUMENT_DISABLED",
          str(holder.get("result")))
finally:
    try:
        if not c1.autocommit:
            c1.execute("COMMIT")
    except Exception:
        pass
    if t is not None:
        try:
            c1.autocommit = True
            c1.execute("SELECT pg_advisory_unlock_all()")
        except Exception:
            pass
        t.join(timeout=10.0)
    try:
        c1.autocommit = True
        c1.execute("SELECT pg_advisory_unlock_all()")
    except Exception:
        pass
    db.pool.putconn(c1)
check("互斥场景整批未落库、批次号未占用", report_count("B11") == 0
      and batch_count() == 0)


# ---------- 12. 单份接口行为不变 + 模型校验 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="B12", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
single = tx_publish(ReportSubmit(instrument="B12", tested_at=dt(2026, 3, 1),
                                 reference="solo-1"))
check("单份发布响应结构不变（无批次字段）",
      set(single.keys()) == {"released", "reason_code", "basis_calibration",
                             "report_id", "reference", "snapshot"},
      str(sorted(single.keys())))
with db.pool.connection() as c:
    row = c.execute("SELECT batch_no FROM reports WHERE reference='solo-1'").fetchone()
check("单份发布报告 batch_no 为 NULL", row["batch_no"] is None)
try:
    tx_publish(ReportSubmit(instrument="B12", tested_at=dt(2026, 3, 1),
                            reference="solo-1"))
    check("单份编号冲突", False)
except HTTPException as e:
    check("单份编号冲突错误结构不变（无附加字段）",
          set(e.detail.keys()) == {"code", "message"}, str(e.detail))

try:
    BatchReportSubmit(batch_no="X", entries=[])
    check("空条目拒绝", False)
except pydantic.ValidationError:
    check("空条目被模型拒绝", True)
try:
    BatchReportSubmit(batch_no="", entries=[entry("B12", 2026, 3, 1)])
    check("空批次号拒绝", False)
except pydantic.ValidationError:
    check("空批次号被模型拒绝", True)

# 单条目批次可用；请求哈希对时区写法稳定（Z 与 +00:00 归一后相同）
one = BatchReportSubmit(batch_no="LOT-12", entries=[entry("B12", 2026, 4, 1, "one-1")])
r1 = tx_publish_batch(one)
check("单条目批次放行成功", r1["count"] == 1)
same_instant = BatchReportSubmit.model_validate({
    "batch_no": "LOT-12",
    "entries": [{"instrument": "B12", "tested_at": "2026-04-01T08:00:00+08:00",
                 "reference": "one-1"}],
})
check("同一时刻不同时区写法哈希一致（幂等重放）",
      batch_request_hash(same_instant) == batch_request_hash(one))
r2 = tx_publish_batch(same_instant)
check("时区写法不同的相同内容重试 -> 返回首次结果", r2 == r1)


print()
if failures:
    print(f"{len(failures)} 项失败:", failures)
    sys.exit(1)
print("全部整批放行并发/边界验证通过")
