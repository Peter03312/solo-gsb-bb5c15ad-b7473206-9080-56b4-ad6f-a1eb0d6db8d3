"""整批放行正确性验证（直连数据库，不走 HTTP）。

覆盖：
 1. 基本批量放行：多仪器、按输入顺序返回报告标识/所据校准版本/快照，行带批次号
 2. 任一项阻断（禁用优先/无校准/区间外/未通过）：422 带条目位置，整批不落库
 3. 失败不占批次号：阻断/冲突后同号修正内容可重发成功
 4. 批内编号重复 -> 422 BATCH_DUPLICATE_REFERENCE（带两处位置）
 5. 与已发布记录编号冲突 -> 409 REPORT_REFERENCE_EXISTS（带位置），整批不落库
 6. 幂等：同号同内容重试原样返回首次结果（报告不重复落库）；同号异内容拒绝
 7. 并发同批次重试：恰好一个首次、其余重放，报告仅一批
 8. 并发同号异内容：恰好一个成功，其余 BATCH_NO_CONTENT_MISMATCH
 9. 并发批量 vs 校准更正：可串行、无半批；重放不读过期/新状态（返回首次结果）
10. 已生效禁用区间内并发批量：全部阻断，零误放行（核心不变量）
11. 并发批量 vs 禁用撤销：结果可串行
12. 不同批次号并发（共享仪器）：均成功
13. 单份接口既有行为不变（与批量共存、编号冲突仍 409）
14. 请求形态：空条目/空批次号在模型层拒绝
"""
from __future__ import annotations

import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from pydantic import ValidationError

import app.db as db
from app.models import (
    CalibrationCorrect,
    CalibrationCreate,
    DisablementCreate,
    ReportBatchSubmit,
    ReportSubmit,
)
from app.repository import (
    _batch_request_hash,
    correct_calibration,
    create_calibration,
    create_disablement,
    get_batch,
    prelock_batch,
    prelock_event,
    prelock_event_then_instrument,
    prelock_instrument,
    publish_batch,
    publish_report,
    revoke_disablement,
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


def item(instrument, y, m, d, reference=None, hh=0):
    return ReportSubmit(instrument=instrument, tested_at=dt(y, m, d, hh),
                        reference=reference)


def tx_batch(payload: ReportBatchSubmit):
    return run_tx(
        lambda c: publish_batch(c, payload),
        prelock=prelock_batch(payload.batch_no,
                              [i.instrument for i in payload.items]),
    )


def tx_publish(payload):
    return run_tx(lambda c: publish_report(c, payload),
                  prelock=prelock_instrument(payload.instrument))


def tx_create_calibration(payload):
    return run_tx(lambda c: create_calibration(c, payload),
                  prelock=prelock_instrument(payload.instrument))


def tx_create_disablement(payload):
    return run_tx(lambda c: create_disablement(c, payload),
                  prelock=prelock_event_then_instrument(payload.event_no, payload.instrument))


def tx_revoke(event_no):
    return run_tx(lambda c: revoke_disablement(c, event_no),
                  prelock=prelock_event(event_no))


def tx_correct(payload, instrument):
    return run_tx(lambda c: correct_calibration(c, payload),
                  prelock=prelock_instrument(instrument))


def batch_call(payload):
    """返回 (status, body-or-detail, replayed)。"""
    try:
        body, replayed = tx_batch(payload)
        return (200 if replayed else 201), body, replayed
    except HTTPException as e:
        return e.status_code, e.detail, None


def reports_of(batch_no):
    with db.pool.connection() as c:
        return c.execute(
            "SELECT * FROM reports WHERE batch_no = %s ORDER BY id", (batch_no,),
        ).fetchall()


def batch_row(batch_no):
    with db.pool.connection() as c:
        return get_batch(c, batch_no)


# ---------- 1. 基本批量放行：顺序、依据、快照、行批次号 ----------
reset()
cal_a = tx_create_calibration(CalibrationCreate(
    instrument="BA", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
cal_b = tx_create_calibration(CalibrationCreate(
    instrument="BB", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))

payload = ReportBatchSubmit(batch_no="B-1", items=[
    item("BA", 2026, 3, 15, "RA-1", hh=8),
    item("BB", 2026, 4, 15, "RB-1", hh=9),
    item("BA", 2026, 5, 15),  # 无编号也允许
])
status, body, replayed = batch_call(payload)
check("批量放行 201 且非重放", status == 201 and replayed is False, str(status))
check("按输入顺序返回 3 项且位置连续",
      [i["position"] for i in body["items"]] == [1, 2, 3], str(body))
check("各项返回报告标识/所据校准版本/快照",
      all(i["report_id"] and i["basis_calibration"]["id"] and i["snapshot"]["released"]
          for i in body["items"]), str(body["items"]))
check("所据校准版本正确（BA/BB 各自 v1）",
      body["items"][0]["basis_calibration"]["id"] == cal_a["id"]
      and body["items"][1]["basis_calibration"]["id"] == cal_b["id"]
      and body["items"][2]["basis_calibration"]["id"] == cal_a["id"], str(body["items"]))
check("报告标识含编号与报告 ID",
      [i["reference"] for i in body["items"]] == ["RA-1", "RB-1", None], str(body["items"]))
rows = reports_of("B-1")
check("落库 3 行且带批次号、快照与响应一致",
      len(rows) == 3
      and [r["id"] for r in rows] == [i["report_id"] for i in body["items"]]
      and all(r["snapshot"]["released"] is True for r in rows), str(len(rows)))
check("批次记录已写入且响应与返回一致",
      batch_row("B-1")["response"] == body, "")
check("内容摘要稳定（同内容同哈希）",
      _batch_request_hash(payload) == batch_row("B-1")["request_hash"], "")


# ---------- 2. 任一项阻断：禁用优先 + 条目位置 + 整批不落库 + 失败不占批次号 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BC", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
tx_create_disablement(DisablementCreate(
    event_no="BC-E", instrument="BC",
    disabled_from=dt(2026, 4, 1), disabled_to=dt(2026, 5, 1), reason="泵故障"))

status, detail, _ = batch_call(ReportBatchSubmit(batch_no="B-2", items=[
    item("BC", 2026, 3, 15, "RC-1"),
    item("BC", 2026, 4, 15, "RC-2"),   # 命中禁用区间
    item("BC", 2026, 6, 15, "RC-3"),
]))
check("第 2 项命中禁用 -> 422 INSTRUMENT_DISABLED 且带位置与禁用依据",
      status == 422 and detail["code"] == "INSTRUMENT_DISABLED"
      and detail["item_index"] == 2
      and detail["basis_disablement"]["event_no"] == "BC-E", str(detail))
check("整批不落库（reports 无行）", reports_of("B-2") == [])
check("失败不占批次号（report_batches 无行）", batch_row("B-2") is None)

# 同批次号修正内容后重发成功（批次号被失败释放）
status, body, _ = batch_call(ReportBatchSubmit(batch_no="B-2", items=[
    item("BC", 2026, 3, 15, "RC-1"),
    item("BC", 2026, 6, 15, "RC-3"),
]))
check("失败批次号可复用：修正后同号重发成功", status == 201 and body["count"] == 2, str(status))

# 各阻断原因均带位置且整批不落库
status, detail, _ = batch_call(ReportBatchSubmit(batch_no="B-2x", items=[
    item("BC", 2026, 3, 15, "ok-1"),
    item("NOPE", 2026, 3, 15, "x-1"),   # 无校准仪器
]))
check("第 2 项无校准 -> 422 NO_CALIBRATION 带位置",
      status == 422 and detail["code"] == "NO_CALIBRATION" and detail["item_index"] == 2,
      str(detail))
check("无校准阻断整批不落库", reports_of("B-2x") == [] and batch_row("B-2x") is None)

status, detail, _ = batch_call(ReportBatchSubmit(batch_no="B-2y", items=[
    item("BC", 2027, 1, 1, "edge"),     # 恰为 valid_to（右开）
]))
check("恰为 valid_to -> 422 OUTSIDE_VALID_PERIOD",
      status == 422 and detail["code"] == "OUTSIDE_VALID_PERIOD"
      and detail["item_index"] == 1, str(detail))

tx_create_calibration(CalibrationCreate(
    instrument="BD", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=False))
status, detail, _ = batch_call(ReportBatchSubmit(batch_no="B-2z", items=[
    item("BC", 2026, 3, 15, "ok-2"),
    item("BD", 2026, 3, 15, "bad-1"),   # 校准未通过
]))
check("第 2 项校准未通过 -> 422 CALIBRATION_NOT_PASSED 带位置",
      status == 422 and detail["code"] == "CALIBRATION_NOT_PASSED"
      and detail["item_index"] == 2, str(detail))
check("未通过阻断整批不落库", reports_of("B-2z") == [])


# ---------- 3. 批内编号重复 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BE", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
status, detail, _ = batch_call(ReportBatchSubmit(batch_no="B-3", items=[
    item("BE", 2026, 3, 15, "DUP"),
    item("BE", 2026, 4, 15, "OTHER"),
    item("BE", 2026, 5, 15, "DUP"),
]))
check("批内编号重复 -> 422 BATCH_DUPLICATE_REFERENCE 带两处位置",
      status == 422 and detail["code"] == "BATCH_DUPLICATE_REFERENCE"
      and detail["item_index"] == 3 and detail["first_item_index"] == 1, str(detail))
check("批内重复整批不落库", reports_of("B-3") == [] and batch_row("B-3") is None)


# ---------- 4. 与已发布记录编号冲突 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BF", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
tx_publish(ReportSubmit(instrument="BF", tested_at=dt(2026, 2, 15), reference="S-1"))
status, detail, _ = batch_call(ReportBatchSubmit(batch_no="B-4", items=[
    item("BF", 2026, 3, 15, "NEW-1"),
    item("BF", 2026, 4, 15, "S-1"),     # 与单份已发布编号冲突
]))
check("与已发布编号冲突 -> 409 REPORT_REFERENCE_EXISTS 带位置",
      status == 409 and detail["code"] == "REPORT_REFERENCE_EXISTS"
      and detail["item_index"] == 2 and detail["reference"] == "S-1", str(detail))
check("编号冲突整批不落库（第 1 项也不落）", reports_of("B-4") == [])
check("编号冲突不占批次号", batch_row("B-4") is None)
# 修正编号后同号重发成功
status, body, _ = batch_call(ReportBatchSubmit(batch_no="B-4", items=[
    item("BF", 2026, 3, 15, "NEW-1"),
    item("BF", 2026, 4, 15, "NEW-2"),
]))
check("冲突批次号修正后同号重发成功", status == 201 and body["count"] == 2, str(status))


# ---------- 5. 幂等：同号同内容重放 / 同号异内容拒绝 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BG", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
p = ReportBatchSubmit(batch_no="B-5", items=[
    item("BG", 2026, 3, 15, "G-1", hh=8),
    item("BG", 2026, 4, 15, "G-2", hh=9),
])
s1, b1, r1 = batch_call(p)
s2, b2, r2 = batch_call(p)
check("首次 201、重试 200 且标记重放", s1 == 201 and r1 is False and s2 == 200 and r2 is True,
      f"{s1},{r1},{s2},{r2}")
check("重试返回首次结果（report_id 等完全一致）", b1 == b2, "")
check("重试不重复落库", len(reports_of("B-5")) == 2, "")

p_diff = ReportBatchSubmit(batch_no="B-5", items=[
    item("BG", 2026, 3, 15, "G-1", hh=8),
    item("BG", 2026, 4, 16, "G-2", hh=9),   # 检测时间不同
])
status, detail, _ = batch_call(p_diff)
check("同号异内容（检测时间不同）-> 409 BATCH_NO_CONTENT_MISMATCH",
      status == 409 and detail["code"] == "BATCH_NO_CONTENT_MISMATCH", str(detail))
p_diff2 = ReportBatchSubmit(batch_no="B-5", items=[
    item("BG", 2026, 3, 15, "G-1", hh=8),
    item("BG", 2026, 4, 15, "G-9", hh=9),   # 编号不同
])
status, detail, _ = batch_call(p_diff2)
check("同号异内容（编号不同）-> 409", status == 409
      and detail["code"] == "BATCH_NO_CONTENT_MISMATCH", str(detail))
check("异内容拒绝不产生新报告", len(reports_of("B-5")) == 2, "")
# 时区写法不同但同一时刻：规范化后视为同内容 -> 重放
p_tz = ReportBatchSubmit(batch_no="B-5", items=[
    ReportSubmit(instrument="BG",
                 tested_at=datetime(2026, 3, 15, 16, 0,
                                    tzinfo=timezone(timedelta(hours=8))),  # = 08:00Z
                 reference="G-1"),
    item("BG", 2026, 4, 15, "G-2", hh=9),
])
s3, b3, r3 = batch_call(p_tz)
check("等价内容（同时刻不同时区写法）重试仍为重放", s3 == 200 and b3 == b1, str(s3))


# ---------- 6. 并发同批次重试：恰好一个首次，其余重放，报告仅一批 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BH", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
p = ReportBatchSubmit(batch_no="B-6", items=[
    item("BH", 2026, 3, 15, "H-1"),
    item("BH", 2026, 4, 15, "H-2"),
])
N = 12
barrier = threading.Barrier(N)


def race_same(i):
    barrier.wait()
    try:
        body, replayed = tx_batch(p)
        return ("ok", replayed, body)
    except HTTPException as e:
        return (e.detail["code"], None, None)


with ThreadPoolExecutor(N) as ex:
    res = list(ex.map(race_same, range(N)))
oks = [r for r in res if r[0] == "ok"]
firsts = [r for r in oks if r[1] is False]
replays = [r for r in oks if r[1] is True]
check("并发12次同批次同内容：全部成功", len(oks) == N, str([r[0] for r in res]))
check("恰好一个首次、其余重放", len(firsts) == 1 and len(replays) == N - 1,
      f"firsts={len(firsts)} replays={len(replays)}")
check("所有返回体一致（同一批报告标识）",
      len({json.dumps(r[2], sort_keys=True) for r in oks}) == 1, "")
check("报告仅落库一批（2 行）", len(reports_of("B-6")) == 2, str(len(reports_of("B-6"))))


# ---------- 7. 并发同号异内容：恰好一个成功 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BI", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
M = 8
barrier = threading.Barrier(M)


def race_diff(i):
    barrier.wait()
    try:
        body, replayed = tx_batch(ReportBatchSubmit(batch_no="B-7", items=[
            item("BI", 2026, 3, 15, f"I-{i}"),
        ]))
        return ("ok", body)
    except HTTPException as e:
        return (e.detail["code"], None)


with ThreadPoolExecutor(M) as ex:
    res = list(ex.map(race_diff, range(M)))
ok_n = sum(1 for r in res if r[0] == "ok")
mismatch_n = sum(1 for r in res if r[0] == "BATCH_NO_CONTENT_MISMATCH")
check("并发8次同号异内容：恰好一个成功", ok_n == 1, str([r[0] for r in res]))
check("其余全部 BATCH_NO_CONTENT_MISMATCH", mismatch_n == M - 1, str([r[0] for r in res]))
check("仅胜出一批落库（1 行）", len(reports_of("B-7")) == 1, "")


# ---------- 8. 并发批量 vs 校准更正：可串行、无半批；重放返回首次结果 ----------
reset()
v1 = tx_create_calibration(CalibrationCreate(
    instrument="BJ", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
K = 8
barrier = threading.Barrier(K + 1)


def race_batch(i):
    barrier.wait()
    try:
        body, replayed = tx_batch(ReportBatchSubmit(batch_no=f"B-8-{i}", items=[
            item("BJ", 2026, 6, 10, f"J-{i}-a"),
            item("BJ", 2026, 6, 11, f"J-{i}-b"),
        ]))
        return ("released", body["items"][0]["basis_calibration"]["version"])
    except HTTPException as e:
        return (e.detail["code"], e.detail.get("item_index"))


def race_corr():
    barrier.wait()
    try:
        tx_correct(CalibrationCorrect(
            corrects_id=v1["id"], version=2,
            valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=False), "BJ")
        return "corrected"
    except HTTPException as e:
        return e.detail["code"]


with ThreadPoolExecutor(K + 1) as ex:
    bf = [ex.submit(race_batch, i) for i in range(K)]
    cf = ex.submit(race_corr)
    bres = [f.result() for f in bf]
    cres = cf.result()
check("更正最终生效", cres == "corrected", str(cres))
check("批量结果仅为 整批放行(依据v1) / 第1项NOT_PASSED 两类",
      all(r == ("released", 1) or r[0] == "CALIBRATION_NOT_PASSED" and r[1] == 1
          for r in bres), str(bres))
with db.pool.connection() as c:
    n = c.execute("SELECT count(*) n FROM reports WHERE instrument='BJ'").fetchone()["n"]
    released_batches = [r for r in bres if r[0] == "released"]
check("无半批：落库行数 == 放行批次数 * 2", n == len(released_batches) * 2,
      f"rows={n} released={len(released_batches)}")
# 关键：更正生效后，对“已放行批次”的同内容重试仍返回首次结果（幂等重放，不重判定）
if released_batches:
    win = released_batches[0][0] and None
    idx = [i for i, r in enumerate(bres) if r[0] == "released"][0]
    body, replayed = tx_batch(ReportBatchSubmit(batch_no=f"B-8-{idx}", items=[
        item("BJ", 2026, 6, 10, f"J-{idx}-a"),
        item("BJ", 2026, 6, 11, f"J-{idx}-b"),
    ]))
    check("更正生效后同内容重试仍返回首次放行结果（重放不重判定）",
          replayed is True and body["items"][0]["basis_calibration"]["version"] == 1,
          str(body["items"][0]["basis_calibration"]))
else:
    check("更正生效后同内容重试仍返回首次放行结果（重放不重判定）", True, "本批全被阻断，跳过")
# 终态：新批次同点提交必被阻断（更正已生效）
status, detail, _ = batch_call(ReportBatchSubmit(batch_no="B-8-final", items=[
    item("BJ", 2026, 6, 10, "J-final"),
]))
check("更正生效后新批次 -> 422 CALIBRATION_NOT_PASSED",
      status == 422 and detail["code"] == "CALIBRATION_NOT_PASSED", str(detail))


# ---------- 9. 已生效禁用区间内并发批量：全部阻断，零误放行 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BK", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
tx_create_disablement(DisablementCreate(
    event_no="BK-E", instrument="BK",
    disabled_from=dt(2026, 1, 1), disabled_to=dt(2027, 1, 1), reason="已生效故障"))
M = 16
barrier = threading.Barrier(M)


def race_batch_blocked(i):
    barrier.wait()
    try:
        tx_batch(ReportBatchSubmit(batch_no=f"B-9-{i}", items=[
            item("BK", 2026, 6, 1, f"K-{i}-1", hh=12),
            item("BK", 2026, 6, 2, f"K-{i}-2", hh=12),
        ]))
        return "released"
    except HTTPException as e:
        return e.detail["code"]


with ThreadPoolExecutor(M) as ex:
    res = list(ex.map(race_batch_blocked, range(M)))
check("已生效区间内并发16批：全部禁用阻断，零误放行",
      set(res) == {"INSTRUMENT_DISABLED"}, str(set(res)))
with db.pool.connection() as c:
    n = c.execute("SELECT count(*) n FROM reports WHERE instrument='BK'").fetchone()["n"]
check("被阻断批次均未落库", n == 0, str(n))


# ---------- 10. 并发批量 vs 禁用撤销：可串行 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BL", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
tx_create_disablement(DisablementCreate(
    event_no="BL-E", instrument="BL",
    disabled_from=dt(2026, 6, 1), disabled_to=dt(2026, 7, 1), reason="故障"))
K = 10
barrier = threading.Barrier(K + 1)


def race_batch_rev(i):
    barrier.wait()
    try:
        tx_batch(ReportBatchSubmit(batch_no=f"B-10-{i}", items=[
            item("BL", 2026, 6, 15, f"L-{i}"),
        ]))
        return "released"
    except HTTPException as e:
        return e.detail["code"]


def race_revoke():
    barrier.wait()
    try:
        tx_revoke("BL-E")
        return "revoked"
    except HTTPException as e:
        return e.detail["code"]


with ThreadPoolExecutor(K + 1) as ex:
    bf = [ex.submit(race_batch_rev, i) for i in range(K)]
    rf = ex.submit(race_revoke)
    bres = [f.result() for f in bf]
    rres = rf.result()
check("撤销最终生效", rres == "revoked", str(rres))
check("并发批量结果仅为放行/禁用阻断两类",
      set(bres) <= {"released", "INSTRUMENT_DISABLED"}, str(set(bres)))
status, body, _ = batch_call(ReportBatchSubmit(batch_no="B-10-final", items=[
    item("BL", 2026, 6, 15, "L-final"),
]))
check("撤销生效后新批次同点放行", status == 201, str(status))


# ---------- 11. 不同批次号并发（共享仪器）：均成功 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BM", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
M = 8
barrier = threading.Barrier(M)


def race_distinct(i):
    barrier.wait()
    try:
        body, _ = tx_batch(ReportBatchSubmit(batch_no=f"B-11-{i}", items=[
            item("BM", 2026, 3, 15, f"M-{i}"),
        ]))
        return "ok"
    except HTTPException as e:
        return e.detail["code"]


with ThreadPoolExecutor(M) as ex:
    res = list(ex.map(race_distinct, range(M)))
check("不同批次号并发8批（共享仪器）：全部成功", res == ["ok"] * M, str(res))
with db.pool.connection() as c:
    n = c.execute("SELECT count(*) n FROM reports WHERE instrument='BM'").fetchone()["n"]
check("8 批共 8 行落库", n == 8, str(n))


# ---------- 12. 单份接口既有行为不变 ----------
reset()
tx_create_calibration(CalibrationCreate(
    instrument="BN", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
status, body, _ = batch_call(ReportBatchSubmit(batch_no="B-12", items=[
    item("BN", 2026, 3, 15, "N-1"),
]))
check("批量单项放行成功", status == 201, str(status))
# 单份与批量编号冲突仍 409
try:
    tx_publish(ReportSubmit(instrument="BN", tested_at=dt(2026, 4, 15), reference="N-1"))
    check("单份与批量编号冲突 -> 409", False, "未抛错")
except HTTPException as e:
    check("单份与批量编号冲突 -> 409 REPORT_REFERENCE_EXISTS",
          e.status_code == 409 and e.detail["code"] == "REPORT_REFERENCE_EXISTS",
          str(e.detail))
# 单份正常放行不受影响
try:
    r = tx_publish(ReportSubmit(instrument="BN", tested_at=dt(2026, 4, 15), reference="N-2"))
    check("单份发布行为不变（放行并返回快照）",
          r["released"] is True and r["snapshot"]["released"] is True, "")
except HTTPException as e:
    check("单份发布行为不变（放行并返回快照）", False, str(e.detail))
# 批量发布的报告在单份查询中可见且带批次号
with db.pool.connection() as c:
    row = c.execute("SELECT batch_no FROM reports WHERE reference='N-1'").fetchone()
    row2 = c.execute("SELECT batch_no FROM reports WHERE reference='N-2'").fetchone()
check("批量行带批次号、单份行批次号为 NULL",
      row["batch_no"] == "B-12" and row2["batch_no"] is None, str((row, row2)))


# ---------- 13. 请求形态：空条目/空批次号模型层拒绝 ----------
try:
    ReportBatchSubmit(batch_no="B-13", items=[])
    check("空条目被拒绝", False, "未抛错")
except ValidationError:
    check("空条目被模型层拒绝（min_length=1）", True)
try:
    ReportBatchSubmit(batch_no="", items=[item("BN", 2026, 3, 15)])
    check("空批次号被拒绝", False, "未抛错")
except ValidationError:
    check("空批次号被模型层拒绝", True)


print()
if failures:
    print(f"{len(failures)} 项失败:", failures)
    sys.exit(1)
print("全部整批放行验证通过")
