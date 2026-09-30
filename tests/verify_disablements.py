"""禁用区间并发与边界正确性验证（直连数据库，不走 HTTP）。

覆盖：
 1. 同仪器并发登记重叠区间：恰好一个成功，其余 409 INTERVAL_OVERLAP，无脏行
 2. 并发“禁用登记 vs 报告提交”：禁用区间内绝无误放行（核心不变量）
 3. 并发“禁用撤销 vs 报告提交”：结果可串行，要么放行（撤销先）要么阻断
 4. 并发“校准更正 vs 禁用登记 vs 报告提交”：不会出现禁用区间内放行
 5. 唯一事件号并发复用：恰好一个成功
 6. 同事件并发撤销：恰好一个成功
 7. 边界相接/左闭右开/开放区间/无效区间/重复事件号/未知与重复撤销
 8. 追溯登记/撤销改变 affected_reports，但已发布报告与快照不改写
"""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi import HTTPException

import app.db as db
from app.models import CalibrationCorrect, CalibrationCreate, DisablementCreate, ReportSubmit
from app.repository import (
    affected_reports,
    create_calibration,
    create_disablement,
    publish_report,
    correct_calibration,
    instrument_of_calibration,
    prelock_event,
    prelock_event_then_instrument,
    prelock_instrument,
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


def tx_create_calibration(payload):
    return run_tx(lambda c: create_calibration(c, payload),
                  prelock=prelock_instrument(payload.instrument))


def tx_create_disablement(payload):
    return run_tx(lambda c: create_disablement(c, payload),
                  prelock=prelock_event_then_instrument(payload.event_no, payload.instrument))


def tx_revoke(event_no):
    return run_tx(lambda c: revoke_disablement(c, event_no),
                  prelock=prelock_event(event_no))


def tx_publish(payload):
    return run_tx(lambda c: publish_report(c, payload),
                  prelock=prelock_instrument(payload.instrument))


def tx_correct(payload):
    with db.pool.connection() as probe:
        instrument = instrument_of_calibration(probe, payload.corrects_id)
    assert instrument is not None
    return run_tx(lambda c: correct_calibration(c, payload),
                  prelock=prelock_instrument(instrument))


def http_call(kind, payload):
    """模拟真实 HTTP 路径：携带与路由一致的事务外预锁。返回 (status, body/detail)。"""
    fn = {
        "cal": tx_create_calibration,
        "dis": tx_create_disablement,
        "rev": tx_revoke,
        "pub": tx_publish,
        "cor": tx_correct,
    }[kind]
    try:
        result = fn(payload)
        if isinstance(result, tuple):
            result = result[0]
        return 200, result
    except HTTPException as e:
        return e.status_code, e.detail


def code_of(exc):
    return exc.detail["code"]


# ---------- 1. 并发登记重叠禁用区间 ----------
reset()
N = 10
barrier = threading.Barrier(N)


def race_register(i):
    barrier.wait()
    payload = DisablementCreate(
        event_no=f"OVL-{i}", instrument="D1",
        disabled_from=dt(2026, 4, 1), disabled_to=dt(2026, 5, 1),
        reason=f"故障 {i}")
    try:
        tx_create_disablement(payload)
        return "ok"
    except HTTPException as e:
        return code_of(e)
    except Exception as e:  # pragma: no cover
        return "OTHER:" + type(e).__name__


with ThreadPoolExecutor(N) as ex:
    results = list(ex.map(race_register, range(N)))
ok = [r for r in results if r == "ok"]
ovl = [r for r in results if r == "INTERVAL_OVERLAP"]
others = [r for r in results if r not in ("ok", "INTERVAL_OVERLAP")]
check("并发10个重叠禁用：恰好1个成功", len(ok) == 1, str(results))
check("并发10个重叠禁用：其余全部 INTERVAL_OVERLAP", len(ovl) == 9 and not others, str(others))
with db.pool.connection() as c:
    n = c.execute(
        "SELECT count(*) n FROM instrument_disablements WHERE instrument='D1'").fetchone()["n"]
check("失败事务未留脏行（仅1条）", n == 1, str(n))


# ---------- 2a. 已生效禁用区间内并发提交：必须全部阻断，无一误放行（确定性核心） ----------
reset()
s, v1 = http_call("cal", CalibrationCreate(
    instrument="D2", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
assert s == 200
# 先登记一个覆盖“当前检测时间”的禁用区间（相对登记时刻，检测点可在区间内任意位置）
http_call("dis", DisablementCreate(
    event_no="D2-BLOCK", instrument="D2",
    disabled_from=dt(2026, 1, 1), disabled_to=dt(2027, 1, 1), reason="已生效故障"))

M = 24
barrier = threading.Barrier(M)
tested = dt(2026, 6, 1, 12)  # 落在已生效禁用区间内


def race_publish(i):
    barrier.wait()
    try:
        r = tx_publish(ReportSubmit(
            instrument="D2", tested_at=tested, reference=f"p-{i}"))
        return ("released", r["report_id"])
    except HTTPException as e:
        return (e.detail["code"], None)


with ThreadPoolExecutor(M) as ex:
    pub = list(ex.map(race_publish, range(M)))
codes = {p[0] for p in pub}
check("已生效区间内并发24份提交：全部禁用阻断，零误放行",
      codes == {"INSTRUMENT_DISABLED"}, str([p[0] for p in pub]))
with db.pool.connection() as c:
    n = c.execute("SELECT count(*) n FROM reports WHERE instrument='D2'").fetchone()["n"]
check("被阻断报告均未落库", n == 0, str(n))

# 撤销后同点并发提交：全部放行（禁用失效，沿用校准判定）
http_call("rev", "D2-BLOCK")
barrier = threading.Barrier(M)
with ThreadPoolExecutor(M) as ex:
    pub2 = list(ex.map(race_publish, range(M)))
check("撤销后并发24份同点提交：全部放行",
      all(p[0] == "released" for p in pub2), str([p[0] for p in pub2]))


# ---------- 2b. 禁用登记与提交并发（含追溯窗口）：结果可串行，无“登记后”误放行 ----------
reset()
http_call("cal", CalibrationCreate(
    instrument="D2B", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
# 窗口位于过去（追溯登记）：登记提交前已放行的报告合法保留（快照不可变），
# 登记提交之后的提交必须阻断。两类结果都允许，关键是不存在第三类脏结果。
M = 24
barrier = threading.Barrier(M + 1)
tested = dt(2026, 6, 1, 12)


def race_publish_b(i):
    barrier.wait()
    try:
        tx_publish(ReportSubmit(
            instrument="D2B", tested_at=tested, reference=f"b-{i}"))
        return "released"
    except HTTPException as e:
        return e.detail["code"]


def race_disable_b():
    barrier.wait()
    try:
        tx_create_disablement(DisablementCreate(
            event_no="D2B-BLOCK", instrument="D2B",
            disabled_from=dt(2026, 6, 1), disabled_to=dt(2026, 7, 1),
            reason="追溯故障"))
        return "registered"
    except HTTPException as e:
        return e.detail["code"]


with ThreadPoolExecutor(M + 1) as ex:
    pf = [ex.submit(race_publish_b, i) for i in range(M)]
    df = ex.submit(race_disable_b)
    pubb = [f.result() for f in pf]
    dres = df.result()
check("追溯禁用最终登记成功", dres == "registered", str(dres))
check("竞态结果仅为放行（登记前）或禁用阻断（登记后）两类",
      set(pubb) <= {"released", "INSTRUMENT_DISABLED"}, str(pubb))
# 尘埃落定后，新的区间内提交必然阻断（不可能再有放行）
s, d = http_call("pub", ReportSubmit(
    instrument="D2B", tested_at=tested, reference="after-settle"))
check("竞态结束后区间内新提交必阻断", s == 422 and d["code"] == "INSTRUMENT_DISABLED", str(d))


# ---------- 3. 撤销 vs 提交 ----------
def race_revoke_publish():
    reset()
    http_call("cal", CalibrationCreate(
        instrument="D3", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
    http_call("dis", DisablementCreate(
        event_no="D3-E", instrument="D3",
        disabled_from=dt(2026, 6, 1), disabled_to=dt(2026, 7, 1), reason="故障"))

    K = 16
    barrier = threading.Barrier(K + 1)

    def pub(i):
        barrier.wait()
        try:
            tx_publish(ReportSubmit(
                instrument="D3", tested_at=dt(2026, 6, 15), reference=f"q-{i}"))
            return "released"
        except HTTPException as e:
            return code_of(e)

    def rev():
        barrier.wait()
        try:
            tx_revoke("D3-E")
            return "revoked"
        except HTTPException as e:
            return code_of(e)

    with ThreadPoolExecutor(K + 1) as ex:
        pf = [ex.submit(pub, i) for i in range(K)]
        rf = ex.submit(rev)
        pres = [f.result() for f in pf]
        rres = rf.result()
    return rres, pres


rres, pres = race_revoke_publish()
check("撤销事务最终成功", rres == "revoked", str(rres))
check("并发提交结果仅为放行/禁用阻断",
      all(p in ("released", "INSTRUMENT_DISABLED") for p in pres), str(pres))
# 撤销后区间已空：再提交同点必然放行（终态判定即可串行化）
s, _ = http_call("pub", ReportSubmit(
    instrument="D3", tested_at=dt(2026, 6, 15), reference="probe"))
check("撤销生效后提交的区间内报告全部放行（探测放行）", s == 200, str(s))
s, r = http_call("pub", ReportSubmit(
    instrument="D3", tested_at=dt(2026, 6, 15), reference="after-revoke"))
check("撤销后直接提交 -> 放行", s == 200 and r["reason_code"] == "PASSED_CALIBRATION_COVERAGE", str(s))


# ---------- 4. 更正 + 禁用 + 发布 三方并发 ----------
reset()
s, v1 = http_call("cal", CalibrationCreate(
    instrument="D4", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
assert s == 200
L = 16
barrier = threading.Barrier(L + 2)


def pub4(i):
    barrier.wait()
    try:
        r = tx_publish(ReportSubmit(
            instrument="D4", tested_at=dt(2026, 6, 10), reference=f"t-{i}"))
        return ("released", r["basis_calibration"]["version"])
    except HTTPException as e:
        return (code_of(e), None)


def corr4():
    barrier.wait()
    try:
        tx_correct(CalibrationCorrect(
            corrects_id=v1["id"], version=2,
            valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=False))
        return "corrected"
    except HTTPException as e:
        return code_of(e)


def dis4():
    barrier.wait()
    try:
        tx_create_disablement(DisablementCreate(
            event_no="D4-E", instrument="D4",
            disabled_from=dt(2026, 6, 1), disabled_to=dt(2026, 7, 1), reason="故障"))
        return "registered"
    except HTTPException as e:
        return code_of(e)


with ThreadPoolExecutor(L + 2) as ex:
    pf = [ex.submit(pub4, i) for i in range(L)]
    cf = ex.submit(corr4)
    df = ex.submit(dis4)
    pres = [f.result() for f in pf]
    cres = cf.result()
    dres2 = df.result()

check("三方并发：更正最终生效", cres == "corrected", str(cres))
check("三方并发：禁用最终生效", dres2 == "registered", str(dres2))
codes = {p[0] for p in pres}
check("三方并发：发布结果只可能是 放行/未通过/禁用阻断",
      codes <= {"released", "CALIBRATION_NOT_PASSED", "INSTRUMENT_DISABLED"}, str(codes))
# 任何“放行”必须依据 passed=TRUE 的校准（数据库核对）。
# 注意：D4-E 为追溯窗口，登记提交前已放行的报告合法保留（快照不可变）；
# 严格的“已生效区间内零误放行”由第 2a 节确定性并发与第 10 节持锁互斥证明。
with db.pool.connection() as c:
    bad_pass = c.execute("""
        SELECT r.id FROM reports r
        JOIN calibration_versions cv ON cv.id = r.basis_calibration_id
        WHERE r.instrument='D4' AND cv.passed = FALSE
    """).fetchall()
check("三方并发：放行依据全部为通过校准", not bad_pass, str(bad_pass))
# 三方都生效后：禁用优先于未通过校准
s, d = http_call("pub", ReportSubmit(
    instrument="D4", tested_at=dt(2026, 6, 10), reference="final"))
check("最终状态：禁用区间内 -> INSTRUMENT_DISABLED（优先于 NOT_PASSED）",
      s == 422 and d["code"] == "INSTRUMENT_DISABLED", str(d))
s, d = http_call("pub", ReportSubmit(
    instrument="D4", tested_at=dt(2026, 8, 10), reference="final2"))
check("最终状态：区间外但校准未通过 -> CALIBRATION_NOT_PASSED",
      s == 422 and d["code"] == "CALIBRATION_NOT_PASSED", str(d))


# ---------- 5. 唯一事件号并发复用 ----------
reset()


def race_same_event(i):
    try:
        tx_create_disablement(DisablementCreate(
            event_no="UNIQUE-E", instrument=f"X{i % 3}",
            disabled_from=dt(2026, 1, 1), disabled_to=dt(2026, 2, 1), reason="x"))
        return "ok"
    except HTTPException as e:
        return code_of(e)


with ThreadPoolExecutor(8) as ex:
    res = list(ex.map(race_same_event, range(8)))
check("同事件号并发8次：恰好1个成功", res.count("ok") == 1, str(res))
check("同事件号并发8次：其余 DISABLEMENT_EVENT_EXISTS",
      all(r in ("ok", "DISABLEMENT_EVENT_EXISTS") for r in res), str(res))


# ---------- 6. 同事件并发撤销 ----------
reset()
http_call("dis", DisablementCreate(
    event_no="REV-E", instrument="R1",
    disabled_from=dt(2026, 1, 1), disabled_to=dt(2026, 2, 1), reason="x"))


def race_revoke(i):
    try:
        tx_revoke("REV-E")
        return "ok"
    except HTTPException as e:
        return code_of(e)


with ThreadPoolExecutor(8) as ex:
    res = list(ex.map(race_revoke, range(8)))
check("同事件并发撤销8次：恰好1个成功", res.count("ok") == 1, str(res))
check("同事件并发撤销8次：其余 ALREADY_REVOKED",
      all(r in ("ok", "DISABLEMENT_ALREADY_REVOKED") for r in res), str(res))
with db.pool.connection() as c:
    row = c.execute(
        "SELECT revoked, revoked_at FROM instrument_disablements WHERE event_no='REV-E'").fetchone()
check("撤销状态唯一且带时间戳", row["revoked"] and row["revoked_at"] is not None)


# ---------- 7. 边界 / 端点 / 开放区间 / 拒绝码 ----------
reset()
http_call("cal", CalibrationCreate(
    instrument="D7", valid_from=dt(2026, 1, 1), valid_to=dt(2028, 1, 1), passed=True))
# 相接区间登记
s, a = http_call("dis", DisablementCreate(
    event_no="D7-1", instrument="D7",
    disabled_from=dt(2026, 4, 1), disabled_to=dt(2026, 5, 1), reason="a"))
s, b = http_call("dis", DisablementCreate(
    event_no="D7-2", instrument="D7",
    disabled_from=dt(2026, 5, 1), disabled_to=dt(2026, 6, 1), reason="b"))
check("禁用区间边界相接允许", s == 200, str(s))
s, d = http_call("pub", ReportSubmit(
    instrument="D7", tested_at=dt(2026, 4, 30, 23), reference="z1"))
check("4/30 23:00 命中前段禁用", s == 422 and d["code"] == "INSTRUMENT_DISABLED"
      and d["basis_disablement"]["event_no"] == "D7-1", str(d))
s, d = http_call("pub", ReportSubmit(
    instrument="D7", tested_at=dt(2026, 5, 1), reference="z2"))
check("恰 5/1 命中后段禁用（左闭右开）", s == 422
      and d["basis_disablement"]["event_no"] == "D7-2", str(d))
s, _ = http_call("pub", ReportSubmit(
    instrument="D7", tested_at=dt(2026, 3, 31), reference="z3"))
check("3/31 区间外正常放行", s == 200)

# 开放区间
s, _ = http_call("dis", DisablementCreate(
    event_no="D7-open", instrument="D7",
    disabled_from=dt(2026, 11, 1), disabled_to=None, reason="长期"))
check("开放禁用区间登记成功", s == 200)
s, d = http_call("pub", ReportSubmit(
    instrument="D7", tested_at=dt(2035, 1, 1), reference="z4"))
check("开放区间覆盖远未来并阻断", s == 422 and d["code"] == "INSTRUMENT_DISABLED", str(d))
# 开放区间与有限区间重叠
s, d = http_call("dis", DisablementCreate(
    event_no="D7-open2", instrument="D7",
    disabled_from=dt(2030, 1, 1), disabled_to=None, reason="x"))
check("开放区间互相重叠 -> 409", s == 409 and d["code"] == "INTERVAL_OVERLAP", str(d))

# 无效区间（含终点早于起点）
s, d = http_call("dis", DisablementCreate(
    event_no="D7-bad", instrument="D7",
    disabled_from=dt(2026, 9, 1), disabled_to=dt(2026, 8, 1), reason="x"))
check("终点早于起点 -> 422 INVALID_INTERVAL", s == 422 and d["code"] == "INVALID_INTERVAL", str(d))

# 重复事件号（含已撤销）
s, d = http_call("dis", DisablementCreate(
    event_no="D7-1", instrument="D7",
    disabled_from=dt(2026, 8, 1), disabled_to=dt(2026, 9, 1), reason="dup"))
check("事件号重复 -> 409", s == 409 and d["code"] == "DISABLEMENT_EVENT_EXISTS")
http_call("rev", "D7-1")
s, d = http_call("dis", DisablementCreate(
    event_no="D7-1", instrument="D7",
    disabled_from=dt(2026, 8, 1), disabled_to=dt(2026, 9, 1), reason="dup2"))
check("已撤销事件号仍不可复用 -> 409", s == 409 and d["code"] == "DISABLEMENT_EVENT_EXISTS")

# 撤销后区间可与历史重叠
s, _ = http_call("dis", DisablementCreate(
    event_no="D7-3", instrument="D7",
    disabled_from=dt(2026, 4, 10), disabled_to=dt(2026, 4, 20), reason="覆盖已撤销"))
check("撤销后区间空出，可登记重叠新事件", s == 200, str(s))


# ---------- 8. affected_reports 随登记/撤销变化，快照不改写 ----------
reset()
http_call("cal", CalibrationCreate(
    instrument="D8", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))
s, rep = http_call("pub", ReportSubmit(
    instrument="D8", tested_at=dt(2026, 3, 15), reference="keep-1"))
rid = rep["report_id"]
check("准备：报告先放行", s == 200)

with db.pool.connection() as c:  # affected 查询只读
    items = affected_reports(c, "D8")
check("无禁用时受影响报告为空", items == [])

http_call("dis", DisablementCreate(
    event_no="D8-E", instrument="D8",
    disabled_from=dt(2026, 3, 1), disabled_to=dt(2026, 4, 1), reason="追溯故障"))
with db.pool.connection() as c:
    items = affected_reports(c, "D8")
check("追溯登记后受影响报告含 keep-1 及禁用依据",
      len(items) == 1 and items[0]["report_id"] == rid
      and items[0]["disablement_basis"]["event_no"] == "D8-E", str(items))
check("随附的放行快照仍是 released=True（不改写）",
      items[0]["release_snapshot"]["released"] is True)

http_call("rev", "D8-E")
with db.pool.connection() as c:
    items = affected_reports(c, "D8")
check("撤销后受影响报告清空", items == [])
with db.pool.connection() as c:
    row = c.execute("SELECT released, snapshot FROM reports WHERE id=%s", (rid,)).fetchone()
check("原报告记录 released 与快照均未改写",
      row["released"] is True and row["snapshot"]["released"] is True, str(row))


# ---------- 9. 无校准仪器仍可登记禁用，且发布优先报禁用 ----------
reset()
s, _ = http_call("dis", DisablementCreate(
    event_no="D9-E", instrument="D9",
    disabled_from=dt(2026, 1, 1), disabled_to=None, reason="到货即故障"))
check("无校准仪器也可登记禁用", s == 200)
s, d = http_call("pub", ReportSubmit(
    instrument="D9", tested_at=dt(2026, 6, 1)))
check("无校准+禁用 -> 优先 INSTRUMENT_DISABLED（而非 NO_CALIBRATION）",
      s == 422 and d["code"] == "INSTRUMENT_DISABLED", str(d))
http_call("rev", "D9-E")
s, d = http_call("pub", ReportSubmit(
    instrument="D9", tested_at=dt(2026, 6, 1)))
check("撤销后无校准 -> NO_CALIBRATION（沿用原判定）",
      s == 422 and d["code"] == "NO_CALIBRATION", str(d))


# ---------- 10. 确定性互斥：禁用持锁未提交时，区间内发布必须等待，提交后阻断 ----------
# 复现生产路径：禁用在“独立 autocommit 连接上先拿会话级仪器锁”，随后在
# SERIALIZABLE 事务里写禁用行但暂不提交；此时并发发布须在取锁阶段被阻塞，
# 其 SERIALIZABLE 快照只能在禁用提交后建立，故必读到禁用并阻断。
reset()
http_call("cal", CalibrationCreate(
    instrument="D10", valid_from=dt(2026, 1, 1), valid_to=dt(2027, 1, 1), passed=True))

from app.repository import _lock_key

c1 = db.pool.getconn()
holder = {"done": False}
t = None
try:
    c1.autocommit = True
    # 1) 先拿会话级仪器锁（与 run_tx 的 prelock 完全一致）
    c1.execute("SELECT pg_advisory_lock(%s)",
               (_lock_key("instrument\x00D10"),))
    # 2) 同连接开启 SERIALIZABLE 事务写禁用行，但不提交（裸 BEGIN 控制提交点）
    c1.autocommit = False
    c1.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
    c1.execute(
        "INSERT INTO instrument_disablements (event_no, instrument, disabled_from,"
        " disabled_to, reason) VALUES ('D10-E','D10',%s,%s,'待提交故障')",
        (dt(2026, 6, 1), dt(2026, 7, 1)))

    def concurrent_publish():
        # 与生产一致：prelock 阶段（autocommit 会话锁）被 c1 阻塞
        try:
            r = tx_publish(ReportSubmit(
                instrument="D10", tested_at=dt(2026, 6, 15), reference="held"))
            holder["result"] = ("released", r["report_id"])
        except HTTPException as e:
            holder["result"] = (code_of(e), None)
        except Exception as e:  # pragma: no cover - 不应出现
            holder["result"] = ("OTHER:" + type(e).__name__, str(e)[:120])
        finally:
            holder["done"] = True

    t = threading.Thread(target=concurrent_publish)
    t.start()
    t.join(timeout=2.0)
    check("禁用持锁未提交时，区间内发布在取锁阶段被阻塞（未提前放行）",
          not holder["done"], f"done={holder.get('done')}")
    # 提交禁用行
    c1.execute("COMMIT")
    # 与生产 run_tx 一致：事务结束后立即释放会话锁，唤醒等待中的发布
    c1.autocommit = True
    c1.execute("SELECT pg_advisory_unlock_all()")
    t.join(timeout=10.0)
    t = None
    check("禁用提交并放锁后，被阻塞的发布被唤醒并阻断（不能误放行）",
          holder["done"] and holder["result"][0] == "INSTRUMENT_DISABLED",
          str(holder.get("result")))
finally:
    # 仍有未收敛线程时：先提交/解锁并等待，再归还连接（避免锁随连接复用被误解）
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

# 对称场景：发布先持锁进行中，禁用登记必须等待，不能读到半生效状态（同一把仪器锁保证）
c1 = db.pool.getconn()
holder2 = {"done": False}
t2 = None
try:
    c1.autocommit = True
    c1.execute("SELECT pg_advisory_lock(%s)",
               (_lock_key("instrument\x00D10"),))  # 模拟“发布”握有仪器锁

    def concurrent_disable():
        try:
            tx_create_disablement(DisablementCreate(
                event_no="D10-E2", instrument="D10",
                disabled_from=dt(2026, 8, 1), disabled_to=dt(2026, 9, 1), reason="x"))
            holder2["result"] = "registered"
        except HTTPException as e:
            holder2["result"] = code_of(e)
        finally:
            holder2["done"] = True

    t2 = threading.Thread(target=concurrent_disable)
    t2.start()
    t2.join(timeout=2.0)
    check("发布持锁期间，禁用登记在取锁阶段被阻塞",
          not holder2["done"], f"done={holder2.get('done')}")
    c1.execute("SELECT pg_advisory_unlock_all()")
    t2.join(timeout=10.0)
    t2 = None
    check("仪器锁释放后禁用登记成功", holder2.get("result") == "registered",
          str(holder2.get("result")))
finally:
    if t2 is not None:
        try:
            c1.execute("SELECT pg_advisory_unlock_all()")
        except Exception:
            pass
        t2.join(timeout=10.0)
    try:
        c1.autocommit = True
        c1.execute("SELECT pg_advisory_unlock_all()")
    except Exception:
        pass
    db.pool.putconn(c1)

# 终态再确认：区间内直接发布被阻断
s, d = http_call("pub", ReportSubmit(
    instrument="D10", tested_at=dt(2026, 6, 15), reference="held-final"))
check("互斥后终态：区间内发布阻断", s == 422 and d["code"] == "INSTRUMENT_DISABLED", str(d))


print()
if failures:
    print(f"{len(failures)} 项失败:", failures)
    sys.exit(1)
print("全部禁用并发/边界验证通过")
