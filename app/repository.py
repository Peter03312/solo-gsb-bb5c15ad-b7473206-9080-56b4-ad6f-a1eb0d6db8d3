"""领域逻辑：校准判定、版本更正、禁用区间、发布（单份/整批）、快照漂移重算。"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from psycopg.errors import ExclusionViolation, UniqueViolation

from .errors import blocked, conflict, not_found

# ---- 判定原因码 ----------------------------------------------------------------
# 放行成功
REASON_PASSED = "PASSED_CALIBRATION_COVERAGE"
# 该仪器当前没有任何校准版本
REASON_NO_CALIBRATION = "NO_CALIBRATION"
# 存在当前校准版本，但检测时间不落在任何 [valid_from, valid_to) 内
REASON_OUTSIDE_PERIOD = "OUTSIDE_VALID_PERIOD"
# 覆盖检测时间的当前校准版本结论为未通过
REASON_NOT_PASSED = "CALIBRATION_NOT_PASSED"
# 检测时间命中该仪器一条未撤销的禁用区间（即使校准通过也拒绝放行）
REASON_INSTRUMENT_DISABLED = "INSTRUMENT_DISABLED"

# ---- 咨询锁 --------------------------------------------------------------------
# 每仪器一把锁（校准写、禁用写、发布共用），另设每事件一把锁（撤销/防事件号竞争）。
# 锁必须在 SERIALIZABLE 事务之外经 prelock 预取（见 transactions.run_tx），
# 否则事务快照会在等待锁期间提前固定而读到旧状态。
# pg_advisory_lock 的双参数重载只支持 (int4,int4)，故用单参数 bigint 形式：
# 对“命名空间前缀 + 标识”做 64 位 FNV-1a 哈希，键空间约 2^63，
# 仪器/事件两类前缀互不串扰，标识增多也不存在实际碰撞。
_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3
_MASK63 = 0x7FFFFFFFFFFFFFFF


def _lock_key(text: str) -> int:
    """文本稳定映射到 pg_advisory_lock(bigint) 的非负有符号 64 位 key（FNV-1a）。"""
    h = _FNV_OFFSET
    for b in text.encode("utf-8"):
        h ^= b
        h = (h * _FNV_PRIME) & 0xFFFFFFFFFFFFFFFF
    return h & _MASK63


def prelock_instrument(instrument: str):
    """返回给 run_tx(prelock=...) 使用的会话锁回调（autocommit 连接上执行）。"""
    key = _lock_key(f"instrument\x00{instrument}")

    def _acquire(lock_conn):
        lock_conn.execute("SELECT pg_advisory_lock(%s)", (key,))
    return _acquire


def prelock_event(event_no: str):
    key = _lock_key(f"event\x00{event_no}")

    def _acquire(lock_conn):
        lock_conn.execute("SELECT pg_advisory_lock(%s)", (key,))
    return _acquire


def prelock_event_then_instrument(event_no: str, instrument: str):
    """固定加锁顺序：先事件、再仪器，杜绝锁环。"""
    event_key = _lock_key(f"event\x00{event_no}")
    instrument_key = _lock_key(f"instrument\x00{instrument}")

    def _acquire(lock_conn):
        lock_conn.execute("SELECT pg_advisory_lock(%s)", (event_key,))
        lock_conn.execute("SELECT pg_advisory_lock(%s)", (instrument_key,))
    return _acquire


def prelock_batch(batch_no: str, instruments: list[str]):
    """整批放行的预锁：先批次锁，再按字典序逐个取全部涉及仪器锁。

    全局锁序统一为“命名锁（event/batch，至多一把）-> 仪器锁（升序）”，
    任何请求都不会持仪器锁等命名锁，故无锁环：
    - 批次锁使同批次号的并发重试串行——先到者提交后，重试者读到首次结果；
    - 仪器锁与单份发布/校准写/禁用登记互斥，整批判定在取锁后的快照上进行，
      不会读到半生效的并发更正/禁用，也不会被并发单份发布插队。
    """
    batch_key = _lock_key(f"batch\x00{batch_no}")
    instrument_keys = [
        _lock_key(f"instrument\x00{name}") for name in sorted(set(instruments))
    ]

    def _acquire(lock_conn):
        lock_conn.execute("SELECT pg_advisory_lock(%s)", (batch_key,))
        for key in instrument_keys:
            lock_conn.execute("SELECT pg_advisory_lock(%s)", (key,))
    return _acquire

_COVERAGE_SQL = """
    SELECT *
    FROM calibration_versions
    WHERE instrument = %s
      AND is_current = TRUE
      AND valid_from <= %s
      AND (valid_to IS NULL OR %s < valid_to)
"""

_NEAREST_SQL = """
    SELECT *
    FROM calibration_versions
    WHERE instrument = %s AND is_current = TRUE
    ORDER BY (valid_from <= %s) DESC,
             CASE WHEN valid_from <= %s THEN valid_to END DESC NULLS LAST,
             valid_from ASC
"""


def _cal_brief(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "chain_id": row["chain_id"],
        "version": row["version"],
        "valid_from": row["valid_from"].isoformat(),
        "valid_to": row["valid_to"].isoformat() if row["valid_to"] else None,
        "passed": row["passed"],
    }


# ---- 禁用区间 ------------------------------------------------------------------

_DISABLEMENT_COVERAGE_SQL = """
    SELECT *
    FROM instrument_disablements
    WHERE instrument = %s
      AND revoked = FALSE
      AND disabled_from <= %s
      AND (disabled_to IS NULL OR %s < disabled_to)
"""


def disablement_brief(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "event_no": row["event_no"],
        "instrument": row["instrument"],
        "disabled_from": row["disabled_from"].isoformat(),
        "disabled_to": row["disabled_to"].isoformat() if row["disabled_to"] else None,
        "reason": row["reason"],
        "revoked": row["revoked"],
    }


def find_active_disablement(conn, instrument: str, at: datetime,
                            *, lock: bool) -> dict[str, Any] | None:
    """命中未撤销禁用区间（左闭右开）则返回该区间，否则 None。

    ``lock=True`` 对命中行加 FOR UPDATE，与并发登记/撤销互斥；排他约束保证
    同仪器未撤销区间不重叠，故至多一条。
    """
    sql = _DISABLEMENT_COVERAGE_SQL + (" FOR UPDATE" if lock else "")
    return conn.execute(sql, (instrument, at, at)).fetchone()


def validate_disablement_payload(payload) -> None:
    """区间形态校验（不持锁、不入库），在取事务/咨询锁之前拒绝无效请求。"""
    if payload.disabled_to is not None and payload.disabled_to <= payload.disabled_from:
        raise blocked("INVALID_INTERVAL",
                      "禁用区间必须满足 disabled_from < disabled_to（左闭右开）")


def create_disablement(conn, payload) -> dict[str, Any]:
    """登记禁用区间。无效区间/事件号复用/未撤销区间重叠均拒绝。

    区间形态由路由在取锁前经 validate_disablement_payload 预检（此处再防御一次）；
    仪器锁与事件锁由调用方经 run_tx(prelock=...) 在事务外预取。
    """
    validate_disablement_payload(payload)
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO instrument_disablements
                    (event_no, instrument, disabled_from, disabled_to, reason)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *
                """,
                (payload.event_no, payload.instrument, payload.disabled_from,
                 payload.disabled_to, payload.reason),
            )
        except UniqueViolation:
            raise conflict("DISABLEMENT_EVENT_EXISTS",
                           f"禁用事件号已存在（含已撤销记录，事件号不可复用）: {payload.event_no}",
                           event_no=payload.event_no)
        except ExclusionViolation:
            raise conflict("INTERVAL_OVERLAP",
                           "与该仪器另一条未撤销禁用区间重叠（边界相接允许）",
                           instrument=payload.instrument)
        return cur.fetchone()


def revoke_disablement(conn, event_no: str) -> dict[str, Any]:
    """按事件号撤销禁用。未知事件 404，重复撤销 409。记录保留。

    事件锁由调用方经 run_tx(prelock=prelock_event(...)) 在事务外预取：
    事件全局唯一，先取事件锁足以与同事件的并发撤销/复用登记互斥；与发布之间
    由 SERIALIZABLE 谓词依赖 + 发布方仪器锁保证可串行（不会在已生效区间放行）。
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM instrument_disablements WHERE event_no = %s FOR UPDATE",
            (event_no,),
        )
        row = cur.fetchone()
        if row is None:
            raise not_found("禁用事件", event_no)
        if row["revoked"]:
            raise conflict("DISABLEMENT_ALREADY_REVOKED",
                           f"禁用事件已撤销，不能重复撤销: {event_no}",
                           event_no=event_no,
                           revoked_at=row["revoked_at"].isoformat())
        cur.execute(
            "UPDATE instrument_disablements SET revoked = TRUE, "
            "revoked_at = clock_timestamp() WHERE id = %s RETURNING *",
            (row["id"],),
        )
        return cur.fetchone()


def list_disablements(conn, instrument: str | None = None,
                      active_only: bool = False) -> list[dict[str, Any]]:
    sql = "SELECT * FROM instrument_disablements"
    where, params = [], []
    if instrument:
        where.append("instrument = %s")
        params.append(instrument)
    if active_only:
        where.append("revoked = FALSE")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY instrument, disabled_from, id"
    return conn.execute(sql, params).fetchall()


_AFFECTED_REPORTS_SQL = """
    SELECT r.id AS report_id, r.reference, r.instrument, r.tested_at,
           r.snapshot, d.*
    FROM reports r
    JOIN instrument_disablements d
      ON d.instrument = r.instrument
     AND d.revoked = FALSE
     AND d.disabled_from <= r.tested_at
     AND (d.disabled_to IS NULL OR r.tested_at < d.disabled_to)
    {where}
    ORDER BY r.instrument, r.tested_at, r.id
"""


def affected_reports(conn, instrument: str | None = None) -> list[dict[str, Any]]:
    """按当前禁用状态，返回检测点命中未撤销禁用区间的已发布报告及禁用依据。

    实时计算、只读：登记/撤销后结果随之变化；原放行记录与快照不改写。
    未撤销区间不重叠，故每份报告至多命中一条禁用依据，不会重复。
    """
    where, params = "", []
    if instrument:
        where = "WHERE r.instrument = %s"
        params.append(instrument)
    rows = conn.execute(_AFFECTED_REPORTS_SQL.format(where=where), params).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append({
            "report_id": row["report_id"],
            "reference": row["reference"],
            "instrument": row["instrument"],
            "tested_at": row["tested_at"].isoformat(),
            "release_snapshot": row["snapshot"],
            "disablement_basis": disablement_brief(row),
        })
    return items


def determine(conn, instrument: str, tested_at: datetime, *, lock: bool) -> dict[str, Any]:
    """返回该时点的校准判定。

    ``lock=True`` 时对覆盖行加 FOR UPDATE 行锁，并在无覆盖时锁定该仪器全部
    当前版本 —— 与校准更正事务互斥，保证“发布”与“更正”可串行化。
    """
    sql = _COVERAGE_SQL + (" FOR UPDATE" if lock else "")
    with conn.cursor() as cur:
        cur.execute(sql, (instrument, tested_at, tested_at))
        covering = cur.fetchall()

    if covering:
        row = covering[0]  # 排除约束保证同仪器当前版本区间不重叠，至多一条
        if row["passed"]:
            return {"releasable": True, "reason_code": REASON_PASSED,
                    "calibration": row, "reason_message": None}
        return {
            "releasable": False, "reason_code": REASON_NOT_PASSED,
            "calibration": row,
            "reason_message": f"检测时间 {tested_at.isoformat()} 被校准版本 "
                              f"v{row['version']} 覆盖，但其结论为未通过",
        }

    # 无覆盖：给出可操作的阻断原因
    with conn.cursor() as cur:
        if lock:
            cur.execute(
                "SELECT id FROM calibration_versions "
                "WHERE instrument = %s AND is_current = TRUE FOR UPDATE",
                (instrument,),
            )
            cur.fetchall()
        cur.execute(
            "SELECT count(*) AS n FROM calibration_versions "
            "WHERE instrument = %s AND is_current = TRUE",
            (instrument,),
        )
        count = cur.fetchone()["n"]
        nearest = None
        if count:
            cur.execute(_NEAREST_SQL, (instrument, tested_at, tested_at))
            nearest = cur.fetchone()

    if count == 0:
        return {"releasable": False, "reason_code": REASON_NO_CALIBRATION,
                "calibration": None,
                "reason_message": f"仪器 {instrument} 当前不存在任何校准记录"}

    if nearest["valid_from"] > tested_at:
        msg = (f"检测时间 {tested_at.isoformat()} 早于最近校准生效时间 "
               f"{nearest['valid_from'].isoformat()}（左闭右开区间）")
    else:
        end = nearest["valid_to"]
        msg = (f"检测时间 {tested_at.isoformat()} 晚于最近校准失效时间 "
               f"{end.isoformat() if end else '开放区间'}（左闭右开区间）")
    return {"releasable": False, "reason_code": REASON_OUTSIDE_PERIOD,
            "calibration": None, "nearest_calibration": nearest,
            "reason_message": msg}


# ---- 校准写入 ------------------------------------------------------------------

def create_calibration(conn, payload) -> dict[str, Any]:
    """创建原始校准记录（新版本链的 v1）。仪器锁由调用方经 prelock 预取。"""
    if payload.valid_to is not None and payload.valid_to <= payload.valid_from:
        raise blocked("INVALID_INTERVAL",
                      "有效区间必须满足 valid_from < valid_to（左闭右开）")
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO calibration_chains (instrument) VALUES (%s) RETURNING id",
            (payload.instrument,),
        )
        chain_id = cur.fetchone()["id"]
        try:
            cur.execute(
                """
                INSERT INTO calibration_versions
                    (chain_id, version, instrument, valid_from, valid_to,
                     passed, corrects_id, is_current)
                VALUES (%s, 1, %s, %s, %s, %s, NULL, TRUE)
                RETURNING *
                """,
                (chain_id, payload.instrument, payload.valid_from,
                 payload.valid_to, payload.passed),
            )
        except ExclusionViolation:
            raise conflict("INTERVAL_OVERLAP",
                           "与该仪器另一条当前校准版本的有效区间重叠",
                           instrument=payload.instrument)
        return cur.fetchone()


def instrument_of_calibration(conn, calibration_id: int) -> str | None:
    """只读：按校准版本 ID 解析所属仪器（用于更正前预取仪器锁）。"""
    row = conn.execute(
        "SELECT instrument FROM calibration_versions WHERE id = %s",
        (calibration_id,),
    ).fetchone()
    return row["instrument"] if row else None


def correct_calibration(conn, payload) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """提交更正版本。返回 (新版本, 受影响的已发布报告漂移列表)。"""
    if payload.valid_to is not None and payload.valid_to <= payload.valid_from:
        raise blocked("INVALID_INTERVAL",
                      "有效区间必须满足 valid_from < valid_to（左闭右开）")

    with conn.cursor() as cur:
        # 定位被引用记录（仪器锁已由调用方经 prelock 在事务外预取）
        cur.execute("SELECT * FROM calibration_versions WHERE id = %s",
                    (payload.corrects_id,))
        original = cur.fetchone()
        if original is None:
            raise not_found("校准记录", payload.corrects_id)

        instrument = original["instrument"]

        # 锁版本链：同一链上的并发更正在此串行
        cur.execute("SELECT id FROM calibration_chains WHERE id = %s FOR UPDATE",
                    (original["chain_id"],))

        cur.execute(
            "SELECT * FROM calibration_versions WHERE chain_id = %s AND is_current = TRUE FOR UPDATE",
            (original["chain_id"],),
        )
        current = cur.fetchone()
        # 唯一部分索引保证至多一条；防御性处理
        assert current is not None, "版本链缺少最新版本，数据异常"

        expected_version = current["version"] + 1
        if payload.version != expected_version:
            raise conflict(
                "VERSION_CONFLICT",
                f"版本号冲突：当前最新版本为 v{current['version']}，"
                f"更正必须提交连续版本号 v{expected_version}，收到 v{payload.version}",
                current_version=current["version"],
                expected_version=expected_version,
            )

        # 先下线旧版，再插入新版（避免与自身触发区间约束）
        cur.execute(
            "UPDATE calibration_versions SET is_current = FALSE "
            "WHERE id = %s RETURNING id",
            (current["id"],),
        )
        try:
            cur.execute(
                """
                INSERT INTO calibration_versions
                    (chain_id, version, instrument, valid_from, valid_to,
                     passed, corrects_id, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                RETURNING *
                """,
                (current["chain_id"], payload.version, instrument,
                 payload.valid_from, payload.valid_to, payload.passed,
                 payload.corrects_id),
            )
        except UniqueViolation:
            raise conflict("VERSION_CONFLICT",
                           "版本冲突：该版本号已存在或链上已存在最新版")
        except ExclusionViolation:
            raise conflict("INTERVAL_OVERLAP",
                           "新版本与该仪器另一条当前校准版本的有效区间重叠",
                           instrument=instrument)
        new_row = cur.fetchone()

    drift = _drift_for_instrument(conn, instrument)
    return new_row, drift


# ---- 报告发布 ------------------------------------------------------------------

def _release_one(conn, instrument: str, tested_at: datetime,
                 reference: str | None, *, batch_no: str | None = None,
                 entry_index: int | None = None) -> dict[str, Any]:
    """单份报告的“禁用优先 -> 校准判定 -> 落库”核心，单份提交与整批放行共用。

    仪器咨询锁已由调用方经 prelock 在 SERIALIZABLE 事务之外预取，
    因此本事务快照在取锁之后建立，读取的禁用区间/校准即稳定的判定依据。
    ``batch_no``/``entry_index`` 仅整批放行时提供：前者写入报告行供追溯，
    后者附进错误 detail 以定位批内条目；单份提交的错误结构保持原样。
    """
    extra: dict[str, Any] = {"instrument": instrument,
                             "tested_at": tested_at.isoformat()}
    if batch_no is not None:
        extra["batch_no"] = batch_no
    if entry_index is not None:
        extra["entry_index"] = entry_index

    # 禁用优先：检测时间命中未撤销禁用区间则拒绝放行，即使当前校准通过
    active = find_active_disablement(conn, instrument, tested_at, lock=True)
    if active is not None:
        raise blocked(
            REASON_INSTRUMENT_DISABLED,
            f"检测时间 {tested_at.isoformat()} 命中仪器 {instrument} "
            f"未撤销禁用区间（事件 {active['event_no']}）：{active['reason']}",
            basis_disablement=disablement_brief(active),
            **extra,
        )

    decision = determine(conn, instrument, tested_at, lock=True)
    basis = decision["calibration"]
    snapshot: dict[str, Any] = {
        "released": decision["releasable"],
        "reason_code": decision["reason_code"],
        "instrument": instrument,
        "tested_at": tested_at.isoformat(),
    }
    if basis is not None:
        snapshot["basis_calibration"] = _cal_brief(basis)
    if not decision["releasable"]:
        snapshot["reason_message"] = decision["reason_message"]
        raise blocked(
            decision["reason_code"],
            decision["reason_message"],
            nearest_calibration=(
                _cal_brief(decision["nearest_calibration"])
                if decision.get("nearest_calibration") else None
            ),
            **extra,
        )

    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO reports
                    (reference, instrument, tested_at, released,
                     basis_calibration_id, snapshot, batch_no)
                VALUES (%s, %s, %s, TRUE, %s, %s, %s)
                RETURNING *
                """,
                (reference, instrument, tested_at,
                 basis["id"], json.dumps(snapshot), batch_no),
            )
        except UniqueViolation:
            # 单份提交保持原错误结构（无附加字段）；整批附加条目位置等信息
            raise conflict("REPORT_REFERENCE_EXISTS",
                           f"报告编号已存在: {reference}",
                           **(extra if batch_no is not None else {}))
        report = cur.fetchone()

    return {
        "released": True,
        "reason_code": REASON_PASSED,
        "basis_calibration": _cal_brief(basis),
        "report_id": report["id"],
        "reference": report["reference"],
        "snapshot": snapshot,
    }


def publish_report(conn, payload) -> dict[str, Any]:
    return _release_one(conn, payload.instrument, payload.tested_at,
                        payload.reference)


# ---- 整批放行 ------------------------------------------------------------------

def validate_batch_payload(payload) -> None:
    """批内报告编号冲突预检（不持锁、不入库），取事务/咨询锁之前拒绝。"""
    seen: dict[str, int] = {}
    for index, entry in enumerate(payload.entries):
        if entry.reference is None:
            continue
        first = seen.get(entry.reference)
        if first is not None:
            raise blocked(
                "BATCH_DUPLICATE_REFERENCE",
                f"批内报告编号重复: {entry.reference}（条目 {first} 与条目 {index}）",
                batch_no=payload.batch_no,
                reference=entry.reference,
                entry_indices=[first, index],
            )
        seen[entry.reference] = index


def batch_request_hash(payload) -> str:
    """规范化请求内容哈希：批次号 + 有序条目（仪器 / UTC 检测时间 / 编号）。

    检测时间已经模型层归一化为 UTC，序列化结果稳定：相同内容必得相同哈希，
    用于成功批次的幂等判定（同号同内容重放，同号不同内容拒绝）。
    """
    canonical = {
        "batch_no": payload.batch_no,
        "entries": [
            {
                "instrument": entry.instrument,
                "tested_at": entry.tested_at.isoformat(),
                "reference": entry.reference,
            }
            for entry in payload.entries
        ],
    }
    blob = json.dumps(canonical, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def publish_batch(conn, payload) -> dict[str, Any]:
    """整批放行：同一可串行快照下按输入顺序逐条判定，全部可放行才原子发布。

    批次锁 + 全部涉及仪器锁由调用方经 prelock 在事务外预取，判定读取的是
    取锁后的稳定状态；并发校准更正/禁用登记/撤销/单份发布经仪器锁或
    SERIALIZABLE 谓词依赖与本品互斥或可串行，不会产生半批或过期判定。

    - 幂等：批次号已成功且内容相同 -> 原样返回首次响应；内容不同 -> 409；
    - 任一条目阻断/编号冲突 -> 抛错整体回滚（批次行一并回滚），不占批次号。
    """
    request_hash = batch_request_hash(payload)

    # 批次锁已使同批次号请求串行：此处要么查无此批，要么读到已提交的首次结果
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM report_batches WHERE batch_no = %s",
            (payload.batch_no,),
        )
        existing = cur.fetchone()
    if existing is not None:
        if existing["request_hash"] == request_hash:
            return existing["response"]  # 相同内容重试：原样重放首次结果
        raise conflict(
            "BATCH_CONTENT_MISMATCH",
            f"批次号 {payload.batch_no} 已成功放行，本次请求内容与首次不一致，拒绝",
            batch_no=payload.batch_no,
        )

    # 批内编号冲突防御性复查（路由已在取锁前预检）
    validate_batch_payload(payload)

    # 先占批次行（响应在报告落库后回填）：任一后续失败整体回滚，批次号不被占用
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO report_batches (batch_no, request_hash, response) "
            "VALUES (%s, %s, '{}')",
            (payload.batch_no, request_hash),
        )

    released: list[dict[str, Any]] = []
    for index, entry in enumerate(payload.entries):
        result = _release_one(conn, entry.instrument, entry.tested_at,
                              entry.reference,
                              batch_no=payload.batch_no, entry_index=index)
        released.append({
            "entry_index": index,
            "report_id": result["report_id"],
            "reference": result["reference"],
            "instrument": entry.instrument,
            "tested_at": entry.tested_at.isoformat(),
            "reason_code": result["reason_code"],
            "basis_calibration": result["basis_calibration"],
            "snapshot": result["snapshot"],
        })

    response = {
        "batch_no": payload.batch_no,
        "released": True,
        "count": len(released),
        "reports": released,
    }
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE report_batches SET response = %s WHERE batch_no = %s",
            (json.dumps(response), payload.batch_no),
        )
    return response


# ---- 漂移重算 ------------------------------------------------------------------

def _drift_for_instrument(conn, instrument: str) -> list[dict[str, Any]]:
    return _drift(conn, instrument=instrument)


def compute_drift(conn, instrument: str | None = None) -> list[dict[str, Any]]:
    return _drift(conn, instrument=instrument)


def _drift(conn, instrument: str | None) -> list[dict[str, Any]]:
    """对已发布报告按当前校准重算；仅返回结论与快照不同的报告。

    快照本身绝不改写。判定不加锁（只读重算）。
    """
    with conn.cursor() as cur:
        if instrument:
            cur.execute(
                "SELECT * FROM reports WHERE released = TRUE AND instrument = %s "
                "ORDER BY id",
                (instrument,),
            )
        else:
            cur.execute("SELECT * FROM reports WHERE released = TRUE ORDER BY id")
        reports = cur.fetchall()

    drifted: list[dict[str, Any]] = []
    for report in reports:
        now_decision = determine(conn, report["instrument"], report["tested_at"],
                                 lock=False)
        snap = report["snapshot"]
        snap_basis = (snap or {}).get("basis_calibration") or {}

        if now_decision["releasable"]:
            now_basis = now_decision["calibration"]
            if now_basis["id"] == snap_basis.get("id"):
                continue  # 结论与依据版本均未变化
            change = "NOW_COVERED_BY_OTHER_VERSION" if snap.get("released") else "NOW_RELEASEABLE"
            drifted.append({
                "report_id": report["id"],
                "reference": report["reference"],
                "instrument": report["instrument"],
                "tested_at": report["tested_at"].isoformat(),
                "change": change,
                "snapshot": snap,
                "current_decision": {
                    "released": True,
                    "reason_code": REASON_PASSED,
                    "basis_calibration": _cal_brief(now_basis),
                },
            })
        else:
            drifted.append({
                "report_id": report["id"],
                "reference": report["reference"],
                "instrument": report["instrument"],
                "tested_at": report["tested_at"].isoformat(),
                "change": "NOW_BLOCKED",
                "snapshot": snap,
                "current_decision": {
                    "released": False,
                    "reason_code": now_decision["reason_code"],
                    "reason_message": now_decision["reason_message"],
                    "basis_calibration": (
                        _cal_brief(now_decision["calibration"])
                        if now_decision["calibration"] else None
                    ),
                },
            })
    return drifted
