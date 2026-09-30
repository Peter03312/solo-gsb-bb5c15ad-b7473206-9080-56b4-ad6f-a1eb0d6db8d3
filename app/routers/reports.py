"""检测报告接口。"""
from __future__ import annotations

from fastapi import APIRouter

from .. import db
from ..errors import not_found
from ..models import BatchReportSubmit, ReportSubmit
from ..repository import compute_drift
from ..repository import (
    prelock_batch,
    prelock_instrument,
    publish_batch,
    publish_report,
    validate_batch_payload,
)
from ..transactions import run_tx

router = APIRouter(prefix="/reports", tags=["reports"])


def _serialize_report(row: dict) -> dict:
    return {
        "id": row["id"],
        "reference": row["reference"],
        "instrument": row["instrument"],
        "tested_at": row["tested_at"].isoformat(),
        "released": row["released"],
        "basis_calibration_id": row["basis_calibration_id"],
        "batch_no": row["batch_no"],
        "snapshot": row["snapshot"],
        "created_at": row["created_at"].isoformat(),
    }


@router.post("", status_code=201)
def submit_report(payload: ReportSubmit):
    """提交检测报告并尝试放行。

    放行条件：检测时间被该仪器一条“结论为通过”的当前校准版本覆盖
    （区间左闭右开，仅最新版参与判定）。

    - 放行：201，返回所据校准版本及不可变判定快照
    - 命中未撤销禁用区间：422 INSTRUMENT_DISABLED（即使校准通过），detail 含禁用依据
    - 缺失校准 / 区间外 / 校准未通过：422，detail 内含阻断原因码与说明
    """
    return run_tx(
        lambda conn: publish_report(conn, payload),
        prelock=prelock_instrument(payload.instrument),
    )


@router.post("/batches", status_code=201)
def submit_batch(payload: BatchReportSubmit):
    """整批放行：一次委托的多份报告按唯一批次号原子发布。

    每个条目沿用单份提交的判定（禁用优先、当前校准版本、左闭右开区间与
    阻断原因），全部条目可放行才在同一事务内发布；按输入顺序返回各报告的
    标识、所据校准版本与发布快照。

    - 任一条目被阻断：422，detail 含 entry_index 与该条目的阻断原因，整批不落库
    - 批内报告编号重复：422 BATCH_DUPLICATE_REFERENCE（取锁前拒绝）
    - 编号与已发布报告冲突：409 REPORT_REFERENCE_EXISTS，detail 含 entry_index
    - 失败整体回滚，不占批次号；成功批次以批次号幂等：相同内容重试返回首次
      结果（201 与原响应），相同批次号不同内容返回 409 BATCH_CONTENT_MISMATCH
    """
    # 批内编号冲突属请求自身矛盾，取锁/开事务前即拒绝
    validate_batch_payload(payload)
    return run_tx(
        lambda conn: publish_batch(conn, payload),
        prelock=prelock_batch(payload.batch_no,
                              [e.instrument for e in payload.entries]),
    )


# 注意：静态路由需在 /{report_id} 之前声明
@router.get("/drift/recompute")
def report_drift(instrument: str | None = None):
    items = run_tx(lambda conn: compute_drift(conn, instrument), readonly=True)
    return {"items": items, "count": len(items)}


@router.get("")
def list_reports(instrument: str | None = None):
    assert db.pool is not None
    with db.pool.connection() as conn:
        if instrument:
            rows = conn.execute(
                "SELECT * FROM reports WHERE instrument = %s ORDER BY id",
                (instrument,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM reports ORDER BY id").fetchall()
    return {"items": [_serialize_report(r) for r in rows]}


@router.get("/{report_id}")
def get_report(report_id: int):
    assert db.pool is not None
    with db.pool.connection() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE id = %s", (report_id,)
        ).fetchone()
    if row is None:
        raise not_found("报告", report_id)
    return {"report": _serialize_report(row)}
