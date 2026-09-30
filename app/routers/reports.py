"""检测报告接口。"""
from __future__ import annotations

from fastapi import APIRouter, Response

from .. import db
from ..errors import not_found
from ..models import ReportBatchSubmit, ReportSubmit
from ..repository import compute_drift, get_batch, prelock_batch, publish_batch
from ..repository import prelock_instrument, publish_report, validate_batch_payload
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


# 注意：静态路由需在 /{report_id} 之前声明
@router.post("/batches", status_code=201)
def submit_batch(payload: ReportBatchSubmit, response: Response):
    """整批放行：唯一批次号 + 非空有序条目，全部可放行才原子发布。

    每项沿用单份判定（禁用优先、当前校准版本、左闭右开区间），整批基于同一
    可串行状态判定；按输入顺序返回报告标识、所据校准版本与发布快照。

    - 全部放行：201（首次）/ 200（同内容幂等重放，返回首次结果）
    - 任一项阻断：422，detail 含 item_index 与阻断原因（同单份原因码），整批不落库
    - 批内编号重复：422 BATCH_DUPLICATE_REFERENCE；与已发布记录冲突：409 REPORT_REFERENCE_EXISTS
    - 批次号已用于不同内容：409 BATCH_NO_CONTENT_MISMATCH；失败批次不占批次号
    """
    # 批内编号重复预检提前到事务/咨询锁之外，避免无效请求无谓排队持锁
    validate_batch_payload(payload)
    body, replayed = run_tx(
        lambda conn: publish_batch(conn, payload),
        prelock=prelock_batch(payload.batch_no,
                              [item.instrument for item in payload.items]),
    )
    if replayed:
        response.status_code = 200
    return body


@router.get("/batches/{batch_no}")
def get_batch_result(batch_no: str):
    """按批次号查询成功批次的首次发布结果（未知批次 404）。"""
    row = run_tx(lambda conn: get_batch(conn, batch_no), readonly=True)
    if row is None:
        raise not_found("报告批次", batch_no)
    return row["response"]


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
