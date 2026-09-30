"""仪器故障禁用区间接口。"""
from __future__ import annotations

from fastapi import APIRouter, Query

from ..models import DisablementCreate, serialize_disablement
from ..repository import (
    affected_reports,
    create_disablement,
    list_disablements,
    prelock_event,
    prelock_event_then_instrument,
    revoke_disablement,
    validate_disablement_payload,
)
from ..transactions import run_tx

router = APIRouter(prefix="/disablements", tags=["disablements"])


@router.post("", status_code=201)
def create(payload: DisablementCreate):
    """登记禁用区间：唯一事件号 + 仪器 + [disabled_from, disabled_to) + 原因。

    - 无效区间（终点 <= 起点） -> 422 INVALID_INTERVAL（取锁前即拒绝）
    - 事件号已存在（含已撤销，不可复用） -> 409 DISABLEMENT_EVENT_EXISTS
    - 与同仪器未撤销区间重叠（边界相接允许） -> 409 INTERVAL_OVERLAP
    """
    # 形态校验提前到事务/咨询锁之外，避免无效请求无谓排队持锁
    validate_disablement_payload(payload)
    row = run_tx(
        lambda conn: create_disablement(conn, payload),
        prelock=prelock_event_then_instrument(payload.event_no, payload.instrument),
    )
    return {"disablement": serialize_disablement(row)}


@router.post("/{event_no}/revocation", status_code=200)
def revoke(event_no: str):
    """按事件号撤销禁用；记录保留供追溯。

    - 未知事件 -> 404 NOT_FOUND
    - 已撤销事件再次撤销 -> 409 DISABLEMENT_ALREADY_REVOKED
    """
    row = run_tx(
        lambda conn: revoke_disablement(conn, event_no),
        prelock=prelock_event(event_no),
    )
    return {"disablement": serialize_disablement(row)}


# 静态路径需在 /{event_no} 之前声明；当前未使用单条 GET，列表已含全部记录
@router.get("/affected-reports")
def get_affected_reports(instrument: str | None = Query(None)):
    """按当前禁用状态查询受影响的已发布报告及对应禁用依据。

    实时计算：登记/撤销禁用后结果随之变化；原放行记录与判定快照不改写。
    只读事务。
    """
    items = run_tx(lambda conn: affected_reports(conn, instrument), readonly=True)
    return {"items": items, "count": len(items)}


@router.get("")
def list_all(
    instrument: str | None = Query(None),
    active_only: bool = Query(False, description="仅返回未撤销区间"),
):
    """禁用区间列表（含已撤销记录，供追溯）。"""
    rows = run_tx(lambda conn: list_disablements(conn, instrument, active_only),
                  readonly=True)
    return {"items": [serialize_disablement(r) for r in rows]}
