"""事务封装：SERIALIZABLE 隔离 + 串行化失败/死锁自动重试。

关键：每仪器/每事件的咨询锁在 SERIALIZABLE 事务 **之外** 先获取（autocommit
会话锁），再把同一连接切到 SERIALIZABLE 开事务拍快照。否则 psycopg 会在执行
函数体前先发出 BEGIN，导致 SERIALIZABLE 快照在“等待锁期间”就固定，拿锁后读到
锁持有者提交前的旧状态（典型后果：禁用区间内误放行）。

会话级咨询锁在事务提交/回滚后仍由该后端连接持有，因此可在同一条连接上完成
“取锁 -> 开 SERIALIZABLE 事务 -> 提交”，每个并发操作只占用一个池连接。
"""
from __future__ import annotations

import time
from typing import Callable, TypeVar

from psycopg import OperationalError
from psycopg import IsolationLevel
from psycopg.errors import DeadlockDetected, SerializationFailure
from psycopg_pool import PoolTimeout

from . import db
from .config import get_settings

T = TypeVar("T")

# 40001 = serialization_failure；40P01 = deadlock_detected
_RETRYABLE = (SerializationFailure, DeadlockDetected)


def run_tx(fn: Callable, *, readonly: bool = False, retries: int | None = None,
           prelock: Callable[[object], None] | None = None) -> T:
    """在 SERIALIZABLE 事务中执行 ``fn(conn)``；冲突时整体重试。

    ``prelock(conn)`` 在 SERIALIZABLE 事务开启前、连接处于 autocommit 时执行，
    通常为 ``SELECT pg_advisory_lock(...)``。该会话锁跨越内部事务重试，
    在最终返回/异常前释放。

    ``fn`` 内部抛出的 HTTPException 会原样透传（不重试）。
    数据库层约束冲突同样透传，由各路由映射为 409。
    """
    settings = get_settings()
    attempts = settings.tx_max_retries if retries is None else retries
    assert db.pool is not None
    pool = db.pool  # 入口捕获，避免并发关闭时模块属性变为 None

    last_error: Exception | None = None
    conn = None
    try:
        try:
            conn = pool.getconn()
        except PoolTimeout:
            from fastapi import HTTPException

            raise HTTPException(status_code=503, detail={
                "code": "SERVICE_BUSY",
                "message": "数据库连接池繁忙，请稍后重试",
            })

        # 1) autocommit 下先取会话锁（等待期间不存在事务快照）
        if prelock is not None:
            conn.autocommit = True
            prelock(conn)

        # 2) 同一连接切到 SERIALIZABLE 拍快照、执行业务
        for attempt in range(attempts + 1):
            try:
                conn.autocommit = False
                conn.read_only = readonly
                conn.isolation_level = IsolationLevel.SERIALIZABLE
                try:
                    with conn.transaction():
                        result = fn(conn)
                    return result
                finally:
                    # 恢复连接，供可能的重试复用；会话咨询锁仍持有
                    conn.read_only = False
                    conn.isolation_level = None
            except _RETRYABLE as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                # 确保新的一轮以干净状态开始（回滚由 with 已完成）
                time.sleep(settings.tx_retry_backoff * (2**attempt))
            except OperationalError as exc:
                # 连接级故障也做一次有限重试
                last_error = exc
                if attempt >= attempts:
                    break
                time.sleep(settings.tx_retry_backoff * (2**attempt))
    finally:
        if conn is not None:
            try:
                if prelock is not None:
                    # 会话锁在事务结束后仍持有，统一释放（解锁在 autocommit 下）
                    conn.autocommit = True
                    conn.execute("SELECT pg_advisory_unlock_all()")
                    conn.autocommit = False
            except Exception:
                pass
            pool.putconn(conn)

    from fastapi import HTTPException

    raise HTTPException(status_code=409, detail={
        "code": "TRANSACTION_CONFLICT",
        "message": "并发事务冲突，重试后仍失败，请稍后重试",
        "database_error": str(last_error),
    })
