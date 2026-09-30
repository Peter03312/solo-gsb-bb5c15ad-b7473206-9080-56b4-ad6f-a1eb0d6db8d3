"""FastAPI 入口。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .db import close_pool, open_pool
from .routers import calibrations, disablements, reports


@asynccontextmanager
async def lifespan(app: FastAPI):
    open_pool()  # 启动即建池并幂等初始化数据库
    try:
        yield
    finally:
        close_pool()


app = FastAPI(
    title="实验室检测报告放行 API",
    version="1.2.0",
    description=(
        "校准记录版本化管理、仪器故障禁用区间与检测报告放行判定（单份 + 整批）。\n\n"
        "- 校准区间左闭右开 [valid_from, valid_to)，同仪器当前版本区间不得重叠\n"
        "- 更正引用原记录并提交连续版本号，旧版保留可查，仅最新版参与判定\n"
        "- 禁用区间左闭右开 [disabled_from, disabled_to)，同仪器未撤销区间不得"
        "重叠（边界相接允许）；可按唯一事件号撤销，记录保留供追溯\n"
        "- 报告检测时间命中未撤销禁用区间即拒绝放行并返回禁用依据，即使校准通过\n"
        "- 整批放行：唯一批次号 + 非空有序条目，同一可串行快照判定、全部通过才"
        "原子发布；失败不占批次号，成功批次按批次号幂等重放\n"
        "- 所有写事务运行在 SERIALIZABLE 隔离级别并加每仪器/每批次咨询锁，冲突自动重试"
    ),
    lifespan=lifespan,
)

app.include_router(calibrations.router)
app.include_router(disablements.router)
app.include_router(reports.router)


@app.get("/health", tags=["system"])
def health():
    from . import db

    assert db.pool is not None
    with db.pool.connection() as conn:
        version = conn.execute("SELECT version() AS v").fetchone()["v"]
    return {"status": "ok", "database": version}
