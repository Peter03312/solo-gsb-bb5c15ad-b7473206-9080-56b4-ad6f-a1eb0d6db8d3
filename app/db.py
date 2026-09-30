"""PostgreSQL 连接池与数据库初始化（幂等）。"""
from __future__ import annotations

import time

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import get_settings

pool: ConnectionPool | None = None

DDL = """
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- 校准版本链：一条链对应同一台仪器同一次校准的全部版本（v1 + 历次更正）
CREATE TABLE IF NOT EXISTS calibration_chains (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument   TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS calibration_versions (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id     BIGINT      NOT NULL REFERENCES calibration_chains(id),
    version      INTEGER     NOT NULL CHECK (version >= 1),
    instrument   TEXT        NOT NULL,
    valid_from   TIMESTAMPTZ NOT NULL,
    valid_to     TIMESTAMPTZ,                       -- NULL = 开放区间（持续有效）
    passed       BOOLEAN     NOT NULL,
    corrects_id  BIGINT REFERENCES calibration_versions(id),  -- v1 为 NULL
    is_current   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_calibration_interval CHECK (valid_to IS NULL OR valid_to > valid_from)
);

-- 同一条链同一版本号只能有一条；更正必须使用连续版本号由应用层保证
CREATE UNIQUE INDEX IF NOT EXISTS uq_calibration_chain_version
    ON calibration_versions (chain_id, version);

-- 每条链至多一个最新版 —— 并发更正也不可能产生“双最新版”
CREATE UNIQUE INDEX IF NOT EXISTS uq_calibration_one_current
    ON calibration_versions (chain_id) WHERE is_current;

-- 同仪器所有“最新版”的有效区间 [valid_from, valid_to) 不得重叠；
-- 旧版本 is_current = FALSE，不参与判定与约束。
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'excl_current_instrument_interval'
    ) THEN
        ALTER TABLE calibration_versions
            ADD CONSTRAINT excl_current_instrument_interval
            EXCLUDE USING gist (
                instrument WITH =,
                tstzrange(valid_from, valid_to, '[)') WITH &&
            ) WHERE (is_current);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_calibration_instrument_current
    ON calibration_versions (instrument) WHERE is_current;
CREATE INDEX IF NOT EXISTS ix_calibration_chain ON calibration_versions (chain_id);

-- 已发布报告（阻断的报告不落库），快照发布当时的判定依据
-- created_at 用 clock_timestamp()：取到每仪器咨询锁后实际落库的墙钟时刻，
-- 作为并发“登记/更正/发布”串行顺序的忠实见证（now() 为事务开始时刻会倒置）。
CREATE TABLE IF NOT EXISTS reports (
    id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reference            TEXT,                          -- 业务侧报告编号（可选，唯一）
    instrument           TEXT        NOT NULL,
    tested_at            TIMESTAMPTZ NOT NULL,
    released             BOOLEAN     NOT NULL,
    basis_calibration_id BIGINT REFERENCES calibration_versions(id),
    snapshot             JSONB       NOT NULL,          -- 发布时判定快照（不可变）
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_report_reference
    ON reports (reference) WHERE reference IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_report_instrument_tested
    ON reports (instrument, tested_at);

-- 仪器故障禁用区间：独立于校准；撤销仅置 revoked=TRUE，记录永久保留供追溯
-- created_at 取 clock_timestamp()（语句实际执行时刻，而非事务开始时刻），
-- 以便跨事务按“取锁后真正写入”的墙钟顺序见证串行化。
CREATE TABLE IF NOT EXISTS instrument_disablements (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_no       TEXT        NOT NULL,                 -- 唯一业务事件号
    instrument     TEXT        NOT NULL,
    disabled_from  TIMESTAMPTZ NOT NULL,
    disabled_to    TIMESTAMPTZ,                          -- NULL = 开放区间（持续禁用）
    reason         TEXT        NOT NULL,
    revoked        BOOLEAN     NOT NULL DEFAULT FALSE,
    revoked_at     TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_disablement_interval CHECK (disabled_to IS NULL OR disabled_to > disabled_from)
);

-- 事件号全局唯一：已撤销记录也占用事件号，不允许复用
CREATE UNIQUE INDEX IF NOT EXISTS uq_disablement_event_no
    ON instrument_disablements (event_no);

-- 同仪器“未撤销”禁用区间 [disabled_from, disabled_to) 不得重叠；
-- 左闭右开使边界相接（前区间终点 == 后区间起点）天然允许；已撤销区间不参与约束。
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'excl_active_disablement_interval'
    ) THEN
        ALTER TABLE instrument_disablements
            ADD CONSTRAINT excl_active_disablement_interval
            EXCLUDE USING gist (
                instrument WITH =,
                tstzrange(disabled_from, disabled_to, '[)') WITH &&
            ) WHERE (NOT revoked);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_disablement_instrument_active
    ON instrument_disablements (instrument, disabled_from) WHERE NOT revoked;

-- 幂等迁移：旧库 reports.created_at 默认 now()（事务开始时刻）会令并发跨事务
-- 时序见证倒置；统一为 clock_timestamp()（实际写入时刻）。新库已为该默认值。
ALTER TABLE reports ALTER COLUMN created_at SET DEFAULT clock_timestamp();
"""


def open_pool() -> ConnectionPool:
    global pool
    settings = get_settings()
    # 容器编排中 db 可能尚未就绪：带退避地等待
    deadline = time.monotonic() + 60
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            candidate = ConnectionPool(
                settings.database_url,
                min_size=settings.db_pool_min_size,
                max_size=settings.db_pool_max_size,
                timeout=settings.db_acquire_timeout,
                kwargs={"row_factory": dict_row, "connect_timeout": settings.db_connect_timeout},
                open=True,
            )
            candidate.wait(timeout=10)
            pool = candidate
            init_db()
            return pool
        except Exception as exc:  # pragma: no cover - 启动期容错
            last_error = exc
            time.sleep(1.0)
    raise RuntimeError(f"无法连接数据库: {last_error}")


def close_pool() -> None:
    global pool
    if pool is not None:
        pool.close()
        pool = None


def init_db() -> None:
    """幂等建表/建约束。应用启动时自动执行，无需手工初始化。"""
    assert pool is not None
    with pool.connection() as conn:
        conn.execute(DDL)
