# 实验室检测报告放行服务

基于 **Python + FastAPI + PostgreSQL** 的 HTTP API：管理仪器校准记录（含有效期、通过/未通过结论、更正版本链）、
仪器**故障禁用区间**（登记/撤销/追溯），并据此对检测报告做放行判定。

## 核心规则

### 校准（原有）

- 校准区间为 **左闭右开** `[valid_from, valid_to)`；`valid_to` 为 `null` 表示开放区间（持续有效）。
- 同一仪器**当前版本**的有效区间**不得重叠**，由 PostgreSQL GiST `EXCLUDE` 约束在数据库层强制。
- 更正必须**引用原记录**（任意历史版本 ID）并提交**连续版本号**（当前最新版 + 1）。
- 旧版本全部保留可查，`is_current=true` 的唯一最新版才参与判定。
- 版本冲突、区间冲突一律拒绝（`409`），错误体含机器可读原因码。

### 仪器故障禁用（本轮新增）

- 仪器校准合格后仍可能因故障被禁用；禁用以**唯一事件号 + 仪器 + 起止时间 + 原因**登记，
  区间为**左闭右开** `[disabled_from, disabled_to)`，`disabled_to` 为 `null` 表示持续禁用。
- 同一仪器**未撤销**禁用区间**不得重叠**（GiST `EXCLUDE` 约束，仅作用于未撤销行），**边界相接允许**。
- 可按事件号**撤销**禁用（`POST /disablements/{event_no}/revocation`）；记录（含撤销时间）**永久保留供追溯**，
  事件号即使撤销也**不可复用**。
- 明确拒绝：未知事件（`404`）、重复撤销（`409 DISABLEMENT_ALREADY_REVOKED`）、
  事件号已存在（`409 DISABLEMENT_EVENT_EXISTS`）、区间重叠（`409 INTERVAL_OVERLAP`）、
  无效区间（`422 INVALID_INTERVAL`）。

### 报告放行

- **禁用优先**：检测时间命中任一**未撤销**禁用区间即**拒绝放行**（`422 INSTRUMENT_DISABLED`），
  错误体的 `basis_disablement` 返回禁用依据（事件号、区间、原因），**即使当前校准通过**。
- 未命中禁用时，沿用原有校准判定：检测时间被一条“结论为通过”的当前校准版本覆盖才放行；
  成功时返回所据校准版本，并保存发布时判定快照（成功响应结构与原格式一致）。
- 校准缺失 / 时间在区间外 / 校准未通过：仍拒绝放行（`422 NO_CALIBRATION /
  OUTSIDE_VALID_PERIOD / CALIBRATION_NOT_PASSED`）。
- 已发布报告的快照**永不改写**；禁用**登记/撤销后受影响查询结果随之变化**，但原放行记录与快照保持原值。
  追溯登记一个覆盖过去的禁用区间，不会撤销此前已放行的报告，只会使其出现在“受影响报告”查询中。

### 整批放行（本轮新增）

- 一次检测委托的多份报告可经 `POST /reports/batches` 按**唯一批次号**整批放行：
  请求含**非空有序**条目，每项给仪器、检测时间和可选报告编号。
- 每个条目**沿用单份提交的全部判定**（禁用优先、当前校准版本、左闭右开区间、阻断原因）；
  整批在**同一可串行快照**上判定，**全部可放行才在同一事务内原子发布**，
  按输入顺序返回各报告的标识、所据校准版本与发布快照。
- 任一失败整批回滚、**不产生任何报告**且**不占批次号**（同号修正后可重提）：
  - 任一条目被阻断 → `422`，`detail` 含 `entry_index` 与该条目的阻断原因/依据；
  - 批内报告编号重复 → `422 BATCH_DUPLICATE_REFERENCE`（含两个条目位置，取锁前拒绝）；
  - 编号与已发布报告冲突 → `409 REPORT_REFERENCE_EXISTS`（含 `entry_index`）。
- 成功批次以批次号**幂等**：相同内容重试返回**首次结果**（`201` 与原响应，不新增报告）；
  同批次号不同内容拒绝（`409 BATCH_CONTENT_MISMATCH`）。
- 批次请求在事务外先取**批次锁 + 全部涉及仪器锁**（仪器锁按字典序，全局锁序与既有
  “事件锁→仪器锁”一致，无锁环），再拍 SERIALIZABLE 快照：并发校准更正、禁用登记/撤销、
  单份发布及同批次重试都**不会产生半批或使用过期状态**。
- 报告行记录 `batch_no`（单份提交为 `null`），列表/单条查询可追溯所属批次。

### 并发与隔离

- 所有写事务运行在 **SERIALIZABLE** 隔离级别并对冲突自动重试；
  校准写、禁用登记/撤销、报告提交在开启事务**之前**先获取**每仪器/每事件会话级咨询锁**
  （64 位 FNV-1a 哈希为键），再拍 SERIALIZABLE 快照——保证判定读取的是“取锁后”的稳定状态，
  **不会在已生效禁用区间内误放行**，并发区间登记也至多一条成功。
- 唯一/部分唯一索引与 GiST 排他约束在数据库层兜底，杜绝“双最新版”、“重叠禁用区间”等脏数据。

## 一键启动

需要 Docker 与 Docker Compose（v2）。

```bash
docker compose up --build
```

启动后：

- API 服务地址与端口：**http://localhost:8000**
- 交互式 API 文档（Swagger UI）：**http://localhost:8000/docs**
- OpenAPI JSON：http://localhost:8000/openapi.json
- 健康检查：http://localhost:8000/health
- PostgreSQL：`localhost:5432`（库/用户/口令均为 `lab`）

数据库（扩展 `btree_gist`、全部表、唯一/排除约束与索引）在 API 首次启动时**幂等自动初始化**，
无需手工执行 SQL；`db` 容器带健康检查，就绪后 API 才启动。

停止并清理：

```bash
docker compose down          # 保留数据卷
docker compose down -v       # 同时删除数据库数据卷
```

## 配置

通过环境变量配置（见 `.env.example`）：

| 变量 | 默认值（容器内） | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://lab:lab@db:5432/lab` | PostgreSQL 连接串 |
| `DB_POOL_MIN_SIZE` | `1` | 连接池最小连接数 |
| `DB_POOL_MAX_SIZE` | `10` | 连接池最大连接数 |
| `DB_CONNECT_TIMEOUT` | `10` | 建连超时（秒） |
| `DB_ACQUIRE_TIMEOUT` | `10` | 从连接池获取连接的超时（秒），超时返回 `503 SERVICE_BUSY` |
| `TX_MAX_RETRIES` | `5` | 串行化/死锁冲突时事务最大重试次数 |
| `TX_RETRY_BACKOFF` | `0.05` | 重试退避基数（秒，指数退避） |

本地不用容器运行时：准备好 PostgreSQL，复制 `.env.example` 为 `.env` 后执行

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## API 一览

### 校准

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/calibrations` | 登记原始校准（版本链 v1） |
| POST | `/calibrations/corrections` | 提交更正版本（引用原记录 + 连续版本号） |
| GET | `/calibrations?instrument=&current_only=` | 列表（旧版保留可查） |
| GET | `/calibrations/{id}` | 单条校准（任意版本） |
| GET | `/calibrations/drift/recompute?instrument=` | 按当前有效性重算，返回与快照结论不同的报告 |

### 禁用区间

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/disablements` | 登记禁用区间（唯一事件号、左闭右开、可开放） |
| POST | `/disablements/{event_no}/revocation` | 按事件号撤销禁用（记录保留） |
| GET | `/disablements?instrument=&active_only=` | 禁用区间列表（默认含已撤销，供追溯） |
| GET | `/disablements/affected-reports?instrument=` | 按当前禁用状态查受影响已发布报告及禁用依据 |

### 报告

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/reports` | 提交报告并判定放行（命中禁用优先阻断） |
| POST | `/reports/batches` | 整批放行：唯一批次号 + 有序条目，同快照判定、原子发布、按批次号幂等 |
| GET | `/reports?instrument=` | 已发布报告列表（含不可变快照与所属批次号） |
| GET | `/reports/{id}` | 单条报告 |
| GET | `/reports/drift/recompute` | 同上，重算漂移 |

## 调用示例

### 1. 登记校准

```bash
curl -sS -X POST http://localhost:8000/calibrations \
  -H 'Content-Type: application/json' \
  -d '{
        "instrument": "HPLC-01",
        "valid_from": "2026-01-01T00:00:00Z",
        "valid_to":   "2026-07-01T00:00:00Z",
        "passed": true
      }'
```

返回：

```json
{
  "calibration": {
    "id": 1, "chain_id": 1, "version": 1, "instrument": "HPLC-01",
    "valid_from": "2026-01-01T00:00:00+00:00",
    "valid_to":   "2026-07-01T00:00:00+00:00",
    "passed": true, "is_current": true, "corrects_id": null,
    "created_at": "2026-09-30T00:00:00+00:00"
  }
}
```

区间重叠的登记会被拒绝：`409 {"detail": {"code": "INTERVAL_OVERLAP", ...}}`。

### 2. 提交报告（放行成功）

```bash
curl -sS -X POST http://localhost:8000/reports \
  -H 'Content-Type: application/json' \
  -d '{
        "instrument": "HPLC-01",
        "tested_at": "2026-03-15T08:00:00Z",
        "reference": "R-2026-0001"
      }'
```

返回（`201`，含所据校准版本与判定快照）：

```json
{
  "released": true,
  "reason_code": "PASSED_CALIBRATION_COVERAGE",
  "basis_calibration": {"id": 1, "chain_id": 1, "version": 1, "passed": true, "...": "..."},
  "report_id": 1,
  "reference": "R-2026-0001",
  "snapshot": {
    "released": true,
    "reason_code": "PASSED_CALIBRATION_COVERAGE",
    "instrument": "HPLC-01",
    "tested_at": "2026-03-15T08:00:00+00:00",
    "basis_calibration": {"id": 1, "chain_id": 1, "version": 1, "passed": true}
  }
}
```

阻断场景（`422`，不落库）的 `detail.code`：

- `INSTRUMENT_DISABLED` —— 检测时间命中未撤销禁用区间（**优先于校准判定**），
  `detail.basis_disablement` 给出事件号/区间/原因；
- `NO_CALIBRATION` —— 该仪器当前没有任何校准；
- `OUTSIDE_VALID_PERIOD` —— 有校准但检测时间不在任何左闭右开区间内（如恰等于 `valid_to`）；
- `CALIBRATION_NOT_PASSED` —— 覆盖该时点的校准结论为“未通过”。

### 2b. 登记禁用、区间内被阻断并返回依据

```bash
curl -sS -X POST http://localhost:8000/disablements \
  -H 'Content-Type: application/json' \
  -d '{
        "event_no": "EVT-2026-0412-01",
        "instrument": "HPLC-01",
        "disabled_from": "2026-04-01T00:00:00Z",
        "disabled_to":   "2026-05-01T00:00:00Z",
        "reason": "高压泵故障返厂维修"
      }'
```

返回（`201`）：

```json
{
  "disablement": {
    "id": 1, "event_no": "EVT-2026-0412-01", "instrument": "HPLC-01",
    "disabled_from": "2026-04-01T00:00:00+00:00",
    "disabled_to":   "2026-05-01T00:00:00+00:00",
    "reason": "高压泵故障返厂维修",
    "revoked": false, "revoked_at": null,
    "created_at": "2026-09-30T00:00:00+00:00"
  }
}
```

此后提交检测时间落在该区间内的报告（校准即使通过）：

```bash
curl -i -X POST http://localhost:8000/reports \
  -H 'Content-Type: application/json' \
  -d '{"instrument":"HPLC-01","tested_at":"2026-04-15T08:00:00Z","reference":"R-2026-0042"}'
# HTTP/1.1 422 Unprocessable Entity
# {"detail":{"code":"INSTRUMENT_DISABLED", ...,
#   "basis_disablement":{"event_no":"EVT-2026-0412-01",
#     "disabled_from":"2026-04-01T00:00:00+00:00",
#     "disabled_to":"2026-05-01T00:00:00+00:00",
#     "reason":"高压泵返厂维修", ...}}}
```

边界：恰为 `disabled_from` 命中（左闭），恰为 `disabled_to` **不**命中（右开）；
`[..,05-01)` 与 `[05-01,..)` 两条未撤销区间可同时存在。

登记被拒的 `detail.code`：

- `422 INVALID_INTERVAL` —— `disabled_to <= disabled_from`；
- `409 DISABLEMENT_EVENT_EXISTS` —— 事件号已存在（含已撤销，不可复用）；
- `409 INTERVAL_OVERLAP` —— 与同仪器另一条未撤销区间重叠（边界相接不报此错）。

### 2c. 撤销禁用与追溯

```bash
curl -X POST http://localhost:8000/disablements/EVT-2026-0412-01/revocation
# 200：revoked=true 且返回 revoked_at；再撤销 -> 409 DISABLEMENT_ALREADY_REVOKED；
# 未知事件号 -> 404 NOT_FOUND。撤销后区间内提交恢复走校准判定。
```

按当前禁用状态查询受影响的**已发布**报告及其禁用依据（实时计算，登记/撤销后结果随之变化；
原放行记录与快照不改写）：

```bash
curl 'http://localhost:8000/disablements/affected-reports?instrument=HPLC-01'
```

```json
{
  "count": 1,
  "items": [{
    "report_id": 12, "reference": "R-2026-0031", "instrument": "HPLC-01",
    "tested_at": "2026-04-10T09:00:00+00:00",
    "release_snapshot": { "released": true, "reason_code": "PASSED_CALIBRATION_COVERAGE", "...": "..." },
    "disablement_basis": { "event_no": "EVT-2026-0412-01", "reason": "高压泵故障返厂维修", "...": "..." }
  }]
}
```

全部禁用记录（含已撤销）可经 `GET /disablements?instrument=HPLC-01` 追溯；
`active_only=true` 仅看未撤销区间。

### 2d. 整批放行（唯一批次号，原子发布）

```bash
curl -sS -X POST http://localhost:8000/reports/batches \
  -H 'Content-Type: application/json' \
  -d '{
        "batch_no": "BATCH-2026-0930-01",
        "entries": [
          {"instrument": "HPLC-01", "tested_at": "2026-03-15T08:00:00Z", "reference": "R-2026-0001"},
          {"instrument": "GC-02",   "tested_at": "2026-03-15T09:30:00Z", "reference": "R-2026-0002"},
          {"instrument": "HPLC-01", "tested_at": "2026-03-16T08:00:00Z"}
        ]
      }'
```

返回（`201`，按输入顺序给出各报告的标识、所据校准版本与发布快照）：

```json
{
  "batch_no": "BATCH-2026-0930-01",
  "released": true,
  "count": 3,
  "reports": [
    {"entry_index": 0, "report_id": 1, "reference": "R-2026-0001",
     "instrument": "HPLC-01", "tested_at": "2026-03-15T08:00:00+00:00",
     "reason_code": "PASSED_CALIBRATION_COVERAGE",
     "basis_calibration": {"id": 1, "chain_id": 1, "version": 1, "...": "..."},
     "snapshot": {"released": true, "...": "..."}}
  ]
}
```

- **幂等**：相同内容重试返回 `201` 与首次完全一致的响应（相同 `report_id`，不新增报告）；
  同批次号不同内容返回 `409 BATCH_CONTENT_MISMATCH`。
- **整批回滚**：任一条目被阻断（如条目 1 命中禁用）返回 `422`，
  `detail` 含 `entry_index`、`basis_disablement` 等该条目的阻断依据，**整批不落库、批次号不被占用**，
  修正后可用同一批次号重提；
  批内编号重复 `422 BATCH_DUPLICATE_REFERENCE`（`entry_indices` 给出两个位置）；
  编号与已发布报告冲突 `409 REPORT_REFERENCE_EXISTS`（`entry_index` 给出位置）。
- 完整演示见 `examples/curl-batches.sh`。

### 3. 更正校准（连续版本号 + 引用原记录）

```bash
curl -sS -X POST http://localhost:8000/calibrations/corrections \
  -H 'Content-Type: application/json' \
  -d '{
        "corrects_id": 1,
        "version": 2,
        "valid_from": "2026-02-01T00:00:00Z",
        "valid_to":   "2026-08-01T00:00:00Z",
        "passed": false
      }'
```

- 版本号必须是当前最新版 + 1，否则 `409 VERSION_CONFLICT`（响应中给出 `current_version`/`expected_version`）；
- 新区间与同仪器其他当前版本重叠时 `409 INTERVAL_OVERLAP`；
- 响应同时含 `affected_report_drift`：因本次更正导致“当前结论与发布快照不同”的报告列表。

更正后再发布同一检测时间：`422 CALIBRATION_NOT_PASSED`；而此前已发布报告的快照保持原值。

### 4. 重算漂移（不改写快照）

```bash
curl -sS 'http://localhost:8000/calibrations/drift/recompute?instrument=HPLC-01'
```

仅返回当前结论（放行/阻断或所据版本）与快照不同的报告，字段含 `change`：

- `NOW_BLOCKED` —— 快照放行，按当前校准已阻断；
- `NOW_COVERED_BY_OTHER_VERSION` —— 仍放行，但所据校准版本已变化；
- `NOW_RELEASEABLE` —— 快照阻断、当前可放行（历史阻断报告原本不落库，属防御性取值）。

### 脚本化完整演示

```bash
bash examples/curl-demo.sh                       # 校准/报告/更正/漂移全流程
bash examples/curl-disablements.sh               # 禁用登记/阻断依据/撤销/追溯/受影响报告
bash examples/curl-batches.sh                    # 整批放行/幂等重试/阻断回滚/编号冲突
BASE_URL=http://localhost:8000 python3 examples/demo.py   # 含并发更正与整批放行的 Python 版
```

### 并发正确性自检脚本（直连数据库）

```bash
DATABASE_URL=postgresql://lab:lab@localhost:5432/lab \
  PYTHONPATH=. python3 tests/verify_concurrency.py    # 校准版本链与发布并发
DATABASE_URL=postgresql://lab:lab@localhost:5432/lab \
  PYTHONPATH=. python3 tests/verify_disablements.py   # 禁用登记/撤销/发布并发与边界
DATABASE_URL=postgresql://lab:lab@localhost:5432/lab \
  PYTHONPATH=. python3 tests/verify_batches.py        # 整批放行幂等/原子性/并发互斥
```

## 并发正确性如何保证

1. **不会产生双最新版**
   - 唯一部分索引 `uq_calibration_one_current ON (chain_id) WHERE is_current`；
   - 更正先锁版本链行（`SELECT … FOR UPDATE`），再“旧版下线 + 新版插入”，同链并发更正天然串行；
   - 版本号非连续（基于过期版本提交）返回 `409 VERSION_CONFLICT`。
2. **当前校准区间 / 未撤销禁用区间都不会重叠**：两条 GiST 排他约束分别强制
   `EXCLUDE (instrument WITH =, tstzrange(..., '[)') WITH &&) WHERE is_current` 与
   `… WHERE NOT revoked`（禁用）；对并发插入同样生效，冲突映射为 `409 INTERVAL_OVERLAP`。
3. **不会在已生效禁用区间内误放行（关键）**
   - 校准写、禁用登记/撤销、发布都在开启 SERIALIZABLE 事务**之前**先取
     **每仪器/每事件会话级咨询锁**（单 bigint 键，64 位 FNV-1a：
     `instrument\x00<id>` / `event\x00<event_no>`），再拍事务快照；
     因此等待锁期间不会提前固定快照，取锁后读到的必为锁持有者已提交的最新状态。
   - 同仪器的“禁用登记 / 撤销 / 校准更正 / 报告提交”经同一把仪器锁串行；
     发布命中区间即返回 `422 INSTRUMENT_DISABLED`，阻断报告不落库。
   - 撤销与发布即使不共持一把锁，也由发布方的仪器锁 + SERIALIZABLE 谓词依赖保证可串行，
     重试耗尽时返回 `409 TRANSACTION_CONFLICT` 而非脏放行。
4. **事件号唯一、撤销可追溯**：`uq_disablement_event_no` 唯一索引（含已撤销行）
   杜绝事件号复用；撤销只置 `revoked=true/revoked_at`，行不删除，随时可追溯；
   撤销后原区间让空，可登记与其重叠的新事件。
5. **快照与受影响查询解耦**：报告发布时的判定快照写入 `reports.snapshot` 后永不更新；
   `/disablements/affected-reports` 用当前未撤销禁用区间与已发布报告实时 JOIN，
   登记/撤销后结果立即变化，且每份报告至多命中一条禁用依据（未撤销区间不重叠）。
6. **整批放行原子且幂等**
   - 批次请求在事务外先取**批次锁**（`batch\x00<batch_no>`）再按字典序取**全部涉及仪器锁**，
     全局锁序“命名锁（event/batch，至多一把）→ 仪器锁（升序）”无锁环；
     同批次号并发重试在批次锁上串行，后到者读到首次已提交结果：内容相同原样重放，
     不同则 `409 BATCH_CONTENT_MISMATCH`。
   - 判定与发布在**同一 SERIALIZABLE 事务**内完成：任一失败整体回滚，**不产生半批**，
     批次行一并回滚故**失败不占批次号**；仪器锁保证并发更正/禁用登记/单份发布
     不会插入批批判定中间，撤销经 SERIALIZABLE 谓词依赖 + 行锁保证可串行。
   - `report_batches.batch_no` 唯一索引兜底幂等键；`reports.reference` 唯一索引兜底
     跨仪器/跨批次的编号冲突（冲突方整批回滚并返回 `409 REPORT_REFERENCE_EXISTS`
     及条目位置）。
