#!/usr/bin/env bash
# 仪器故障禁用区间端到端示例：
# 校准+放行 -> 登记禁用（区间内拒绝并给依据）-> 重叠/无效/重复事件拒绝
# -> 撤销（区间内恢复放行）-> 追溯禁用与受影响报告查询（快照不改写）。
# 依赖：curl（jq 可选，没有时用 python -m json.tool）
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8000}"
pp() { if command -v jq >/dev/null 2>&1; then jq .; else python3 -m json.tool; fi; }
SUF="${RANDOM}${RANDOM}"
INST="HPLC-DX-${SUF}"

echo "== 1. 登记校准（2026 全年，通过） =="
curl -sS -X POST "$BASE/calibrations" -H 'Content-Type: application/json' -d "{
  \"instrument\": \"$INST\",
  \"valid_from\": \"2026-01-01T00:00:00Z\",
  \"valid_to\":   \"2027-01-01T00:00:00Z\",
  \"passed\": true
}" | pp

echo "== 2. 发布一份检测时间 2026-03-15 的报告（放行） =="
curl -sS -X POST "$BASE/reports" -H 'Content-Type: application/json' -d "{
  \"instrument\": \"$INST\",
  \"tested_at\": \"2026-03-15T08:00:00Z\",
  \"reference\": \"DX-R-0001-$SUF\"
}" | pp

echo "== 3. 登记禁用区间事件 EVT-$SUF-A：[2026-04-01, 2026-05-01)，泵故障 =="
curl -sS -X POST "$BASE/disablements" -H 'Content-Type: application/json' -d "{
  \"event_no\": \"EVT-$SUF-A\",
  \"instrument\": \"$INST\",
  \"disabled_from\": \"2026-04-01T00:00:00Z\",
  \"disabled_to\":   \"2026-05-01T00:00:00Z\",
  \"reason\": \"高压泵故障返厂维修\"
}" | pp

echo "== 4. 区间内（4/15）提交报告：即使校准通过也 422 INSTRUMENT_DISABLED，返回禁用依据 =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports" \
  -H 'Content-Type: application/json' -d "{
    \"instrument\": \"$INST\",
    \"tested_at\": \"2026-04-15T08:00:00Z\",
    \"reference\": \"DX-R-0002-$SUF\"
  }"

echo "== 5. 边界相接区间 [2026-05-01, 2026-06-01) 允许登记 =="
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X POST "$BASE/disablements" \
  -H 'Content-Type: application/json' -d "{
    \"event_no\": \"EVT-$SUF-B\",
    \"instrument\": \"$INST\",
    \"disabled_from\": \"2026-05-01T00:00:00Z\",
    \"disabled_to\":   \"2026-06-01T00:00:00Z\",
    \"reason\": \"复检观察期\"
  }"

echo "== 6. 重叠登记 -> 409 INTERVAL_OVERLAP =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/disablements" \
  -H 'Content-Type: application/json' -d "{
    \"event_no\": \"EVT-$SUF-OVL\",
    \"instrument\": \"$INST\",
    \"disabled_from\": \"2026-04-20T00:00:00Z\",
    \"disabled_to\":   \"2026-05-10T00:00:00Z\",
    \"reason\": \"重叠区间\"
  }"

echo "== 7. 重复事件号 -> 409 DISABLEMENT_EVENT_EXISTS =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/disablements" \
  -H 'Content-Type: application/json' -d "{
    \"event_no\": \"EVT-$SUF-A\",
    \"instrument\": \"$INST\",
    \"disabled_from\": \"2026-09-01T00:00:00Z\",
    \"disabled_to\":   \"2026-10-01T00:00:00Z\",
    \"reason\": \"重复事件号\"
  }"

echo "== 8. 无效区间（终点 <= 起点）-> 422 INVALID_INTERVAL =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/disablements" \
  -H 'Content-Type: application/json' -d "{
    \"event_no\": \"EVT-$SUF-BAD\",
    \"instrument\": \"$INST\",
    \"disabled_from\": \"2026-08-01T00:00:00Z\",
    \"disabled_to\":   \"2026-08-01T00:00:00Z\",
    \"reason\": \"零长度区间\"
  }"

echo "== 9. 按事件号撤销禁用 EVT-$SUF-A =="
curl -sS -X POST "$BASE/disablements/EVT-$SUF-A/revocation" | pp

echo "== 10. 撤销后 4/15 恢复放行（沿用校准判定） =="
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X POST "$BASE/reports" \
  -H 'Content-Type: application/json' -d "{
    \"instrument\": \"$INST\",
    \"tested_at\": \"2026-04-15T08:00:00Z\",
    \"reference\": \"DX-R-0003-$SUF\"
  }"

echo "== 11. 重复撤销 -> 409 DISABLEMENT_ALREADY_REVOKED；未知事件 -> 404 =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/disablements/EVT-$SUF-A/revocation"
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/disablements/NO-SUCH-EVENT/revocation"

echo "== 12. 追溯登记覆盖 3/15 的历史禁用，再查受影响已发布报告及依据 =="
curl -sS -o /dev/null -X POST "$BASE/disablements" -H 'Content-Type: application/json' -d "{
  \"event_no\": \"EVT-$SUF-RETRO\",
  \"instrument\": \"$INST\",
  \"disabled_from\": \"2026-03-01T00:00:00Z\",
  \"disabled_to\":   \"2026-03-20T00:00:00Z\",
  \"reason\": \"事后认定的隐性故障期\"
}"
curl -sS "$BASE/disablements/affected-reports?instrument=$INST" | pp

echo "== 13. 撤销追溯禁用后，受影响报告列表随之清空（原报告与快照不改写） =="
curl -sS -o /dev/null -X POST "$BASE/disablements/EVT-$SUF-RETRO/revocation"
curl -sS "$BASE/disablements/affected-reports?instrument=$INST" | pp

echo "== 14. 禁用区间全量列表（含已撤销记录，供追溯） =="
curl -sS "$BASE/disablements?instrument=$INST" | pp
