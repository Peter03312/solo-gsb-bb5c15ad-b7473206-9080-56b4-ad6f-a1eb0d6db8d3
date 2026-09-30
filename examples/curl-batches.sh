#!/usr/bin/env bash
# 整批放行端到端示例：
# 两台仪器校准 -> 整批放行（多仪器多项）-> 同内容重试幂等重放 -> 同号异内容拒绝
# -> 禁用区间内任一项阻断（整批不落库、失败不占批次号）-> 修正后同号重发成功
# -> 批内重复/与已发布冲突 -> 批次结果查询。
# 依赖：curl（jq 可选，没有时用 python -m json.tool）
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8000}"
pp() { if command -v jq >/dev/null 2>&1; then jq .; else python3 -m json.tool; fi; }
SUF="${RANDOM}${RANDOM}"
I1="LC-A-${SUF}"
I2="LC-B-${SUF}"
BATCH="BATCH-${SUF}"

echo "== 1. 登记两台仪器的校准（2026 全年，通过） =="
curl -sS -o /dev/null -X POST "$BASE/calibrations" -H 'Content-Type: application/json' -d "{
  \"instrument\": \"$I1\",
  \"valid_from\": \"2026-01-01T00:00:00Z\",
  \"valid_to\":   \"2027-01-01T00:00:00Z\",
  \"passed\": true
}"
curl -sS -o /dev/null -X POST "$BASE/calibrations" -H 'Content-Type: application/json' -d "{
  \"instrument\": \"$I2\",
  \"valid_from\": \"2026-01-01T00:00:00Z\",
  \"valid_to\":   \"2027-01-01T00:00:00Z\",
  \"passed\": true
}"

echo "== 2. 整批放行：批次号 $BATCH，两台仪器各一份报告 -> 201 =="
curl -sS -X POST "$BASE/reports/batches" -H 'Content-Type: application/json' -d "{
  \"batch_no\": \"$BATCH\",
  \"items\": [
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-03-15T08:00:00Z\", \"reference\": \"R1-$SUF\"},
    {\"instrument\": \"$I2\", \"tested_at\": \"2026-03-16T09:00:00Z\", \"reference\": \"R2-$SUF\"}
  ]
}" | pp

echo "== 3. 相同内容重试 -> 200 幂等重放，报告标识与首次完全一致（不重复落库） =="
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d "{
  \"batch_no\": \"$BATCH\",
  \"items\": [
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-03-15T08:00:00Z\", \"reference\": \"R1-$SUF\"},
    {\"instrument\": \"$I2\", \"tested_at\": \"2026-03-16T09:00:00Z\", \"reference\": \"R2-$SUF\"}
  ]
}"

echo "== 4. 同批次号不同内容 -> 409 BATCH_NO_CONTENT_MISMATCH =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d "{
  \"batch_no\": \"$BATCH\",
  \"items\": [
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-03-17T08:00:00Z\", \"reference\": \"R9-$SUF\"}
  ]
}"

echo "== 5. 登记 $I1 的禁用区间 [2026-04-01, 2026-05-01) =="
curl -sS -o /dev/null -X POST "$BASE/disablements" -H 'Content-Type: application/json' -d "{
  \"event_no\": \"EVT-$SUF\",
  \"instrument\": \"$I1\",
  \"disabled_from\": \"2026-04-01T00:00:00Z\",
  \"disabled_to\":   \"2026-05-01T00:00:00Z\",
  \"reason\": \"高压泵故障\"
}"

echo "== 6. 新批次第 2 项命中禁用 -> 422 INSTRUMENT_DISABLED（带 item_index 与禁用依据），整批不落库 =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d "{
  \"batch_no\": \"${BATCH}-2\",
  \"items\": [
    {\"instrument\": \"$I2\", \"tested_at\": \"2026-04-15T08:00:00Z\", \"reference\": \"R3-$SUF\"},
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-04-15T08:00:00Z\", \"reference\": \"R4-$SUF\"}
  ]
}"

echo "== 7. 失败不占批次号：修正第 2 项检测时间后同号重发 -> 201 =="
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d "{
  \"batch_no\": \"${BATCH}-2\",
  \"items\": [
    {\"instrument\": \"$I2\", \"tested_at\": \"2026-04-15T08:00:00Z\", \"reference\": \"R3-$SUF\"},
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-06-15T08:00:00Z\", \"reference\": \"R4-$SUF\"}
  ]
}"

echo "== 8. 批内编号重复 -> 422 BATCH_DUPLICATE_REFERENCE（带两处位置） =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d "{
  \"batch_no\": \"${BATCH}-3\",
  \"items\": [
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-07-15T08:00:00Z\", \"reference\": \"R5-$SUF\"},
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-07-16T08:00:00Z\", \"reference\": \"R5-$SUF\"}
  ]
}"

echo "== 9. 与已发布编号冲突 -> 409 REPORT_REFERENCE_EXISTS（带位置），整批不落库 =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d "{
  \"batch_no\": \"${BATCH}-4\",
  \"items\": [
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-08-15T08:00:00Z\", \"reference\": \"R6-$SUF\"},
    {\"instrument\": \"$I1\", \"tested_at\": \"2026-08-16T08:00:00Z\", \"reference\": \"R1-$SUF\"}
  ]
}"

echo "== 10. 按批次号查询首次发布结果 =="
curl -sS "$BASE/reports/batches/$BATCH" | pp
