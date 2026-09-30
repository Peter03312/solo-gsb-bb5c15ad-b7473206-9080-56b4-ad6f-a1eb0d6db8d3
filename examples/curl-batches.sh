#!/usr/bin/env bash
# 整批放行端到端示例：多台仪器多份报告按唯一批次号原子发布
# 演示：整批放行 -> 幂等重试 -> 同号不同内容拒绝 -> 条目阻断整批回滚 -> 失败不占批次号
# 依赖：curl（jq 可选，没有时用 python -m json.tool）
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8000}"
pp() { if command -v jq >/dev/null 2>&1; then jq .; else python3 -m json.tool; fi; }

echo "== 0. 准备：两台仪器各登记一条校准（通过） =="
curl -sS -X POST "$BASE/calibrations" -H 'Content-Type: application/json' -d '{
  "instrument": "HPLC-01",
  "valid_from": "2026-01-01T00:00:00Z",
  "valid_to":   "2027-01-01T00:00:00Z",
  "passed": true
}' -o /dev/null -w 'HPLC-01 校准 -> HTTP %{http_code}\n'
curl -sS -X POST "$BASE/calibrations" -H 'Content-Type: application/json' -d '{
  "instrument": "GC-02",
  "valid_from": "2026-01-01T00:00:00Z",
  "valid_to":   null,
  "passed": true
}' -o /dev/null -w 'GC-02 校准 -> HTTP %{http_code}\n'

echo "== 1. 整批放行：一次委托两台仪器三份报告，批次号 BATCH-2026-0930-01 =="
curl -sS -X POST "$BASE/reports/batches" -H 'Content-Type: application/json' -d '{
  "batch_no": "BATCH-2026-0930-01",
  "entries": [
    {"instrument": "HPLC-01", "tested_at": "2026-03-15T08:00:00Z", "reference": "R-2026-0001"},
    {"instrument": "GC-02",   "tested_at": "2026-03-15T09:30:00Z", "reference": "R-2026-0002"},
    {"instrument": "HPLC-01", "tested_at": "2026-03-16T08:00:00Z"}
  ]
}' | pp

echo "== 2. 幂等：相同内容重试 -> 201 原样返回首次结果（相同 report_id，不新增报告） =="
curl -sS -X POST "$BASE/reports/batches" -H 'Content-Type: application/json' -d '{
  "batch_no": "BATCH-2026-0930-01",
  "entries": [
    {"instrument": "HPLC-01", "tested_at": "2026-03-15T08:00:00Z", "reference": "R-2026-0001"},
    {"instrument": "GC-02",   "tested_at": "2026-03-15T09:30:00Z", "reference": "R-2026-0002"},
    {"instrument": "HPLC-01", "tested_at": "2026-03-16T08:00:00Z"}
  ]
}' -o /dev/null -w 'HTTP %{http_code}（与首次一致）\n'

echo "== 3. 同批次号不同内容 -> 409 BATCH_CONTENT_MISMATCH =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d '{
    "batch_no": "BATCH-2026-0930-01",
    "entries": [{"instrument": "HPLC-01", "tested_at": "2026-03-17T08:00:00Z"}]
  }' || true

echo "== 4. 任一条目被阻断 -> 422 带 entry_index，整批不产生报告 =="
curl -sS -X POST "$BASE/disablements" -H 'Content-Type: application/json' -d '{
  "event_no": "EVT-2026-0412-01", "instrument": "GC-02",
  "disabled_from": "2026-04-01T00:00:00Z", "disabled_to": "2026-05-01T00:00:00Z",
  "reason": "检测器故障"
}' -o /dev/null -w '（先登记一条 GC-02 的禁用区间 -> HTTP %{http_code}）\n'
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d '{
    "batch_no": "BATCH-2026-0930-02",
    "entries": [
      {"instrument": "HPLC-01", "tested_at": "2026-03-20T08:00:00Z", "reference": "R-2026-0010"},
      {"instrument": "GC-02",   "tested_at": "2026-04-15T08:00:00Z", "reference": "R-2026-0011"}
    ]
  }' || true

echo "== 5. 失败不占批次号：同号修正条目后整批成功 =="
curl -sS -X POST "$BASE/reports/batches" -H 'Content-Type: application/json' -d '{
  "batch_no": "BATCH-2026-0930-02",
  "entries": [
    {"instrument": "HPLC-01", "tested_at": "2026-03-20T08:00:00Z", "reference": "R-2026-0010"},
    {"instrument": "GC-02",   "tested_at": "2026-03-15T10:00:00Z", "reference": "R-2026-0011"}
  ]
}' | pp

echo "== 6. 批内编号重复 -> 422 BATCH_DUPLICATE_REFERENCE（带两个条目位置） =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d '{
    "batch_no": "BATCH-2026-0930-03",
    "entries": [
      {"instrument": "HPLC-01", "tested_at": "2026-03-20T08:00:00Z", "reference": "DUP-1"},
      {"instrument": "HPLC-01", "tested_at": "2026-03-21T08:00:00Z", "reference": "DUP-1"}
    ]
  }' || true

echo "== 7. 编号与已发布报告冲突 -> 409 REPORT_REFERENCE_EXISTS（带条目位置） =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/batches" \
  -H 'Content-Type: application/json' -d '{
    "batch_no": "BATCH-2026-0930-04",
    "entries": [
      {"instrument": "HPLC-01", "tested_at": "2026-03-22T08:00:00Z", "reference": "R-2026-0099"},
      {"instrument": "HPLC-01", "tested_at": "2026-03-23T08:00:00Z", "reference": "R-2026-0001"}
    ]
  }' || true

echo "== 8. 报告列表可追溯批次号（batch_no 字段） =="
curl -sS "$BASE/reports?instrument=HPLC-01" | pp
