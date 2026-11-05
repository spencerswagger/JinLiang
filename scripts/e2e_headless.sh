#!/usr/bin/env bash
# 验收 ④：不经浏览器，纯 curl 走完整条链。
#
#   serve → 建连接（测连返回版本）→ 枚举库表 → 建任务 → 预检 → run（桩）
#         → 读 SSE 看到阶段变化 → export 出 yaml → 重启进程 → 数据还在
#
# 需要：一个能连的 PostgreSQL（JINLIANG_TEST_PG_DSN）与 MySQL（JINLIANG_TEST_MYSQL_DSN）。
# 依赖：curl、python3（只用标准库，不装任何包）。
#
# 用法：scripts/e2e_headless.sh [二进制路径]
set -euo pipefail

BIN="${1:-./dist/jinliang}"
PORT="${E2E_PORT:-18080}"
BASE="http://127.0.0.1:${PORT}"
WORK="$(mktemp -d)"
DATA="${WORK}/data"
TOKEN=""
SERVER_PID=""

if [ ! -x "${BIN}" ]; then
  echo "找不到可执行文件 ${BIN}（先 go build -o ${BIN} ./cmd/jinliang）" >&2
  exit 2
fi
: "${JINLIANG_TEST_PG_DSN:?需要 JINLIANG_TEST_PG_DSN}"
: "${JINLIANG_TEST_MYSQL_DSN:?需要 JINLIANG_TEST_MYSQL_DSN}"

cleanup() {
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${WORK}"
}
trap cleanup EXIT

say() { printf '\n=== %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

# json <表达式>：从 stdin 的 JSON 里取值（只依赖 python3 标准库）
json() { python3 -c 'import json,sys;d=json.load(sys.stdin);print(eval("d"+sys.argv[1]))' "$1"; }

start_server() {
  "${BIN}" serve --listen "127.0.0.1:${PORT}" --data-dir "${DATA}" >"${WORK}/serve.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    if curl -fsS "${BASE}/api/health" >/dev/null 2>&1; then return 0; fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "--- serve.log ---" >&2; cat "${WORK}/serve.log" >&2
      fail "serve 起不来"
    fi
    sleep 0.1
  done
  fail "serve 10 秒内没有响应 /api/health"
}

api() { # api <method> <path> [json-body]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "${body}" ]; then
    curl -fsS -X "${method}" "${BASE}${path}" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      --data-binary "${body}"
  else
    curl -fsS -X "${method}" "${BASE}${path}" -H "Authorization: Bearer ${TOKEN}"
  fi
}

say "0a. 准备测试表（只建 e2e_* 这两张，方便对着空库跑）"
if [ "${E2E_FIXTURE:-1}" = "1" ]; then
  go run ./test/fixture >"${WORK}/fixture.log" 2>&1 || { cat "${WORK}/fixture.log" >&2; fail "fixture 失败"; }
  cat "${WORK}/fixture.log"
else
  echo "  跳过（E2E_FIXTURE=0）"
fi

say "0b. 首次启动：还没有管理员口令时应该明确说「先设口令」"
start_server
code=$(curl -s -o "${WORK}/pre.json" -w '%{http_code}' "${BASE}/api/connections")
[ "${code}" = "428" ] || fail "首启时 /api/connections 应返回 428，实际 ${code}"
grep -q "管理员口令" "${WORK}/pre.json" || fail "首启提示里应该提到管理员口令"

say "1. 建管理员（口令只存 argon2id 哈希）"
TOKEN=$(curl -fsS -X POST "${BASE}/api/setup" -H 'Content-Type: application/json' \
  --data-binary '{"password":"e2e-night-shift-password"}' | json '["token"]')
[ -n "${TOKEN}" ] || fail "setup 没有返回 token"

say "2. 建连接（一条 dsn 就够）并真的测连"
PG_JSON=$(python3 -c 'import json,os,sys;print(json.dumps({"name":"e2e-pg","dsn":os.environ["JINLIANG_TEST_PG_DSN"]}))')
api POST /api/connections "${PG_JSON}" >"${WORK}/conn.json"
CONN_ID=$(json '["connection"]["id"]' <"${WORK}/conn.json")
PG_VERSION=$(json '["test"]["server_version"]' <"${WORK}/conn.json")
[ -n "${PG_VERSION}" ] || fail "测连没有返回服务端版本"
echo "  PostgreSQL ${PG_VERSION}，连接 ${CONN_ID}"

MY_JSON=$(python3 -c 'import json,os;print(json.dumps({"name":"e2e-mysql","dsn":os.environ["JINLIANG_TEST_MYSQL_DSN"]}))')
api POST /api/connections "${MY_JSON}" >"${WORK}/conn2.json"
MY_ID=$(json '["connection"]["id"]' <"${WORK}/conn2.json")
MY_VERSION=$(json '["test"]["server_version"]' <"${WORK}/conn2.json")
echo "  MySQL ${MY_VERSION}，连接 ${MY_ID}"

say "3. 枚举库表（catalog）"
api GET "/api/catalog?connection_id=${CONN_ID}&schema=public" >"${WORK}/cat.json"
TABLE=$(python3 - "${WORK}/cat.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
tables=d.get("tables") or []
if not tables:
    print(""); raise SystemExit
# 选一张有主键的表，预检更干净
for t in tables:
    if t.get("primary_key"):
        print(t["table"]); raise SystemExit
print(tables[0]["table"])
PY
)
[ -n "${TABLE}" ] || fail "catalog 没有返回任何表（测试库需要至少一张表）"
echo "  选中表：${TABLE}"

say "4. 建任务"
JOB_BODY=$(python3 - "${CONN_ID}" "${MY_ID}" "${TABLE}" <<'PY'
import json,sys
conn, my, table = sys.argv[1], sys.argv[2], sys.argv[3]
print(json.dumps({
  "name": "e2e-orders",
  "source_id": conn,
  "target_id": my,
  "enabled": False,
  "spec": {
    "source": {"scope": {"schema": "public", "include": [table]}},
    "target": {"schema": "report", "on_exists": "append", "create_indexes": "defer"},
    "mode": {"frequency": "continuous+initial"},
    "content": {"tables": {table: {}}}
  }
}))
PY
)
api POST /api/jobs "${JOB_BODY}" >"${WORK}/job.json"
JOB_ID=$(json '["id"]' <"${WORK}/job.json")
JOB_KEY=$(json '["job_key"]' <"${WORK}/job.json")
echo "  任务 ${JOB_ID}，指纹 ${JOB_KEY}"

say "5. 预检（四态报告）"
PRE_CODE=$(curl -s -o "${WORK}/preflight.json" -w '%{http_code}' -X POST \
  "${BASE}/api/jobs/${JOB_ID}/preflight" -H "Authorization: Bearer ${TOKEN}")
[ "${PRE_CODE}" = "200" ] || fail "preflight 返回 ${PRE_CODE}"
python3 - "${WORK}/preflight.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
checks=d.get("checks") or []
assert checks, "预检必须至少给出一项检查"
for c in checks:
    print("  [%s] %-10s %s" % (c["status"], c["id"], c["title"]))
print("  can_start=%s" % d.get("can_start"))
PY

say "5b. 预检要么放行，要么给出可执行的下一步（不假装能跑）"
CAN_START=$(json '["can_start"]' <"${WORK}/preflight.json")
if [ "${CAN_START}" = "True" ]; then
  echo "  can_start=True：源库与目标端都满足 continuous+initial"
else
  echo "  can_start=False：预检拦住了。按设计这不等于拒绝——它必须同时给出下一步："
  python3 - "${WORK}/preflight.json" <<'PYCHECK'
import json, sys
d = json.load(open(sys.argv[1]))
blocked = [c for c in d["checks"] if c["status"] == "blocked"]
assert blocked, "can_start=False 时必须至少有一项 blocked 说明原因"
for c in blocked:
    assert c.get("code"), "blocked 项必须挂错误码（UI 靠它渲染四段式）: %r" % c
    print("  卡点 [%s] %s -> %s" % (c["code"], c["title"], c["detail"]))
    if c.get("sql"):
        print("  可复制执行：%s" % c["sql"])
PYCHECK
  # 按预检给出的替代方案改成定时增量，再预检一次：应该就能开跑了
  INTERVAL_BODY=$(python3 - "${CONN_ID}" "${MY_ID}" "${TABLE}" <<'PYBODY'
import json, sys
conn, my, table = sys.argv[1], sys.argv[2], sys.argv[3]
print(json.dumps({
    "name": "e2e-orders",
    "source_id": conn,
    "target_id": my,
    "enabled": False,
    "spec": {
        "source": {"scope": {"schema": "public", "include": [table]}},
        "target": {"schema": "report", "on_exists": "append", "create_indexes": "defer"},
        "mode": {"frequency": "interval", "interval": "5m", "incremental_column": "updated_at"},
        "content": {"tables": {table: {}}},
    },
}))
PYBODY
)
  api PUT "/api/jobs/${JOB_ID}" "${INTERVAL_BODY}" >/dev/null
  curl -fsS -X POST "${BASE}/api/jobs/${JOB_ID}/preflight" -H "Authorization: Bearer ${TOKEN}" \
    >"${WORK}/preflight2.json"
  json '["can_start"]' <"${WORK}/preflight2.json" | grep -qx 'True' \
    || fail "按预检建议改成定时增量后仍然跑不了"
  echo "  改成定时增量（watermark=updated_at）后 can_start=True：替代方案真的能走通"
fi

say "6. run（桩：只推进阶段，不搬数据）"
api POST "/api/jobs/${JOB_ID}/run" >"${WORK}/run.json"
json '["stub"]' <"${WORK}/run.json" | grep -qi true || fail "run 必须明确返回 stub=true"

say "7. 读 SSE，看阶段变化（不经浏览器）"
curl -sN --max-time 15 "${BASE}/api/jobs/${JOB_ID}/events?token=${TOKEN}" >"${WORK}/sse.txt" || true
DONE=$(grep -c '"type":"done"' "${WORK}/sse.txt" || true)
[ "${DONE}" -ge 1 ] || { cat "${WORK}/sse.txt"; fail "SSE 里没有看到 done 事件"; }
for phase in preflight prepare snapshot catchup streaming draining done; do
  grep -q "\"phase\":\"${phase}\"" "${WORK}/sse.txt" || fail "SSE 里缺少阶段 ${phase}"
done
echo "  SSE 事件行数：$(wc -l <"${WORK}/sse.txt")，阶段全部出现"

say "8. 运行历史落库"
api GET "/api/runs?job_id=${JOB_ID}" >"${WORK}/runs.json"
json '["count"]' <"${WORK}/runs.json" | grep -qx '1' || fail "应该有 1 条运行记录"

say "9. export 出 yaml（不能含任何密码）"
curl -fsS "${BASE}/api/jobs/${JOB_ID}/yaml" -H "Authorization: Bearer ${TOKEN}" >"${WORK}/job.yaml"
grep -qE 'password|:pw|jinliang_test' "${WORK}/job.yaml" && fail "导出的 yaml 里出现了密码或明文 DSN"
grep -q 'env:JINLIANG_CONN_' "${WORK}/job.yaml" || fail "导出的 yaml 应该用 env:JINLIANG_CONN_<id> 占位"

say "10. 重启进程：连接与任务必须还在（控制库落盘）"
kill "${SERVER_PID}"; wait "${SERVER_PID}" 2>/dev/null || true; SERVER_PID=""
start_server
TOKEN=$(curl -fsS -X POST "${BASE}/api/login" -H 'Content-Type: application/json' \
  --data-binary '{"password":"e2e-night-shift-password"}' | json '["token"]')
api GET /api/connections >"${WORK}/conns2.json"
json '["count"]' <"${WORK}/conns2.json" | grep -qx '2' || fail "重启后连接数不对"
api GET "/api/jobs/${JOB_ID}" >"${WORK}/job2.json"
json '["job"]["job_key"]' <"${WORK}/job2.json" | grep -qx "${JOB_KEY}" || fail "重启后任务指纹变了"

say "11. 纯 CLI：doctor / export / import 往返"
"${BIN}" doctor --data-dir "${DATA}" >/dev/null || fail "doctor 报告不健康"
"${BIN}" export --data-dir "${DATA}" --out "${WORK}/cli-export.yaml" || fail "CLI export 失败"
"${BIN}" import --data-dir "${DATA}" --file "${WORK}/cli-export.yaml" || fail "CLI import 往返失败"

say "12. 导出的 yaml 过 schema 校验（验收 ③）"
if python3 -c 'import jsonschema, yaml' 2>/dev/null; then
  python3 scripts/validate_schema.py "${WORK}/job.yaml" "${WORK}/cli-export.yaml"
else
  # 离线环境装不上 jsonschema：用只依赖标准库的同一份 schema 校验器兜底
  echo "  （本机没有 jsonschema，改用 stdlib 校验器）"
  python3 scripts/validate_schema_lite.py "${WORK}/job.yaml" "${WORK}/cli-export.yaml"
fi

say "PASS：headless 全链路通过（连接 → 库表 → 任务 → 预检 → 桩运行 → SSE → 导出 → 重启 → CLI）"
