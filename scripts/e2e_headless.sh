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
RAW_BIN="${BIN}"

if [ ! -x "${BIN}" ]; then
  echo "找不到可执行文件 ${BIN}（先 go build -o ${BIN} ./cmd/jinliang）" >&2
  exit 2
fi

# 四个库的 DSN 一律从环境读（见 /.keys/env.sh），脚本里没有一个字面量连接串。
# 缺哪个就显式打 SKIP —— 静默通过是这里最不能犯的错。
PG_TEST_DSN="${JINLIANG_TEST_PG_DSN:-}"
PG_SRC_DSN="${JINLIANG_TEST_PG_SRC_DSN:-}"
MY_TEST_DSN="${JINLIANG_TEST_MYSQL_DSN:-}"
MY_SRC_DSN="${JINLIANG_TEST_MYSQL_SRC_DSN:-}"

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

say "0. 环境自检：四库 DSN 逐条点名，缺的显式 SKIP"
redact() { printf '%s' "$1" | sed -E 's#(://[^:]*:)[^@]*@#\1***@#'; }
dsn_state() { # dsn_state <变量名> <值>
  if [ -n "$2" ]; then printf '  %-30s OK    %s\n' "$1" "$(redact "$2")"
  else printf '  %-30s SKIP  未设置（该库不参与本次验收）\n' "$1"; fi
}
dsn_state JINLIANG_TEST_PG_DSN     "${PG_TEST_DSN}"
dsn_state JINLIANG_TEST_PG_SRC_DSN "${PG_SRC_DSN}"
dsn_state JINLIANG_TEST_MYSQL_DSN  "${MY_TEST_DSN}"
dsn_state JINLIANG_TEST_MYSQL_SRC_DSN "${MY_SRC_DSN}"

# 注意：set -e 下函数最后一个命令返回 1 会让赋值直接退出脚本，所以必须显式 return 0。
pick_pg() {
  if [ -n "${PG_TEST_DSN}" ]; then printf '%s' "${PG_TEST_DSN}"
  elif [ -n "${PG_SRC_DSN}" ]; then printf '%s' "${PG_SRC_DSN}"
  fi
  return 0
}
pick_my() {
  if [ -n "${MY_TEST_DSN}" ]; then printf '%s' "${MY_TEST_DSN}"
  elif [ -n "${MY_SRC_DSN}" ]; then printf '%s' "${MY_SRC_DSN}"
  fi
  return 0
}
PG_DSN="$(pick_pg)"
MY_DSN="$(pick_my)"

# 源端优先 PostgreSQL（有 timestamptz 增量列），目标端优先 MySQL；
# 只有一种引擎时同引擎自环（目标用另一个 schema/database），仍算「一个库跑通完整链」。
if [ -n "${PG_DSN}" ]; then SRC_DSN="${PG_DSN}"; SRC_LABEL=postgres
elif [ -n "${MY_DSN}" ]; then SRC_DSN="${MY_DSN}"; SRC_LABEL=mysql
else
  # 退出码 78 = EX_CONFIG（sysexits.h）：一个 DSN 都没给，说明环境没准备好。
  # 这里绝不能 exit 0——CI 会把「什么都没验」当成「全部通过」，那是假绿。
  echo "  SKIP：四个 DSN 全空，完整链无法运行（这不是通过，退出码 78 = EX_CONFIG）" >&2
  exit 78
fi
if [ "${SRC_LABEL}" = "postgres" ] && [ -n "${MY_DSN}" ]; then TGT_DSN="${MY_DSN}"; TGT_LABEL=mysql
elif [ "${SRC_LABEL}" = "mysql" ] && [ -n "${PG_DSN}" ]; then TGT_DSN="${PG_DSN}"; TGT_LABEL=postgres
else TGT_DSN="${SRC_DSN}"; TGT_LABEL="${SRC_LABEL}"; echo "  只有 ${SRC_LABEL} 一种引擎：目标端回落到同一实例（换 schema）"
fi
echo "  本次链路：源 ${SRC_LABEL} → 目标 ${TGT_LABEL}"

say "0a. 准备测试表（只建 e2e_* 这两张，方便对着空库跑）"
# fixture 自己按 DSN 环境变量决定建哪些库：空库也能自举，缺的库它不碰。
# fixture 需要 go 工具链；没有 go 时只在已有表的情况下继续，否则 SKIP。
if [ "${E2E_FIXTURE:-1}" = "1" ]; then
  if command -v go >/dev/null 2>&1; then
    if ! go run ./test/fixture >"${WORK}/fixture.log" 2>&1; then
      cat "${WORK}/fixture.log" >&2
      fail "fixture 失败（见上面报错；空库也建不出来说明 DSN 或权限有问题）"
    fi
    cat "${WORK}/fixture.log"
    grep -q "ok" "${WORK}/fixture.log" || echo "  SKIP：没有给 *_TEST_DSN，fixture 未建任何表（假定表已存在）"
  else
    echo "  SKIP：本机没有 go 工具链，跳过 fixture（假定 e2e_* 表已存在）"
  fi
else
  echo "  跳过（E2E_FIXTURE=0）"
fi

say "0b. 首次启动：还没有管理员口令时应该明确说「先设口令」"
start_server
code=$(curl -s -o "${WORK}/pre.json" -w '%{http_code}' "${BASE}/api/connections")
[ "${code}" = "428" ] || fail "首启时 /api/connections 应返回 428，实际 ${code}"
grep -q "管理员口令" "${WORK}/pre.json" || fail "首启提示里应该提到管理员口令"

say "1. 建管理员（口令只存 argon2id 哈希；首启端点要 CSRF，不能裸奔）"
JAR="${WORK}/cookies.txt"
curl -fsS -c "${JAR}" "${BASE}/api/session" >/dev/null
CSRF=$(python3 - "${JAR}" <<'PYCOOKIE'
import sys
# curl 的 Netscape 格式：domain 	 flag 	 path 	 secure 	 expires 	 name 	 value
for line in open(sys.argv[1], encoding="utf-8"):
    if line.startswith("#") and not line.startswith("#HttpOnly_"):
        continue
    f = line.rstrip("\n").split("\t")
    if len(f) >= 7 and f[5] == "jl_csrf":
        print(f[6])
        break
PYCOOKIE
)
[ -n "${CSRF}" ] || fail "GET /api/session 没有种下 jl_csrf cookie"

# 先证明「没有 CSRF 的 setup 会被拒」，再证明「带上就能过」
NOCSRF=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${BASE}/api/setup" \
  -H 'Content-Type: application/json' --data-binary '{"password":"should-not-work-here"}')
[ "${NOCSRF}" = "403" ] || fail "无 CSRF 的 /api/setup 应返回 403，实际 ${NOCSRF}"
echo "  无 CSRF 的 setup 被拒（403），符合预期"

TOKEN=$(curl -fsS -b "${JAR}" -c "${JAR}" -X POST "${BASE}/api/setup" \
  -H "X-CSRF-Token: ${CSRF}" -H 'Content-Type: application/json' \
  --data-binary '{"password":"e2e-night-shift-password"}' | json '["token"]')
[ -n "${TOKEN}" ] || fail "setup 没有返回 token"

say "2. 建连接（一条 dsn 就够）并真的测连"
# DSN 通过环境变量交给 python，避免出现在进程参数（ps 可见）里。
export E2E_SRC_DSN="${SRC_DSN}" E2E_TGT_DSN="${TGT_DSN}"
export E2E_SRC_LABEL="${SRC_LABEL}" E2E_TGT_LABEL="${TGT_LABEL}"
SRC_JSON=$(python3 -c 'import json,os;print(json.dumps({"name":"e2e-src-"+os.environ["E2E_SRC_LABEL"],"dsn":os.environ["E2E_SRC_DSN"]}))')
api POST /api/connections "${SRC_JSON}" >"${WORK}/conn.json"
CONN_ID=$(json '["connection"]["id"]' <"${WORK}/conn.json")
SRC_VERSION=$(json '["test"]["server_version"]' <"${WORK}/conn.json")
[ -n "${SRC_VERSION}" ] || fail "源端测连没有返回服务端版本"
echo "  源 ${SRC_LABEL} ${SRC_VERSION}，连接 ${CONN_ID}"

if [ "${TGT_DSN}" = "${SRC_DSN}" ]; then
  MY_ID="${CONN_ID}"; MY_VERSION="${SRC_VERSION}"; EXPECT_CONNS=1
  echo "  目标复用同一连接 ${MY_ID}（同引擎自环，目标换 schema）"
else
  TGT_JSON=$(python3 -c 'import json,os;print(json.dumps({"name":"e2e-tgt-"+os.environ["E2E_TGT_LABEL"],"dsn":os.environ["E2E_TGT_DSN"]}))')
  api POST /api/connections "${TGT_JSON}" >"${WORK}/conn2.json"
  MY_ID=$(json '["connection"]["id"]' <"${WORK}/conn2.json")
  MY_VERSION=$(json '["test"]["server_version"]' <"${WORK}/conn2.json")
  [ -n "${MY_VERSION}" ] || fail "目标端测连没有返回服务端版本"
  EXPECT_CONNS=2
  echo "  目标 ${TGT_LABEL} ${MY_VERSION}，连接 ${MY_ID}"
fi

say "3. 枚举库表（catalog）"
# 不传 schema：让服务端列出它自己认的第一个 schema，脚本才对 PG/MySQL 都成立。
api GET "/api/catalog?connection_id=${CONN_ID}" >"${WORK}/cat.json"
SRC_SCHEMA=$(json '["schema"]' <"${WORK}/cat.json")
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
echo "  schema=${SRC_SCHEMA}，选中表：${TABLE}"

say "4. 建任务"
JOB_BODY=$(python3 - "${CONN_ID}" "${MY_ID}" "${TABLE}" "${SRC_SCHEMA}" <<'PY'
import json,sys
conn, my, table, schema = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
print(json.dumps({
  "name": "e2e-orders",
  "source_id": conn,
  "target_id": my,
  "enabled": False,
  "spec": {
    "source": {"scope": {"schema": schema, "include": [table]}},
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
  INTERVAL_BODY=$(python3 - "${CONN_ID}" "${MY_ID}" "${TABLE}" "${SRC_SCHEMA}" <<'PYBODY'
import json, sys
conn, my, table, schema = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
print(json.dumps({
    "name": "e2e-orders",
    "source_id": conn,
    "target_id": my,
    "enabled": False,
    "spec": {
        "source": {"scope": {"schema": schema, "include": [table]}},
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

say "6. run 的能力边界：MySQL 目标端要明确拒绝（不能回 202 之后什么都不发生）"
BOUNDARY_CODE=$(curl -s -o "${WORK}/run-boundary.json" -w '%{http_code}' \
  -X POST "${BASE}/api/jobs/${JOB_ID}/run" -H "Authorization: Bearer ${TOKEN}")
if [ "${TGT_LABEL}" = "postgres" ]; then
  echo "  （本次目标端是 PostgreSQL，跳过边界检查）"
else
  [ "${BOUNDARY_CODE}" = "422" ] || fail "MySQL 目标端应返回 422，实际 ${BOUNDARY_CODE}"
  python3 - "${WORK}/run-boundary.json" <<'PYB'
import json,sys
e=json.load(open(sys.argv[1]))["error"]
assert "PostgreSQL" in e["why"] or "只支持" in e["message"], e
assert e.get("next"), "能力边界也要给下一步"
print("  422 + 四段式：%s" % e["message"])
PYB
fi

say "6b. 建一个 PostgreSQL → PostgreSQL 的任务，跑真引擎"
if [ "${SRC_LABEL}" != "postgres" ]; then
  echo "  SKIP：本次只有 MySQL 源；B1 引擎是 PG→PG，跳过真跑"
  REAL_JOB="${JOB_ID}"
else
  # 目标端用同一个 PG 连接的另一个 schema（fixture 已建好 jl_e2e_out）
  api GET "/api/catalog?connection_id=${CONN_ID}&schema=${SRC_SCHEMA}" >"${WORK}/cat2.json"
  PG_JOB=$(python3 - "${CONN_ID}" "${TABLE}" "${SRC_SCHEMA}" <<'PYP'
import json,sys
conn, table, schema = sys.argv[1], sys.argv[2], sys.argv[3]
print(json.dumps({
  "name": "e2e-pg2pg",
  "source_id": conn,
  "target_id": conn,
  "enabled": False,
  "spec": {
    "source": {"scope": {"schema": schema, "include": [table]}},
    "target": {"schema": "jl_e2e_out", "on_exists": "append", "create_indexes": "defer"},
    "mode": {"frequency": "once"},
    "content": {"tables": {table: {}}}
  }
}))
PYP
)
  api POST /api/jobs "${PG_JOB}" >"${WORK}/pgjob.json"
  REAL_JOB=$(json '["id"]' <"${WORK}/pgjob.json")
  [ -n "${REAL_JOB}" ] || fail "建 PG→PG 任务失败"
  ACC_CODE=$(curl -s -o "${WORK}/run2.json" -w '%{http_code}' -X POST \
    "${BASE}/api/jobs/${REAL_JOB}/run" -H "Authorization: Bearer ${TOKEN}")
  [ "${ACC_CODE}" = "202" ] || { cat "${WORK}/run2.json"; fail "run 应该异步接受（202），实际 ${ACC_CODE}"; }
  json '["accepted"]' <"${WORK}/run2.json" | grep -qi true || fail "run 应返回 accepted=true"
  json '["events"]'      <"${WORK}/run2.json" | grep -q '/events' || fail "run 应告诉客户端事件流地址"
  echo "  202 accepted（异步），事件流：$(json '["events"]' <"${WORK}/run2.json")"
fi

say "7. 读 SSE，看阶段变化（不经浏览器；凭证只走请求头）"
# query 里放 token 会被访问日志/历史/Referer 收走，所以这条路径必须被拒绝
QTOKEN=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
  "${BASE}/api/jobs/${REAL_JOB}/events?token=${TOKEN}")
[ "${QTOKEN}" = "401" ] || fail "?token= 形式的 SSE 应该被拒绝（401），实际 ${QTOKEN}"
echo "  ?token= 被拒绝（401）：凭证不再进 URL"
curl -sN --max-time 30 -H "Authorization: Bearer ${TOKEN}" \
  "${BASE}/api/jobs/${REAL_JOB}/events" >"${WORK}/sse.txt" || true
if [ "${SRC_LABEL}" != "postgres" ]; then
  echo "  SKIP：没有 PG 源，无法产生真事件"
else
  grep -q '"type":"done"' "${WORK}/sse.txt" || { head -30 "${WORK}/sse.txt"; fail "SSE 里没有看到 done 事件"; }
  for phase in prepare snapshot draining done; do
    grep -q "\"phase\":\"${phase}\"" "${WORK}/sse.txt" || fail "SSE 里缺少阶段 ${phase}"
  done
  grep -q '"type":"progress"' "${WORK}/sse.txt" || fail "SSE 里没有 progress（全量进度）"
  grep -q 'event: phase' "${WORK}/sse.txt" || fail "SSE 帧缺少 event: 行（浏览器要靠它订阅）"
  echo "  SSE 事件行数：$(wc -l <"${WORK}/sse.txt")，阶段与进度都出现"
fi

say "8. 运行历史落库（真引擎：phase=done、finish_reason=completed）"
if [ "${SRC_LABEL}" != "postgres" ]; then
  echo "  SKIP：没有真跑"
else
  api GET "/api/runs?job_id=${REAL_JOB}" >"${WORK}/runs.json"
  python3 - "${WORK}/runs.json" <<'PYR'
import json,sys
d=json.load(open(sys.argv[1]))
runs=d.get("runs") or []
assert len(runs) >= 1, "应该有运行记录"
r=runs[0]
assert r["phase"] == "done", r
assert r.get("finish_reason") == "completed", r
assert r.get("kind") == "sync", r
print("  运行记录：phase=%s finish_reason=%s rows=%s kind=%s" % (r["phase"], r.get("finish_reason"), r["rows_done"], r.get("kind")))
PYR
fi

say "9. export 出 yaml（规范路径 /export；不能含任何密码）"
curl -fsS "${BASE}/api/jobs/${JOB_ID}/export" -H "Authorization: Bearer ${TOKEN}" >"${WORK}/job.yaml"
# 弃用别名 /yaml 必须返回逐字节相同的内容（保留一个周期，之后移除时同步改 E1009 文案）
curl -fsS "${BASE}/api/jobs/${JOB_ID}/yaml" -H "Authorization: Bearer ${TOKEN}" >"${WORK}/job-alias.yaml"
cmp -s "${WORK}/job.yaml" "${WORK}/job-alias.yaml" \
  || fail "/export 与弃用别名 /yaml 返回的内容不一致（别名必须逐字节相同）"
echo "  /export 与 /yaml 内容一致（别名可用，规范路径已生效）"
grep -qiE 'password|passwd|secret' "${WORK}/job.yaml" && fail "导出的 yaml 里出现了密码字段"
grep -qE '://[^/"[:space:]]+:[^@"[:space:]]+@' "${WORK}/job.yaml" && fail "导出的 yaml 里出现了带口令的连接串"
grep -q 'env:JINLIANG_CONN_' "${WORK}/job.yaml" || fail "导出的 yaml 应该用 env:JINLIANG_CONN_<id> 占位"

say "10. 重启进程：连接与任务必须还在（控制库落盘）"
kill "${SERVER_PID}"; wait "${SERVER_PID}" 2>/dev/null || true; SERVER_PID=""
start_server
TOKEN=$(curl -fsS -X POST "${BASE}/api/login" -H 'Content-Type: application/json' \
  --data-binary '{"password":"e2e-night-shift-password"}' | json '["token"]')
api GET /api/connections >"${WORK}/conns2.json"
json '["count"]' <"${WORK}/conns2.json" | grep -qx "${EXPECT_CONNS}" || fail "重启后连接数不对（期望 ${EXPECT_CONNS}）"
api GET "/api/jobs/${JOB_ID}" >"${WORK}/job2.json"
json '["job"]["job_key"]' <"${WORK}/job2.json" | grep -qx "${JOB_KEY}" || fail "重启后任务指纹变了"

say "11. 纯 CLI：doctor / export / import 往返"
"${BIN}" doctor --data-dir "${DATA}" >/dev/null || fail "doctor 报告不健康"
"${BIN}" export --data-dir "${DATA}" --out "${WORK}/cli-export.yaml" || fail "CLI export 失败"
"${BIN}" import --data-dir "${DATA}" --file "${WORK}/cli-export.yaml" || fail "CLI import 往返失败"

say "12. 导出的 yaml 必须过 schema 校验（验收 ③，这里是断言不是打印）"
STRICT_RC=0
python3 scripts/validate_schema.py "${WORK}/job.yaml" "${WORK}/cli-export.yaml" \
  >"${WORK}/schema.log" 2>&1 || STRICT_RC=$?
if [ "${STRICT_RC}" = "0" ]; then
  cat "${WORK}/schema.log"
  echo "  validate_schema.py（真 jsonschema）判定通过"
elif grep -qE 'ModuleNotFoundError|No module named' "${WORK}/schema.log"; then
  # 离线沙盒装不上 jsonschema/pyyaml（出口 PyPI 不可达）——显式 SKIP 严格校验，
  # 改用读同一份 schema/job.schema.json 的 stdlib 实现，仍然是硬断言。
  echo "  SKIP：validate_schema.py 缺依赖（jsonschema/pyyaml 不可安装）"
  grep -E 'No module named' "${WORK}/schema.log" | sed 's/^/    /'
  python3 scripts/validate_schema_lite.py "${WORK}/job.yaml" "${WORK}/cli-export.yaml" \
    || fail "yaml 不符合 schema/job.schema.json（stdlib 校验器判定失败）"
  echo "  validate_schema_lite.py 通过（与 CI 用的是同一个 schema 文件）"
else
  cat "${WORK}/schema.log" >&2
  fail "validate_schema.py 判定 yaml 不合 schema"
fi

say "13. 本次验收覆盖了哪些库（缺的必须是 SKIP，不能算通过）"
printf '  %-30s %s\n' "JINLIANG_TEST_PG_DSN"      "$([ -n "${PG_TEST_DSN}" ] && echo "USED/SKIP-BY-NEED" || echo "SKIP(未设置)")"
printf '  %-30s %s\n' "JINLIANG_TEST_PG_SRC_DSN"  "$([ -n "${PG_SRC_DSN}" ] && echo "USED/SKIP-BY-NEED" || echo "SKIP(未设置)")"
printf '  %-30s %s\n' "JINLIANG_TEST_MYSQL_DSN"   "$([ -n "${MY_TEST_DSN}" ] && echo "USED/SKIP-BY-NEED" || echo "SKIP(未设置)")"
printf '  %-30s %s\n' "JINLIANG_TEST_MYSQL_SRC_DSN" "$([ -n "${MY_SRC_DSN}" ] && echo "USED/SKIP-BY-NEED" || echo "SKIP(未设置)")"

say "PASS：headless 全链路通过（连接 → 库表 → 任务 → 预检 → 桩运行 → SSE → 导出 → 重启 → CLI）"
