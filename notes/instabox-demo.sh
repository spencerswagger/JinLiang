#!/bin/bash
# InstaBox 真机演示：无浏览器链路（curl）跑一次 PG -> MySQL 同步。
# 用法（在 InstaBox 沙箱内）：
#   ./demo.sh setup   # 设管理员口令 + 建两个连接 + 测连 + catalog
#   ./demo.sh job     # 建任务 + preflight
#   ./demo.sh run     # run + 轮询到全量完成
#   ./demo.sh incr    # 源库增量 20 ins / 10 upd / 5 del，等追平
#   ./demo.sh status  # 打印任务与最近运行
set -u
B=${B:-http://127.0.0.1:8080}
TOK=${TOK:-demo}
AUTH="Authorization: Bearer $TOK"
PW=${PW:-Demo-Passw0rd-2026}
J=/tmp/jl_cookies.txt
SRC_DSN='postgres://jinliang:jinliang@127.0.0.1:55432/src?sslmode=disable'
TGT_DSN='mysql://jinliang:jinliang@127.0.0.1:53306/report'
IDF=/tmp/jl_ids.sh

post() { curl -sS -X POST "$1" -H "$AUTH" -H 'Content-Type: application/json' ${2:+-d "$2"}; }

case "${1:-}" in
setup)
  rm -f $J
  echo "== GET /api/session"
  curl -sS -c $J $B/api/session; echo
  CSRF=$(grep -w jl_csrf $J | cut -f7)
  echo "== POST /api/setup (csrf=${CSRF:0:8}…)"
  curl -sS -b $J -c $J -X POST $B/api/setup -H 'Content-Type: application/json' \
    -H "X-CSRF-Token: $CSRF" -d "{\"password\":\"$PW\"}" | jq -c '{ok,token:(.token|.[0:8]+"…")}'

  echo "== POST /api/connections (pg-src)"
  SRC=$(curl -sS -X POST $B/api/connections -H "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"name\":\"pg-src\",\"dsn\":\"$SRC_DSN\"}")
  echo "$SRC" | jq -c '{id:.connection.id,kind:.connection.kind,test_ok:.test.ok,ms:.test.latency_ms,err:.test.error}'

  echo "== POST /api/connections (mysql-report)"
  TGT=$(curl -sS -X POST $B/api/connections -H "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"name\":\"mysql-report\",\"dsn\":\"$TGT_DSN\"}")
  echo "$TGT" | jq -c '{id:.connection.id,kind:.connection.kind,test_ok:.test.ok,ms:.test.latency_ms,err:.test.error}'

  SRC_ID=$(echo "$SRC" | jq -r .connection.id)
  TGT_ID=$(echo "$TGT" | jq -r .connection.id)
  printf 'SRC_ID=%s\nTGT_ID=%s\n' "$SRC_ID" "$TGT_ID" > $IDF

  echo "== POST /api/connections/{id}/test (both)"
  post "$B/api/connections/$SRC_ID/test" | jq -c '{ok,version,latency_ms,error}'
  post "$B/api/connections/$TGT_ID/test" | jq -c '{ok,version,latency_ms,error}'

  echo "== GET /api/catalog?connection_id=$SRC_ID"
  curl -sS "$B/api/catalog?connection_id=$SRC_ID" -H "$AUTH" | jq -c '.tables[]? | {name,rows:.rows_estimate,pk}'
  ;;
job)
  . $IDF
  echo "== POST /api/jobs"
  curl -sS -X POST $B/api/jobs -H "$AUTH" -H 'Content-Type: application/json' -d "{
    \"name\": \"instabox-demo-orders\",
    \"source_id\": \"$SRC_ID\",
    \"target_id\": \"$TGT_ID\",
    \"enabled\": true,
    \"spec\": {
      \"version\": 1,
      \"source\": {\"kind\":\"postgres\",\"dsn\":\"env:X\",\"scope\":{\"schema\":\"public\",\"include\":[\"orders\",\"customers\"]}},
      \"target\": {\"kind\":\"mysql\",\"dsn\":\"env:X\",\"schema\":\"report\",\"on_exists\":\"append\",\"create_indexes\":\"defer\"},
      \"mode\": {\"frequency\":\"continuous+initial\"},
      \"content\": {\"tables\":{\"orders\":{},\"customers\":{}}}
    }
  }" | tee /tmp/job.json | jq -c '{id,name,source_kind,target_kind,job_key}'
  JOB_ID=$(jq -r .id /tmp/job.json)
  printf 'JOB_ID=%s\n' "$JOB_ID" >> $IDF
  echo "== POST /api/jobs/$JOB_ID/preflight"
  post "$B/api/jobs/$JOB_ID/preflight" | tee /tmp/preflight.json | jq -c '{verdict,summary:(.summary // .report.summary)}'
  ;;
run)
  . $IDF
  echo "== POST /api/jobs/$JOB_ID/run"; date +%s.%N
  post "$B/api/jobs/$JOB_ID/run" | jq -c .
  ;;
status)
  . $IDF
  curl -sS $B/api/jobs/$JOB_ID -H "$AUTH" | jq -c '{id,last_run:(.runs[0] // .last_run)}'
  ;;
*)
  echo "usage: $0 setup|job|run|status" >&2; exit 2;;
esac
