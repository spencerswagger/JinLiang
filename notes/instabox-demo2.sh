#!/bin/bash
# InstaBox 真机演示（替代口径）：origin/main 只支持 PG -> PG，这里跑同规模的数据流，
# 指标口径与 PG->MySQL 版完全一致（全量耗时 / 追平耗时 / 行数 / 校验和 / 延迟）。
#
#   ./demo2.sh conn    # 建 pg-report 连接 + 测连
#   ./demo2.sh job     # 建任务 + preflight
#   ./demo2.sh run     # 起跑并轮询到 streaming（打全量耗时）
#   ./demo2.sh incr    # 源端 20 ins / 10 upd / 5 del，等追平并打耗时
#   ./demo2.sh counts  # 两端行数 + 校验和对比
set -u
B=${B:-http://127.0.0.1:8080}
AUTH="Authorization: Bearer ${TOK:-demo}"
SRC_ID=${SRC_ID:-$(grep SRC_ID= /tmp/jl_ids.sh | cut -d= -f2)}
IDF=/tmp/jl_ids2.sh
TGT_DSN='postgres://jinliang:jinliang@127.0.0.1:55433/report?sslmode=disable'

psrc() { docker exec -i pgsrc psql -U jinliang -d src -tAc "$1"; }
ptgt() { docker exec -i pgreport psql -U jinliang -d report -tAc "$1"; }
q() { psql=$1; shift; $psql "$@"; }

CSUM_SQL="select count(*), sum(id), sum(amount_cents), coalesce(md5(string_agg(id || ':' || customer_id || ':' || amount_cents || ':' || status || ':' || region, ',' order by id)),'-') from orders"

case "${1:-}" in
conn)
  curl -sS -X POST $B/api/connections -H "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"name\":\"pg-report\",\"dsn\":\"$TGT_DSN\"}" \
    | jq -c '{id:.connection.id,kind:.connection.kind,test_ok:.test.ok,ms:.test.latency_ms,err:.test.error}' \
    | tee /tmp/conn2.json
  TGT_ID=$(jq -r .id /tmp/conn2.json)
  printf 'TGT_ID=%s\nSRC_ID=%s\n' "$TGT_ID" "$SRC_ID" > $IDF
  echo "== test"
  curl -sS -X POST $B/api/connections/$TGT_ID/test -H "$AUTH" | jq -c '{ok,version,latency_ms,error}'
  ;;
job)
  . $IDF
  curl -sS -X POST $B/api/jobs -H "$AUTH" -H 'Content-Type: application/json' -d "{
    \"name\": \"instabox-demo-orders-pg\",
    \"source_id\": \"$SRC_ID\",
    \"target_id\": \"$TGT_ID\",
    \"enabled\": true,
    \"spec\": {
      \"version\": 1,
      \"source\": {\"kind\":\"postgres\",\"dsn\":\"env:X\",\"scope\":{\"schema\":\"public\",\"include\":[\"orders\",\"customers\"]}},
      \"target\": {\"kind\":\"postgres\",\"dsn\":\"env:X\",\"schema\":\"public\",\"on_exists\":\"append\",\"create_indexes\":\"defer\"},
      \"mode\": {\"frequency\":\"continuous+initial\"},
      \"content\": {\"tables\":{\"orders\":{},\"customers\":{}}}
    }
  }" | tee /tmp/job2.json | jq -c '{id,name,job_key}'
  JOB_ID=$(jq -r .id /tmp/job2.json)
  printf 'JOB2_ID=%s\n' "$JOB_ID" >> $IDF
  echo "== preflight"
  curl -sS -X POST $B/api/jobs/$JOB_ID/preflight -H "$AUTH" | tee /tmp/pf2.json | jq -c '{can_start,checks:[.checks[]|{id,status,detail}]}'
  ;;
run)
  . $IDF; JOB_ID=$JOB2_ID
  T0=$(date +%s.%N)
  curl -sS -X POST $B/api/jobs/$JOB_ID/run -H "$AUTH" -H 'Content-Type: application/json' -d '{}' | jq -c . | head -3
  SNAP_DONE=""; CATCH_DONE=""
  for i in $(seq 1 300); do
    R=$(curl -sS $B/api/jobs/$JOB_ID -H "$AUTH" | jq -c '.runs[0] // .last_run')
    PH=$(echo "$R" | jq -r '.phase // "?"'); RD=$(echo "$R" | jq -r '.rows_done // 0'); LG=$(echo "$R" | jq -r '.lag_ms // "-"')
    NOW=$(date +%s.%N)
    if [ "$PH" = "snapshot" ] || [ "$PH" = "prepare" ] || [ "$PH" = "preflight" ]; then LAST=$NOW; fi
    if [ "$PH" = "catchup" ] && [ -z "$SNAP_DONE" ]; then SNAP_DONE=$NOW; echo "  [$(echo "$NOW-$T0"|bc)s] phase=catchup rows_done=$RD"; fi
    if [ "$PH" = "streaming" ] && [ -z "$CATCH_DONE" ]; then
      CATCH_DONE=$NOW
      echo "  [$(echo "$NOW-$T0"|bc)s] phase=streaming rows_done=$RD lag_ms=$LG"
      echo "FULL_LOAD_SECONDS=$(echo "$NOW-$T0"|bc)"
      break
    fi
    if [ "$PH" = "failed" ]; then echo "FAILED: $R"; break; fi
    sleep 1
  done
  echo "== final run: $(curl -sS $B/api/jobs/$JOB_ID -H "$AUTH" | jq -c '.runs[0]')"
  ;;
incr)
  . $IDF; JOB_ID=$JOB2_ID
  echo "== 源端增量：20 insert / 10 update / 5 delete"
  psrc "insert into orders (id,customer_id,amount_cents,status,region,note,updated_at)
        select 200000+g, 1+(g%20000), 12345+g, 2, 'cn', 'incr', now() from generate_series(1,20) g;" 
  psrc "update orders set amount_cents = amount_cents + 777, updated_at = now() where id between 1001 and 1010;"
  psrc "delete from orders where id between 101 and 105;"
  T0=$(date +%s.%N)
  EXP=$(psrc "select count(*) from orders")
  for i in $(seq 1 120); do
    GOT=$(ptgt "select count(*) from orders")
    if [ "$GOT" = "$EXP" ] && [ "$(ptgt "select count(*) from orders where id between 1001 and 1010 and amount_cents % 1000 = 789")" -ge 1 ] 2>/dev/null; then :; fi
    if [ "$GOT" = "$EXP" ]; then
      T1=$(date +%s.%N); echo "CATCHUP_SECONDS=$(echo "$T1-$T0"|bc) (target=$GOT source=$EXP)"; break
    fi
    sleep 0.5
  done
  curl -sS $B/api/jobs/$JOB_ID -H "$AUTH" | jq -c '.runs[0] | {phase,rows_done,lag_ms,lag_bytes}'
  ;;
counts)
  echo "== orders 源端:"; psrc "$CSUM_SQL"
  echo "== orders 目标端:"; ptgt "$CSUM_SQL"
  echo "== customers 源端:"; psrc "select count(*), coalesce(md5(string_agg(id||':'||name||':'||coalesce(city,''), ',' order by id)),'-') from customers"
  echo "== customers 目标端:"; ptgt "select count(*), coalesce(md5(string_agg(id||':'||name||':'||coalesce(city,''), ',' order by id)),'-') from customers"
  ;;
*)
  echo "usage: $0 conn|job|run|incr|counts" >&2; exit 2;;
esac
