#!/bin/bash
# 干净重跑：删旧任务 + 清目标表 + 新任务 + SSE 抓事件 + 毫秒计时。
#   ./demo3.sh clean     # 清理并建新任务
#   ./demo3.sh run       # 起跑，抓 SSE，轮询到 streaming，打全量耗时
#   ./demo3.sh incr      # 源端 20 ins / 10 upd / 5 del，等追平，打耗时
#   ./demo3.sh counts    # 两端行数 + 校验和
set -u
B=${B:-http://127.0.0.1:8080}
AUTH="Authorization: Bearer ${TOK:-demo}"
SRC_ID=${SRC_ID:-$(grep SRC_ID= /tmp/jl_ids.sh | cut -d= -f2)}
TGT_ID=${TGT_ID:-$(grep TGT_ID= /tmp/jl_ids2.sh | cut -d= -f2)}
IDF=/tmp/jl_ids3.sh

psrc() { docker exec -i pgsrc psql -U jinliang -d src -tAc "$1"; }
ptgt() { docker exec -i pgreport psql -U jinliang -d report -tAc "$1"; }
ms() { date +%s%3N; }
CSUM="select count(*), sum(id), sum(amount_cents), coalesce(md5(string_agg(id||':'||customer_id||':'||amount_cents||':'||status||':'||region, ',' order by id)),'-') from orders"

case "${1:-}" in
clean)
  for id in $(curl -sS $B/api/jobs -H "$AUTH" | jq -r '.jobs[]?.id'); do
    curl -sS -X DELETE $B/api/jobs/$id -H "$AUTH" >/dev/null; echo "deleted job $id"
  done
  ptgt "drop table if exists orders; drop table if exists customers;" >/dev/null
  echo "== 目标库表已清空: $(ptgt "select count(*) from information_schema.tables where table_schema='public' and table_name in ('orders','customers')")"
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
  }" | tee /tmp/job3.json | jq -c '{id,name,job_key}'
  JOB=$(jq -r .id /tmp/job3.json); printf 'JOB3=%s\n' "$JOB" > $IDF
  curl -sS -X POST $B/api/jobs/$JOB/preflight -H "$AUTH" | jq -c '{can_start, warn:[.checks[]|select(.status!="ok")|.id]}'
  ;;
run)
  . $IDF; JOB=${JOB:-$JOB3}
  rm -f ~/events.log
  (curl -sS -N "$B/api/jobs/$JOB/events" -H "$AUTH" > ~/events.log 2>&1 &) 
  sleep 1
  T0=$(ms)
  curl -sS -X POST $B/api/jobs/$JOB/run -H "$AUTH" -H 'Content-Type: application/json' -d '{}' | jq -c '{accepted,job_id}'
  for i in $(seq 1 900); do
    PH=$(curl -sS $B/api/jobs/$JOB -H "$AUTH" | jq -r '.runs[0].phase // "?"')
    case "$PH" in
      catchup) [ -z "${S:-}" ] && S=$(ms) && echo "PHASE catchup at +$((S-T0))ms" ;;
      streaming) N=$(ms); echo "PHASE streaming at +$((N-T0))ms"; echo "FULL_LOAD_MS=$((N-T0))"; break ;;
      failed) echo "FAILED"; break ;;
    esac
    sleep 0.3
  done
  sleep 2
  echo "== run: $(curl -sS $B/api/jobs/$JOB -H "$AUTH" | jq -c '.runs[0]|{phase,rows_done,lag_bytes,started_at,heartbeat_at}')"
  pkill -f 'api/jobs/.*/events' 2>/dev/null
  echo "== events: $(wc -l < ~/events.log) 行"
  ;;
incr)
  . $IDF; JOB=${JOB:-$JOB3}
  echo "== 源端增量"
  psrc "insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) select 300000+g, 1+(g%20000), 12345+g, 2, 'cn', 'incr', now() from generate_series(1,20) g;" 
  psrc "update orders set amount_cents = amount_cents + 777, updated_at = now() where id between 1001 and 1010;"
  psrc "delete from orders where id between 101 and 105;"
  EXP=$(psrc "select count(*) from orders"); EXPECT_MD5=$(psrc "select md5(string_agg(id||':'||customer_id||':'||amount_cents||':'||status||':'||region, ',' order by id)) from orders")
  T0=$(ms)
  for i in $(seq 1 240); do
    GOT=$(ptgt "select count(*) from orders")
    if [ "$GOT" = "$EXP" ]; then
      GMD5=$(ptgt "select md5(string_agg(id||':'||customer_id||':'||amount_cents||':'||status||':'||region, ',' order by id)) from orders")
      if [ "$GMD5" = "$EXPECT_MD5" ]; then N=$(ms); echo "CATCHUP_MS=$((N-T0)) rows=$GOT md5=$GMD5"; break; fi
    fi
    sleep 0.2
  done
  echo "== run: $(curl -sS $B/api/jobs/$JOB -H "$AUTH" | jq -c '.runs[0]|{phase,rows_done,lag_bytes}')"
  ;;
counts)
  echo "src: $(psrc "$CSUM")"
  echo "tgt: $(ptgt "$CSUM")"
  echo "src customers: $(psrc "select count(*), md5(string_agg(id||':'||name||':'||coalesce(city,''), ',' order by id)) from customers")"
  echo "tgt customers: $(ptgt "select count(*), md5(string_agg(id||':'||name||':'||coalesce(city,''), ',' order by id)) from customers")"
  ;;
*) echo "usage: $0 clean|run|incr|counts" >&2; exit 2;;
esac
