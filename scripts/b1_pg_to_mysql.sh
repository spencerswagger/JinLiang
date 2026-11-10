#!/usr/bin/env bash
# PG 源 → MySQL 目标端：全量 + 增量 + 中断/重搬验收。
#
#   ROWS=20000 scripts/b1_pg_to_mysql.sh ./dist/jinliang                # 全量 + 增量（增/改/删）
#   ROWS=20000 scripts/b1_pg_to_mysql.sh ./dist/jinliang once           # 只搬存量
#   ROWS=20000 scripts/b1_pg_to_mysql.sh ./dist/jinliang kill           # 全量中途 kill -9 再重启
#   ROWS=20000 scripts/b1_pg_to_mysql.sh ./dist/jinliang fullinterval   # 整轮重搬：源端删行必须消失
#
# 两个 DSN 从环境读：JINLIANG_TEST_PG_SRC_DSN（源）、JINLIANG_TEST_MYSQL_DSN（目标）。
# 缺任何一个 → exit 78（EX_CONFIG）= CI 红，不静默跳过。
set -euo pipefail

BIN=${1:-./dist/jinliang}
MODE=${2:-full}
ROWS=${ROWS:-20000}
SRC=${JINLIANG_TEST_PG_SRC_DSN:-}
DST=${JINLIANG_TEST_MYSQL_DSN:-}

if [ -z "$SRC" ] || [ -z "$DST" ]; then
  echo "EX_CONFIG: 缺 JINLIANG_TEST_PG_SRC_DSN / JINLIANG_TEST_MYSQL_DSN" >&2
  exit 78
fi
for c in psql mysql; do
  command -v "$c" >/dev/null 2>&1 || { echo "EX_CONFIG: 没有 $c" >&2; exit 78; }
done

# 从 DSN 里取 MySQL 的库名与连接参数（不硬编码）
MY_HOST=$(printf '%s' "$DST" | sed -E 's#^mysql://[^@]*@([^:/]+).*#\1#')
MY_PORT=$(printf '%s' "$DST" | sed -E 's#^mysql://[^@]*@[^:]+:([0-9]+).*#\1#')
MY_USER=$(printf '%s' "$DST" | sed -E 's#^mysql://([^:]+):.*#\1#')
MY_PASS=$(printf '%s' "$DST" | sed -E 's#^mysql://[^:]+:([^@]*)@.*#\1#')
MY_DB=$(printf '%s' "$DST" | sed -E 's#^mysql://[^/]+/([^?]+).*#\1#')

my() { mysql -h "$MY_HOST" -P "$MY_PORT" -u "$MY_USER" -p"$MY_PASS" -N "$@" 2>/dev/null; }
pg() { psql "$SRC" -v ON_ERROR_STOP=1 -tAq -c "$1"; }

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
DATA="$WORK/data"
TBL=my_orders

fingerprint_pg() {
  pg "SELECT count(*)::text || ':' || COALESCE(md5(string_agg(id||'|'||payload||'|'||amount::text||'|'||
      to_char(created_at at time zone 'UTC','YYYY-MM-DD HH24:MI:SS.US'), E'\n' order by id)), '-')
      FROM $TBL"
}
fingerprint_my() {
  my -e "SET SESSION group_concat_max_len=1000000000;
    SELECT CONCAT(COUNT(*), ':', COALESCE(md5(group_concat(concat_ws('|', id, payload, amount,
      date_format(created_at,'%Y-%m-%d %H:%i:%s.%f')) ORDER BY id SEPARATOR '\n')), '-'))
    FROM \`$MY_DB\`.\`$TBL\`"
}
my_rows() { my -e "SELECT COUNT(*) FROM \`$MY_DB\`.\`$TBL\`"; }

echo "== 1. 造数：源端 $ROWS 行 =="
pg "DROP TABLE IF EXISTS $TBL" >/dev/null
pg "CREATE TABLE $TBL (id bigint PRIMARY KEY, payload text, amount numeric(12,2), created_at timestamptz)" >/dev/null
pg "INSERT INTO $TBL SELECT g, 'row-'||g, (g%10000)::numeric/100, now() - (g||' seconds')::interval
    FROM generate_series(1,$ROWS) g" >/dev/null
echo "   源端指纹 $(fingerprint_pg)"

echo "== 2. 清空目标库与残留对象 =="
my -e "DROP TABLE IF EXISTS \`$MY_DB\`.\`$TBL\`" || true
my -e "DROP TABLE IF EXISTS \`$MY_DB\`.\`_jl_meta\`, \`$MY_DB\`.\`_jl_chunk\`, \`$MY_DB\`.\`_jl_lease\`, \`$MY_DB\`.\`_jl_object\`, \`$MY_DB\`.\`_jl_instance\`" || true
pg "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'jl_%'" >/dev/null
pg "DO \$\$ DECLARE p text; BEGIN FOR p IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'jl_%' LOOP EXECUTE 'DROP PUBLICATION '||quote_ident(p); END LOOP; END \$\$" >/dev/null

FREQ=continuous+initial
[ "$MODE" = "once" ] && FREQ=once
[ "$MODE" = "fullinterval" ] && FREQ=full_interval
cat > "$WORK/job.yaml" <<YAML
version: 1
job: pg-to-mysql
source:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_SRC_DSN
  scope:
    schema: public
  snapshot:
    chunk_rows: 2000
target:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_DSN
  on_exists: append
content:
  tables:
    $TBL: {}
mode:
  frequency: $FREQ
  interval: 1h
reliability:
  batch_rows: 5000
YAML

echo "== 3. 导入任务 =="
"$BIN" import --file "$WORK/job.yaml" --data-dir "$DATA" | tail -2

if [ "$MODE" = "kill" ]; then
  echo "== 4. 全量中途 kill -9 =="
  "$BIN" run --job pg-to-mysql --data-dir "$DATA" --stop-after 600s > "$WORK/run1.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 400); do
    grep -q "第 2 块" "$WORK/run1.log" 2>/dev/null && break
    sleep 0.05
  done
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  MID=$(my_rows || echo 0)
  echo "   已 kill -9；目标端此刻 $MID 行"
  echo "   残留状态：$(my -e "SELECT CONCAT(phase, ' ', COALESCE(position,'(无位点)')) FROM \`$MY_DB\`.\`_jl_meta\`" 2>/dev/null || echo 无)"
  # 停机期间源端删掉一批（这些行已经在第 1 块里搬过去了）：MySQL 目标端没有块级续跑，
  # 位点又是在全量**之后**才写的，所以重启必然整轮重搬——重搬前必须先清表，
  # 否则源端删掉的行会永远留在目标端（upsert 不会删）。
  pg "DELETE FROM $TBL WHERE id BETWEEN 100 AND 199" >/dev/null
  echo "   停机期间源端删除了 100 行（id 100-199），重启后必须消失"
  echo "== 5. 重启：整轮重搬（清表后再搬）=="
  timeout 600 "$BIN" run --job pg-to-mysql --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run2.log" 2>&1 || true
  grep -E "清空了|全量完成|复制" "$WORK/run2.log" | head -4 || true
  if grep -qE "退出码 1|阶段 failed" "$WORK/run2.log"; then
    echo "--- 重启那一轮失败，日志： ---"; tail -12 "$WORK/run2.log"
  fi
  FINAL=$(my_rows)
  SRC_FP=$(fingerprint_pg); DST_FP=$(fingerprint_my)
  echo "   中断行数 $MID / 最终 $FINAL 行（源端 $((ROWS-100)) 行）"
  echo "   源端 $SRC_FP"
  echo "   目标 $DST_FP"
  [ "$FINAL" = "$((ROWS-100))" ] || { echo "FAIL: 重搬之后行数不对（应为 $((ROWS-100))）"; exit 1; }
  [ "$SRC_FP" = "$DST_FP" ] || { echo "FAIL: 重搬之后源与目标指纹不一致"; exit 1; }
  echo "PASS: 中断后整轮重搬并清表，$FINAL 行、指纹一致 $SRC_FP"
  exit 0
fi

if [ "$MODE" = "fullinterval" ]; then
  echo "== 4. 第一轮：整轮重搬（full_interval）=="
  "$BIN" run --job pg-to-mysql --data-dir "$DATA" > "$WORK/run1.log" 2>&1 || { tail -6 "$WORK/run1.log"; exit 1; }
  N1=$(my_rows)
  echo "   第一轮后目标端 $N1 行"
  [ "$N1" = "$ROWS" ] || { echo "FAIL: 第一轮应搬 $ROWS 行，实际 $N1"; exit 1; }

  echo "== 5. 源端删 500 行，第二轮必须清表重搬（不留脏行）=="
  pg "DELETE FROM $TBL WHERE id <= 500" >/dev/null
  "$BIN" run --job pg-to-mysql --data-dir "$DATA" > "$WORK/run2.log" 2>&1 || { tail -6 "$WORK/run2.log"; exit 1; }
  grep -E "清空了|全量完成" "$WORK/run2.log" | head -3 || true
  N2=$(my_rows)
  SRC_FP=$(fingerprint_pg); DST_FP=$(fingerprint_my)
  echo "   第二轮后目标端 $N2 行（源端 $((ROWS-500)) 行）"
  echo "   源端 $SRC_FP"
  echo "   目标 $DST_FP"
  [ "$N2" = "$((ROWS-500))" ] || {
    echo "FAIL: 目标端 $N2 行，源端 $((ROWS-500)) 行 —— 源端删掉的行留在目标端（重搬前没清表）"
    exit 1
  }
  [ "$SRC_FP" = "$DST_FP" ] || { echo "FAIL: 指纹不一致"; exit 1; }
  echo "PASS: 整轮重搬先清表，$N2 行、指纹一致 $SRC_FP"
  exit 0
fi

echo "== 4. 第一次跑（全量 → 增量 → 收尾）=="
"$BIN" run --job pg-to-mysql --data-dir "$DATA" --stop-after 10s > "$WORK/run1.log" 2>&1 || true
grep -E "全量完成|增量：" "$WORK/run1.log" | tail -3 || true
SRC_FP=$(fingerprint_pg); DST_FP=$(fingerprint_my)
echo "   全量后 源端 $SRC_FP / 目标 $DST_FP"
[ "$SRC_FP" = "$DST_FP" ] || { echo "FAIL: 全量之后源与目标不一致"; tail -8 "$WORK/run1.log"; exit 1; }

if [ "$MODE" = "once" ]; then
  echo "PASS: $ROWS 行，PG → MySQL 全量一致（行数 + md5）$SRC_FP"
  exit 0
fi

echo "== 5. 增量：插入 / 更新 / 删除各一笔，量追平时延 =="
pg "INSERT INTO $TBL VALUES ($ROWS+1, 'inserted-after-snapshot', 1.23, now())" >/dev/null
pg "UPDATE $TBL SET payload='updated-after-snapshot', amount=9.87 WHERE id=2" >/dev/null
pg "DELETE FROM $TBL WHERE id=3" >/dev/null
T0=$(date +%s.%N)
"$BIN" run --job pg-to-mysql --data-dir "$DATA" --stop-when-idle 3s --stop-after 120s > "$WORK/run2.log" 2>&1 &
RUNPID=$!
LAT=""
for _ in $(seq 1 900); do
  GOT=$(my -e "SELECT CONCAT(
      (SELECT COUNT(*) FROM \`$MY_DB\`.\`$TBL\` WHERE id=$((ROWS+1))), ',',
      (SELECT COUNT(*) FROM \`$MY_DB\`.\`$TBL\` WHERE id=2 AND payload='updated-after-snapshot'), ',',
      (SELECT COUNT(*) FROM \`$MY_DB\`.\`$TBL\` WHERE id=3))" 2>/dev/null || echo "0,0,1")
  if [ "$GOT" = "1,1,0" ]; then
    T1=$(date +%s.%N)
    LAT=$(python3 -c "print(f'{$T1-$T0:.2f}')")
    break
  fi
  sleep 0.1
done
wait "$RUNPID" 2>/dev/null || true
if [ -z "$LAT" ]; then
  echo "FAIL: 增量 90 秒内没追平（最后一次：${GOT:-无}）"
  tail -20 "$WORK/run2.log"
  exit 1
fi
echo "   追平时延 ${LAT}s（源端三笔改动 → 目标端全部可见）"
if grep -qE "复制 [1-9]" "$WORK/run2.log"; then
  echo "FAIL: 增量轮不该再整表重搬（日志里出现了「复制 N 行」）"; exit 1
fi

echo "== 6. 对比（行数 + 内容指纹）=="
SRC_FP=$(fingerprint_pg); DST_FP=$(fingerprint_my)
echo "   源端 $SRC_FP"
echo "   目标 $DST_FP"
[ "$SRC_FP" = "$DST_FP" ] || { echo "FAIL: 增量之后源与目标不一致"; exit 1; }

echo "PASS: PG → MySQL 全量 + 增量一致（$ROWS 行、指纹相同、追平 ${LAT}s）"
