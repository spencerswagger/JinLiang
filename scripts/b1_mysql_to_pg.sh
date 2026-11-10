#!/usr/bin/env bash
# B4 验收：MySQL 源 → PostgreSQL 目标端的真链路（全量 + 增量 + 中断续跑）。
#
#   ROWS=200000 scripts/b1_mysql_to_pg.sh ./dist/jinliang            # 全量 → 增量 → 收尾
#   ROWS=200000 scripts/b1_mysql_to_pg.sh ./dist/jinliang kill       # 全量中途 kill -9 再重启
#   ROWS=200000 scripts/b1_mysql_to_pg.sh ./dist/jinliang once       # once：只搬存量
#   ROWS=20000  scripts/b1_mysql_to_pg.sh ./dist/jinliang binpk      # BINARY(16) 主键 + 中途 kill -9 续跑
#   CHUNK_ROWS=5000 ROWS=20000 scripts/b1_mysql_to_pg.sh ./dist/jinliang kill   # 小块跑得快
#
# 需要：一个 MySQL 源库（log_bin=ON，账号有 REPLICATION CLIENT/SLAVE）与一个 PG 目标库，
# 以及 mysql / psql 客户端。缺任何一个都以 78（EX_CONFIG）退出——CI 里这是红的，不是跳过。
set -euo pipefail

BIN=${1:-./dist/jinliang}
MODE=${2:-full}
ROWS=${ROWS:-200000}
SRC=${JINLIANG_TEST_MYSQL_SRC_DSN:-}
DST=${JINLIANG_TEST_PG_DSN:-}

if [ -z "$SRC" ] || [ -z "$DST" ]; then
  echo "EX_CONFIG: 缺 JINLIANG_TEST_MYSQL_SRC_DSN / JINLIANG_TEST_PG_DSN（MySQL 源 → PG 目标）" >&2
  exit 78
fi
for bin in mysql psql; do
  command -v "$bin" >/dev/null 2>&1 || { echo "EX_CONFIG: 没有 $bin 客户端" >&2; exit 78; }
done

# 从 DSN 里拆出 MySQL 客户端参数：mysql://user:pass@host:port/db
proto_strip=${SRC#mysql://}
creds=${proto_strip%%@*}
rest=${proto_strip#*@}
hostport=${rest%%/*}
dbpart=${rest#*/}
db=${dbpart%%\?*}
MUSER=${creds%%:*}
MPASS=${creds#*:}
MHOST=${hostport%%:*}
MPORT=${hostport#*:}
SRC_DB=$db

MYSQL=(mysql --protocol=TCP -h "$MHOST" -P "$MPORT" -u "$MUSER" "--password=$MPASS" --batch --skip-column-names)
mysql_src() { "${MYSQL[@]}" -e "$1" "$SRC_DB"; }
psql_dst() { psql "$DST" -v ON_ERROR_STOP=1 -tAq -c "$1"; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
JOB="$WORK/job.yaml"
DATA="$WORK/data"

# 内容指纹：两边都按「主键顺序拼接每行的 md5」再 md5 一次，所以能跨引擎比。
# 时间列格式必须一致：MySQL DATE_FORMAT 的 %f 是 6 位微秒，PG to_char 的 US 也是 6 位。
fp_mysql() {
  mysql_src "SET SESSION group_concat_max_len=1073741824;
    SELECT CONCAT(COUNT(*), ':', COALESCE(MD5(GROUP_CONCAT(h ORDER BY id SEPARATOR '')), '-'))
    FROM (SELECT id, MD5(CONCAT_WS('|', id, payload, amount,
             DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'))) h FROM orders) x"
}
fp_pg() {
  psql_dst "SELECT count(*)::text || ':' || COALESCE(md5(string_agg(h, '' ORDER BY id)), '-')
    FROM (SELECT id, md5(concat_ws('|', id, payload, amount,
             to_char(created_at, 'YYYY-MM-DD HH24:MI:SS.US'))) h FROM orders) x"
}

if [ "$MODE" = "binpk" ]; then
  # BINARY(16) 主键 + 快照中途 kill -9：块边界必须保类型，否则续跑按文本比会跳到表尾。
  # id 末尾带 0xFFFF（非法 UTF-8），这正是真实 UUID 存二进制的形态。
  echo "== 1. 造数：BINARY(16) 主键 $ROWS 行 =="
  mysql_src "DROP TABLE IF EXISTS bin_orders"
  mysql_src "CREATE TABLE bin_orders (
               id         BINARY(16) NOT NULL PRIMARY KEY,
               payload    VARCHAR(64) NOT NULL,
               amount     DECIMAL(12,2) NULL,
               created_at DATETIME(6) NULL
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
  STEP=${INSERT_STEP:-20000}
  i=0
  while [ "$i" -lt "$ROWS" ]; do
    hi=$((i + STEP - 1)); [ "$hi" -ge "$ROWS" ] && hi=$((ROWS - 1))
    mysql_src "SET SESSION cte_max_recursion_depth=1000000;
               INSERT INTO bin_orders(id,payload,amount,created_at)
               SELECT UNHEX(CONCAT(LPAD(HEX(n),28,'0'),'FFFF')), CONCAT('row-',n), (n % 10000) / 100,
                      TIMESTAMPADD(SECOND, -n, '2026-01-01 00:00:00')
               FROM (WITH RECURSIVE seq AS (SELECT $i AS n UNION ALL SELECT n+1 FROM seq WHERE n < $hi)
                     SELECT n FROM seq) s(n)"
    i=$((hi + 1))
  done
  fp_bin_mysql() {
    mysql_src "SET SESSION group_concat_max_len=1073741824;
      SELECT CONCAT(COUNT(*), ':', COALESCE(MD5(GROUP_CONCAT(h ORDER BY id SEPARATOR '')), '-'))
      FROM (SELECT id, MD5(CONCAT_WS('|', LOWER(HEX(id)), payload, amount,
               DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'))) h FROM bin_orders) x"
  }
  fp_bin_pg() {
    psql_dst "SELECT count(*)::text || ':' || COALESCE(md5(string_agg(h, '' ORDER BY id)), '-')
      FROM (SELECT id, md5(concat_ws('|', encode(id, 'hex'), payload, amount,
               to_char(created_at, 'YYYY-MM-DD HH24:MI:SS.US'))) h FROM bin_orders) x"
  }
  echo "   源端指纹 $(fp_bin_mysql)"

  echo "== 2. 清空目标端与残留状态 =="
  psql_dst "DROP TABLE IF EXISTS bin_orders CASCADE" >/dev/null
  psql_dst "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance CASCADE" >/dev/null

  CHUNK=${CHUNK_ROWS:-1000}
  cat > "$JOB" <<YAML
version: 1
job: b4-mysql-binpk
source:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_SRC_DSN
  scope:
    schema: $SRC_DB
  snapshot:
    chunk_rows: $CHUNK
target:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_DSN
  schema: public
  on_exists: append
content:
  tables:
    bin_orders: {}
mode:
  frequency: continuous+initial
reliability:
  batch_rows: 5000
YAML
  echo "== 3. 导入任务 =="
  "$BIN" import --file "$JOB" --data-dir "$DATA" | tail -2

  echo "== 4. 全量中途 kill -9 =="
  "$BIN" run --job b4-mysql-binpk --data-dir "$DATA" --stop-after 600s > "$WORK/run1.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 400); do
    grep -q "第 2 块" "$WORK/run1.log" 2>/dev/null && break
    sleep 0.05
  done
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  MID=$(psql_dst "SELECT count(*) FROM bin_orders" 2>/dev/null || echo 0)
  echo "   已 kill -9；目标端此刻 $MID 行"
  echo "   残留状态：$(psql_dst "SELECT phase||' '||COALESCE(position,'(无位点)') FROM _jl_meta" 2>/dev/null || echo 无)"
  echo "   块进度里最后一条边界：$(psql_dst "SELECT COALESCE(range_to,'(空)') FROM _jl_chunk ORDER BY chunk_no DESC LIMIT 1" 2>/dev/null || echo 无)"

  echo "== 5. 重启：从已提交的块边界续跑 =="
  timeout 600 "$BIN" run --job b4-mysql-binpk --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run2.log" 2>&1 || true
  grep -E "续跑|全量完成|清空了上次" "$WORK/run2.log" | head -3 || true
  RESUME_ROWS=$(grep -oE "全量完成：[0-9]+ 行" "$WORK/run2.log" | tail -1 | grep -oE "[0-9]+" || echo 0)
  TOTAL=$(psql_dst "SELECT count(*) FROM bin_orders")
  echo "   中断行数 $MID / 续跑行数 $RESUME_ROWS / 最终 $TOTAL 行（应为 $ROWS）"

  echo "== 6. 对比（行数 + 内容指纹）=="
  SRC_FP=$(fp_bin_mysql); DST_FP=$(fp_bin_pg)
  echo "   源端 $SRC_FP"
  echo "   目标 $DST_FP"
  if [ "$TOTAL" != "$ROWS" ]; then
    echo "FAIL: 续跑之后少了 $((ROWS - TOTAL)) 行——块边界丢类型会让续跑跳到表尾（静默少行）"
    exit 1
  fi
  [ "$SRC_FP" = "$DST_FP" ] || { echo "FAIL: 源与目标指纹不一致"; exit 1; }
  echo "PASS: BINARY(16) 主键续跑，中断 $MID 行 → 最终 $TOTAL 行，指纹一致 $SRC_FP"
  exit 0
fi

echo "== 1. 造数：MySQL 源端 $ROWS 行 =="
mysql_src "DROP TABLE IF EXISTS orders"
mysql_src "CREATE TABLE orders (
             id         BIGINT NOT NULL PRIMARY KEY,
             payload    VARCHAR(64) NOT NULL,
             amount     DECIMAL(12,2) NULL,
             created_at DATETIME(6) NULL
           ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
# 分批插，避免一条语句太大
STEP=${INSERT_STEP:-20000}
i=1
while [ "$i" -le "$ROWS" ]; do
  hi=$((i + STEP - 1))
  [ "$hi" -gt "$ROWS" ] && hi=$ROWS
  mysql_src "SET SESSION cte_max_recursion_depth=1000000;
             INSERT INTO orders(id,payload,amount,created_at)
             SELECT g, CONCAT('row-', g), (g % 10000) / 100,
                    TIMESTAMPADD(SECOND, -g, '2026-01-01 00:00:00')
             FROM (WITH RECURSIVE seq AS (SELECT $i AS n UNION ALL SELECT n+1 FROM seq WHERE n < $hi)
                   SELECT n FROM seq) s(g)"
  i=$((hi + 1))
done
echo "   源端指纹 $(fp_mysql)"

echo "== 2. 清空目标端与残留状态 =="
psql_dst "DROP TABLE IF EXISTS orders CASCADE" >/dev/null
psql_dst "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance CASCADE" >/dev/null

CHUNK=${CHUNK_ROWS:-50000}
FREQ=continuous+initial
[ "$MODE" = "once" ] && FREQ=once
cat > "$JOB" <<YAML
version: 1
job: b4-mysql-orders
source:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_SRC_DSN
  scope:
    schema: $SRC_DB
  snapshot:
    chunk_rows: $CHUNK
target:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_DSN
  schema: public
  on_exists: append
content:
  tables:
    orders: {}
mode:
  frequency: $FREQ
reliability:
  batch_rows: 5000
YAML

echo "== 3. 导入任务 =="
"$BIN" import --file "$JOB" --data-dir "$DATA"

if [ "$MODE" = "kill" ]; then
  echo "== 4. 全量中途 kill -9 =="
  "$BIN" run --job b4-mysql-orders --data-dir "$DATA" --stop-after 600s > "$WORK/run1.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 400); do
    grep -q "第 2 块" "$WORK/run1.log" 2>/dev/null && break
    sleep 0.05
  done
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  MID=$(psql_dst "SELECT count(*) FROM orders" 2>/dev/null || echo 0)
  echo "   已 kill -9（快照进行中）；目标端此刻 $MID 行"
  echo "   残留状态：$(psql_dst "SELECT phase||' '||COALESCE(position,'(无位点)') FROM _jl_meta" 2>/dev/null || echo 无)"
  # 停机期间源端删掉一行（已在第 1 块里搬过去）：续跑不会重读那一段，
  # 这一行只能靠「增量从原来的位点重放」删除。
  mysql_src "DELETE FROM orders WHERE id=5"
  echo "   停机期间源端删除了 id=5（重启后必须消失）"
  echo "== 5. 重启：识别「全量没搬完」，从已提交的块边界续跑 =="
  timeout 600 "$BIN" run --job b4-mysql-orders --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run2.log" 2>&1 || true
  grep -E "续跑|全量完成|跳过全量|清空了上次" "$WORK/run2.log" | head -3 || true
  RESUME_ROWS=$(grep -oE "全量完成：[0-9]+ 行" "$WORK/run2.log" | tail -1 | grep -oE "[0-9]+" || echo 0)
  if [ "$MID" -ge "$ROWS" ]; then
    echo "   注意：kill 落在全量完成之后（中断时目标端已有 $MID 行 = 全部），这一轮没有测到「续跑」"
  else
    echo "   中断行数 $MID / 续跑行数 $RESUME_ROWS / 预期总数 $((ROWS - 1))（源端停机期间删了 id=5）"
  fi
elif [ "$MODE" = "once" ]; then
  echo "== 4. once：只搬存量 =="
  "$BIN" run --job b4-mysql-orders --data-dir "$DATA" --verify > "$WORK/run1.log" 2>&1
  grep -E "全量完成|校验" "$WORK/run1.log" | tail -3 || true
  SRC_FP=$(fp_mysql); DST_FP=$(fp_pg)
  echo "   源端 $SRC_FP"
  echo "   目标 $DST_FP"
  [ "$SRC_FP" = "$DST_FP" ] || { echo "FAIL: once 搬完之后源与目标不一致"; exit 1; }
  echo "PASS: MySQL → PG，$ROWS 行，once 模式，指纹一致 $SRC_FP"
  exit 0
else
  echo "== 4. 第一次跑（全量 → 增量 → 收尾）=="
  "$BIN" run --job b4-mysql-orders --data-dir "$DATA" --stop-after 10s > "$WORK/run1.log" 2>&1 || true
  grep -E "全量完成|增量：|追平" "$WORK/run1.log" | tail -4 || true
  SRC_FP=$(fp_mysql); DST_FP=$(fp_pg)
  echo "   全量后 源端 $SRC_FP / 目标 $DST_FP"
  [ "$SRC_FP" = "$DST_FP" ] || { echo "FAIL: 全量之后源与目标不一致"; exit 1; }
fi

echo "== 6. 增量：插入 / 更新 / 删除各一笔，量追平时延 =="
mysql_src "INSERT INTO orders(id,payload,amount,created_at) VALUES ($ROWS+1,'inserted-after-snapshot',1.23,NOW(6))"
mysql_src "UPDATE orders SET payload='updated-after-snapshot', amount=9.87 WHERE id=2"
mysql_src "DELETE FROM orders WHERE id=3"
T0=$(date +%s.%N)
# 后台跑增量；轮询目标端直到三笔都到齐（追平 = 目标端看到源端的最新状态）
"$BIN" run --job b4-mysql-orders --data-dir "$DATA" --stop-when-idle 3s > "$WORK/run3.log" 2>&1 &
RUNPID=$!
LAT=""
for _ in $(seq 1 600); do
  GOT=$(psql_dst "SELECT
      (SELECT count(*) FROM orders WHERE id=$((ROWS+1)))::text || ',' ||
      (SELECT count(*) FROM orders WHERE id=2 AND payload='updated-after-snapshot')::text || ',' ||
      (SELECT count(*) FROM orders WHERE id=3)::text" 2>/dev/null || echo "0,0,1")
  if [ "$GOT" = "1,1,0" ]; then
    T1=$(date +%s.%N)
    LAT=$(python3 -c "print(f'{$T1-$T0:.2f}')")
    break
  fi
  sleep 0.1
done
wait "$RUNPID" 2>/dev/null || true
if [ -z "$LAT" ]; then
  echo "FAIL: 增量 60 秒内没追平（最后一次：${GOT:-无}）"
  echo "--- 增量那一轮的日志 ---"
  grep -E "DBG|增量|error" "$WORK/run3.log" | head -20
  tail -10 "$WORK/run3.log"
  tail -20 "$WORK/run3.log"
  exit 1
fi
echo "   追平时延 ${LAT}s（源端三笔改动 → 目标端全部可见）"

echo "== 7. 对比（行数 + 内容指纹）=="
SRC_FP=$(fp_mysql); DST_FP=$(fp_pg)
echo "   源端 $SRC_FP"
echo "   目标 $DST_FP"
[ "$SRC_FP" = "$DST_FP" ] || { echo "FAIL: 源与目标不一致（应当不重不丢）"; exit 1; }

echo "== 8. status 一眼看 =="
"$BIN" status --job b4-mysql-orders --data-dir "$DATA" || true

echo "PASS: MySQL → PG，$ROWS 行，模式 $MODE，指纹一致 $SRC_FP，追平时延 ${LAT}s"
