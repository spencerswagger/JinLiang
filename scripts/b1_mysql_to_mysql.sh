#!/usr/bin/env bash
# B4 验收：MySQL 源 → MySQL 目标端的真链路（全量 → 增量 → 崩溃恢复）。
#
#   ROWS=200000 scripts/b1_mysql_to_mysql.sh ./dist/jinliang           # 全量 → 增量
#   ROWS=200000 scripts/b1_mysql_to_mysql.sh ./dist/jinliang kill      # 增量跑到一半 kill -9 → 重启
#   ROWS=200000 scripts/b1_mysql_to_mysql.sh ./dist/jinliang killcopy  # 全量中途 kill -9 → 块级续跑
#
# 需要两个 **不同的 MySQL 实例**（源库一个、目标库一个）：
#   JINLIANG_TEST_MYSQL_SRC_DSN  源
#   JINLIANG_TEST_MYSQL_DST_DSN  目标（没设就用 JINLIANG_TEST_MYSQL_DSN）
# 缺任何一个都以 78（EX_CONFIG）退出——CI 里这是红的，不是跳过。
#
# 为什么必须两个实例：源库与目标库在同一个实例上时，目标端的写入（含同步自己的位点表）
# 会同时进源端的 binlog，等于「读到自己刚写的行」。这不是我们承诺支持的拓扑，
# 但脚本必须把这件事说出来，而不是等用户遇到一个看起来像 bug 的列名错误。
set -euo pipefail

BIN=${1:-./dist/jinliang}
MODE=${2:-full}
ROWS=${ROWS:-200000}
SRC=${JINLIANG_TEST_MYSQL_SRC_DSN:-}
DST=${JINLIANG_TEST_MYSQL_DST_DSN:-${JINLIANG_TEST_MYSQL_DSN:-}}

if [ -z "$SRC" ] || [ -z "$DST" ]; then
  echo "EX_CONFIG: 缺 JINLIANG_TEST_MYSQL_SRC_DSN / JINLIANG_TEST_MYSQL_DST_DSN（MySQL 源 → MySQL 目标）" >&2
  exit 78
fi
command -v mysql >/dev/null 2>&1 || { echo "EX_CONFIG: 没有 mysql 客户端" >&2; exit 78; }

# mysql://user:pass@host:port/db → 客户端参数
split_dsn() { # $1=dsn，输出 "user pass host port db"
  local s=${1#mysql://} creds rest hostport db
  creds=${s%%@*}; rest=${s#*@}
  hostport=${rest%%/*}; db=${rest#*/}; db=${db%%\?*}
  local user=${creds%%:*} pass=${creds#*:}
  [ "$pass" = "$creds" ] && pass=""
  local host=${hostport%%:*} port=${hostport#*:}
  [ "$port" = "$hostport" ] && port=3306
  echo "$user $pass $host $port $db"
}
read -r SUSER SPASS SHOST SPORT SDB <<<"$(split_dsn "$SRC")"
read -r DUSER DPASS DHOST DPORT DDB <<<"$(split_dsn "$DST")"

if [ "$SHOST:$SPORT" = "$DHOST:$DPORT" ]; then
  echo "注意：源库与目标库在同一个实例（$SHOST:$SPORT）。目标端的写入会进源端 binlog，"
  echo "      这条拓扑不在本期承诺内（增量会读到自己刚写的行）。请给目标库另一个实例。"
  exit 78
fi

my() { # $1=host $2=port $3=user $4=pass $5=db $6=sql
  mysql --protocol=TCP -h "$1" -P "$2" -u "$3" "--password=$4" --batch --skip-column-names \
        -e "$6" "$5" 2>/dev/null
}
src() { my "$SHOST" "$SPORT" "$SUSER" "$SPASS" "$SDB" "$1"; }
dst() { my "$DHOST" "$DPORT" "$DUSER" "$DPASS" "$DDB" "$1"; }

# 指纹：两端同一段 SQL（都是 MySQL），行数 + 按主键排序逐行 md5 再 md5。
fp() { # $1=src|dst
  local q="SET SESSION group_concat_max_len=1073741824;
    SELECT CONCAT(COUNT(*), ':', COALESCE(MD5(GROUP_CONCAT(h ORDER BY id SEPARATOR '')), '-'))
    FROM (SELECT id, MD5(CONCAT_WS('|', id, payload, amount,
             DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'))) h FROM m2m_orders) x"
  if [ "$1" = src ]; then src "$q"; else dst "$q"; fi
}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
JOB="$WORK/job.yaml"
DATA="$WORK/data"

echo "== 1. 造数：源端 $ROWS 行 =="
src "DROP TABLE IF EXISTS m2m_orders"
src "CREATE TABLE m2m_orders (
       id         BIGINT NOT NULL PRIMARY KEY,
       payload    VARCHAR(64) NOT NULL,
       amount     DECIMAL(12,2) NULL,
       created_at DATETIME(6) NULL
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
STEP=${INSERT_STEP:-20000}
i=1
while [ "$i" -le "$ROWS" ]; do
  hi=$((i + STEP - 1)); [ "$hi" -gt "$ROWS" ] && hi=$ROWS
  src "SET SESSION cte_max_recursion_depth=1000000;
       INSERT INTO m2m_orders(id,payload,amount,created_at)
       SELECT n, CONCAT('row-', n), (n % 10000) / 100,
              TIMESTAMPADD(SECOND, -n, '2026-01-01 00:00:00')
       FROM (WITH RECURSIVE seq AS (SELECT $i AS n UNION ALL SELECT n+1 FROM seq WHERE n < $hi)
             SELECT n FROM seq) s"
  i=$((hi + 1))
done
echo "   源端指纹 $(fp src)"

echo "== 2. 清空目标端与残留状态 =="
dst "DROP TABLE IF EXISTS m2m_orders"
dst "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance"

cat > "$JOB" <<YAML
version: 1
job: b4-mysql-to-mysql
source:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_SRC_DSN
  scope:
    schema: $SDB
  snapshot:
    chunk_rows: ${CHUNK_ROWS:-50000}
target:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_DST_DSN
  schema: $DDB
  on_exists: append
content:
  tables:
    m2m_orders: {}
mode:
  frequency: continuous+initial
reliability:
  batch_rows: 5000
YAML

echo "== 3. 导入任务 =="
# 任务里的 dsn 写成 env: 引用，所以跑的时候这两个变量必须在环境里。
export JINLIANG_TEST_MYSQL_SRC_DSN=$SRC
export JINLIANG_TEST_MYSQL_DST_DSN=$DST
"$BIN" import --file "$JOB" --data-dir "$DATA" | tail -2

echo "== 4. 第一轮：全量 → 增量 =="
T0=$(date +%s.%N)
if [ "$MODE" = "killcopy" ]; then
  "$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-after 600s > "$WORK/run1.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 600); do
    grep -q "第 2 块" "$WORK/run1.log" 2>/dev/null && break
    sleep 0.05
  done
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  MID=$(dst "SELECT COUNT(*) FROM m2m_orders" || echo 0)
  echo "   全量中途 kill -9：目标端此刻 $MID/$ROWS 行"
  echo "   块进度最后一条边界：$(dst "SELECT COALESCE(range_to,'(空)') FROM _jl_chunk ORDER BY chunk_no DESC LIMIT 1" || echo 无)"
  timeout 600 "$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run2.log" 2>&1 || true
  RESUME_ROWS=$(grep -oE "复制 [0-9]+ 行" "$WORK/run2.log" | tail -1 | grep -oE "[0-9]+" || echo 0)
  echo "   续跑重搬 $RESUME_ROWS 行（应为 $((ROWS - MID)) 左右，即只搬没搬完的）"
  T1=$(date +%s.%N)
  echo "   续跑耗时 $(python3 -c "print(f'{$T1-$T0:.2f}')")s"
  S=$(fp src); D=$(fp dst)
  echo "   源端 $S / 目标 $D"
  [ "$S" = "$D" ] || { echo "FAIL: 块级续跑之后两端不一致"; exit 1; }
  echo "PASS: MySQL → MySQL 全量中途 kill -9，续跑后两端一致 $S"
  exit 0
fi

T0=$(date +%s.%N)
"$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run1.log" 2>&1 || true
T1=$(date +%s.%N)
grep -E "全量完成|已追平|实时同步" "$WORK/run1.log" | tail -3 || true
FULL_ELAPSED=$(python3 -c "print(f'{$T1-$T0:.2f}')")
S=$(fp src); D=$(fp dst)
echo "   全量+追平耗时 ${FULL_ELAPSED}s；源端 $S / 目标 $D"
[ "$S" = "$D" ] || { echo "FAIL: 全量之后两端不一致"; exit 1; }

echo "== 5. 增量：插入 / 更新 / 删除各一笔，量追平时延 =="
src "INSERT INTO m2m_orders(id,payload,amount,created_at) VALUES ($((ROWS+1)),'inserted-after-snapshot',1.23,NOW(6))"
src "UPDATE m2m_orders SET payload='updated-after-snapshot', amount=9.87 WHERE id=2"
src "DELETE FROM m2m_orders WHERE id=3"

if [ "$MODE" = "kill" ]; then
  # 增量跑到一半 kill -9：在「流已经搬了一段、还没追平」的窗口里杀。
  "$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-after 600s > "$WORK/incr.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 600); do
    N=$(dst "SELECT COUNT(*) FROM m2m_orders WHERE id=$((ROWS+1))" 2>/dev/null || echo 0)
    [ "$N" = "1" ] && break
    sleep 0.05
  done
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  echo "   增量中途 kill -9（三笔里已到目标端的：$(dst "SELECT CONCAT((SELECT COUNT(*) FROM m2m_orders WHERE id=$((ROWS+1))),',',(SELECT COUNT(*) FROM m2m_orders WHERE id=2 AND payload='updated-after-snapshot'),',',(SELECT COUNT(*) FROM m2m_orders WHERE id=3))" || echo '?')）"
  # 停机期间再改三笔：只能靠「重启后从落库位点重放」补上，全量重搬补不了（视图更早）。
  src "INSERT INTO m2m_orders(id,payload,amount,created_at) VALUES ($((ROWS+2)),'written-while-down',3.45,NOW(6))"
  src "UPDATE m2m_orders SET payload='touched-while-down' WHERE id BETWEEN 10 AND 20"
  src "DELETE FROM m2m_orders WHERE id BETWEEN 30 AND 34"
  T0=$(date +%s.%N)
  "$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run3.log" 2>&1 || true
  T1=$(date +%s.%N)
  REBUILD=$(grep -oE "复制 [0-9]+ 行" "$WORK/run3.log" | tail -1 | grep -oE "[0-9]+" || echo 0)
  echo "   重启：重搬 $REBUILD 行（必须是 0）、耗时 $(python3 -c "print(f'{$T1-$T0:.2f}')")s"
  [ "$REBUILD" = "0" ] || { echo "FAIL: 重启后又搬了 $REBUILD 行存量——这是整轮重搬，不是位点续跑"; exit 1; }
  S=$(fp src); D=$(fp dst)
  echo "   源端 $S / 目标 $D"
  [ "$S" = "$D" ] || { echo "FAIL: 崩溃恢复之后两端不一致（停机期间的改动没追回）"; exit 1; }
  echo "PASS: MySQL → MySQL 增量中途 kill -9，重启重搬 0 行，两端一致 $S"
  exit 0
fi

T0=$(date +%s.%N)
"$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-when-idle 3s > "$WORK/run2.log" 2>&1 || true
LAT=""
for _ in $(seq 1 600); do
  GOT=$(dst "SELECT CONCAT((SELECT COUNT(*) FROM m2m_orders WHERE id=$((ROWS+1))),',',
      (SELECT COUNT(*) FROM m2m_orders WHERE id=2 AND payload='updated-after-snapshot'),',',
      (SELECT COUNT(*) FROM m2m_orders WHERE id=3))" 2>/dev/null || echo "0,0,1")
  if [ "$GOT" = "1,1,0" ]; then
    T1=$(date +%s.%N); LAT=$(python3 -c "print(f'{$T1-$T0:.2f}')"); break
  fi
  sleep 0.1
done
[ -n "$LAT" ] || { echo "FAIL: 增量没有追平"; tail -10 "$WORK/run2.log"; exit 1; }
echo "   追平时延 ${LAT}s"

S=$(fp src); D=$(fp dst)
echo "   源端 $S / 目标 $D"
[ "$S" = "$D" ] || { echo "FAIL: 增量之后两端不一致"; exit 1; }
echo "PASS: MySQL → MySQL，$ROWS 行，全量 ${FULL_ELAPSED}s，增量追平 ${LAT}s，指纹一致 $S"
