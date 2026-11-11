#!/usr/bin/env bash
# B4 验收：MySQL 源 → MySQL 目标端的真链路（全量 → 增量 → 崩溃恢复）。
#
#   ROWS=200000 scripts/b1_mysql_to_mysql.sh ./dist/jinliang           # 全量 → 增量
#   ROWS=200000 scripts/b1_mysql_to_mysql.sh ./dist/jinliang kill      # 增量跑到一半 kill -9 → 重启
#   ROWS=200000 scripts/b1_mysql_to_mysql.sh ./dist/jinliang killcopy  # 全量中途 kill -9 → 块级续跑
#   ROWS=200000 scripts/b1_mysql_to_mysql.sh ./dist/jinliang accounting # 块记账本身（阻塞判据；不需要续跑接线）
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
GC="$(cd "$(dirname "$0")" && pwd)/gate_checks.py"
# killcopy 要在「块」这一级证伪：表 2 万行、块 2000 行 → 至少 8 个块，
# kill 之后_已提交的块_不许重搬。块太大（默认 5 万）时一张表只有一个块，
# 这一格就退化成「整表重搬」也看不出来。
if { [ "$MODE" = "killcopy" ] || [ "$MODE" = "accounting" ]; } && [ -z "${CHUNK_ROWS:-}" ]; then
  CHUNK_ROWS=2000
fi
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
# ── 块记账本身（阻塞判据）───────────────────────────────────────
# 只验「目标端有没有认真记账」，不依赖引擎的续跑读路径——后者还没接（Core）。
# 记账契约（Data #95）：搬运中每个块一行（chunk_no>0、done=1、rows=该块行数）；
# 表搬完后写 chunk_no=0 的表标记，且它的 rows **必须等于各块之和**（= 表行数）。
if [ "$MODE" = "accounting" ]; then
  "$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-after 600s > "$WORK/run1.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 600); do
    grep -q "第 2 块" "$WORK/run1.log" 2>/dev/null && break
    sleep 0.05
  done
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true

  ROWS_IN=$(dst "SELECT COUNT(*) FROM m2m_orders" 2>/dev/null || echo 0)
  BLOCKS=$(dst "SELECT COUNT(*) FROM _jl_chunk WHERE chunk_no>0 AND done=1" 2>/dev/null || echo 0)
  ROWS_DONE=$(dst "SELECT COALESCE(SUM(\`rows\`),0) FROM _jl_chunk WHERE chunk_no>0 AND done=1" 2>/dev/null || echo 0)
  echo "   全量中途 kill -9：目标端 $ROWS_IN/$ROWS 行；已提交块 ${BLOCKS} 个 / ${ROWS_DONE} 行"

  # ① 搬运中必须逐块记账
  if [ "${BLOCKS:-0}" -lt 1 ]; then
    echo "FAIL: 没有一个 done=1 的块——搬运期块记账没写（记账口径①）"; exit 1
  fi
  # ② 记账不许虚报（块行数合计不能超过目标端实有行数）
  if [ "${ROWS_DONE:-0}" -gt "${ROWS_IN:-0}" ]; then
    echo "FAIL: 已提交块合计 ${ROWS_DONE} 行 > 目标端实有 ${ROWS_IN} 行（记账虚报）"; exit 1
  fi

  # ③ 跑完一轮：表标记 chunk_no=0 必须在，且 rows == 各块之和 == 表行数
  timeout 600 "$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run2.log" 2>&1 || true
  TBL_DONE=$(dst "SELECT COUNT(*) FROM _jl_chunk WHERE chunk_no=0 AND done=1" 2>/dev/null || echo 0)
  TBL_ROWS=$(dst "SELECT COALESCE(MAX(\`rows\`),0) FROM _jl_chunk WHERE chunk_no=0 AND done=1" 2>/dev/null || echo 0)
  SUM_BLOCKS=$(dst "SELECT COALESCE(SUM(\`rows\`),0) FROM _jl_chunk WHERE chunk_no>0" 2>/dev/null || echo 0)
  ALL_ROWS=$(dst "SELECT COUNT(*) FROM m2m_orders" 2>/dev/null || echo 0)
  echo "   跑完后：表标记 ${TBL_DONE}（rows=${TBL_ROWS}），各块之和 ${SUM_BLOCKS}，表行数 ${ALL_ROWS}"
  if [ "${TBL_DONE:-0}" -lt 1 ]; then
    echo "FAIL: 表搬完了却没有 chunk_no=0 的完成标记（记账口径③）"; exit 1
  fi
  if [ "${TBL_ROWS:-0}" -ne "${SUM_BLOCKS:-0}" ]; then
    echo "FAIL: 表标记 rows=${TBL_ROWS} ≠ 各块之和 ${SUM_BLOCKS}（记账口径③）"; exit 1
  fi
  if [ "${ALL_ROWS:-0}" -ne "$ROWS" ] || [ "${TBL_ROWS:-0}" -ne "$ROWS" ]; then
    echo "FAIL: 表标记 ${TBL_ROWS} / 实际 ${ALL_ROWS}，应为 $ROWS"; exit 1
  fi
  echo "PASS: 块记账成立（搬运期 ${BLOCKS} 块 / ${ROWS_DONE} 行；完成后表标记 rows=${TBL_ROWS} = 各块之和 = 表行数）"
  exit 0
fi

if [ "$MODE" = "killcopy" ]; then
  "$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-after 600s > "$WORK/run1.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 600); do
    grep -q "第 2 块" "$WORK/run1.log" 2>/dev/null && break
    sleep 0.05
  done
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  MID=$(dst "SELECT COUNT(*) FROM m2m_orders" 2>/dev/null || echo 0)
  CHUNKROWS=$(dst "SELECT COUNT(*) FROM _jl_chunk" 2>/dev/null || echo 0)
  CHUNKLAST=$(dst "SELECT COALESCE(range_to,'(空)') FROM _jl_chunk ORDER BY chunk_no DESC LIMIT 1" 2>/dev/null || echo 无)
  echo "   全量中途 kill -9：目标端此刻 $MID/$ROWS 行（块 ${CHUNK_ROWS:-50000} 行/块）"
  echo "   _jl_chunk 行数：$CHUNKROWS；最后一条边界：$CHUNKLAST"

  # ── 判据 1：块记账必须存在 ─────────────────────────────────────
  # 没有块记账时，重启只能整轮重搬，而旧脚本只比「两端最终一致」——整轮重搬也能一致，
  # 于是这一格长期是假绿（kill 在 4000/20000 行时 _jl_chunk 0 行、重启又搬 20000 行，仍 PASS）。
  if [ "${CHUNKROWS:-0}" -le 0 ]; then
    echo "FAIL: _jl_chunk 里 0 行 —— 目标端没写块记账（mysqlsink 的块级续跑还没接上）。"
    echo "      这一格不能算通过：没有块边界，'续跑' 只能是整轮重搬（旧脚本正因此假绿）。"
    echo "      等块记账落地后这一格会自动变绿；在此之前它**必须红**。"
    exit 1
  fi

  timeout 600 "$BIN" run --job b4-mysql-to-mysql --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run2.log" 2>&1 || true
  # 重启这一轮搬了多少行：generic 路径的收尾行是「复制 N 行」。
  RESUME_ROWS=$(grep -oE "复制 [0-9]+ 行" "$WORK/run2.log" | tail -1 | grep -oE "[0-9]+" || echo 0)
  EXPECT=$((ROWS - MID))                 # 没搬完、必须补的行数
  echo "   中断 $MID 行 → 重启搬 $RESUME_ROWS 行（未搬完 $EXPECT 行；口径②=单表续跑）"
  # ── 判据 2（单表口径）：只允许「中断时正在写的那一块」回滚重来 ─────────────
  # 注意：这不是「重启重搬 0 行」（那是整轮口径，用在 MySQL→PG 的 kill 那格）。
  python3 "$GC" recopy --scope table --resumed "$RESUME_ROWS" --remaining "$EXPECT" \
    --chunk "${CHUNK_ROWS:-50000}" || {
    echo "FAIL: 重启搬 $RESUME_ROWS 行，未搬完 $EXPECT 行（±1 块 ${CHUNK_ROWS:-50000}）：这不是块级续跑。"
    echo "--- run2.log 尾部 ---"; tail -20 "$WORK/run2.log"; exit 1
  }

  T1=$(date +%s.%N)
  echo "   续跑耗时 $(python3 -c "print(f'{$T1-$T0:.2f}')")s"
  S=$(fp src); D=$(fp dst)
  echo "   源端 $S / 目标 $D"
  [ "$S" = "$D" ] || { echo "FAIL: 块级续跑之后两端不一致"; exit 1; }
  echo "PASS: MySQL → MySQL 全量中途 kill -9，块进度 $CHUNKROWS 行，单表续跑（重启搬 $RESUME_ROWS ≈ 未搬完 $EXPECT ±1 块），两端一致 $S"
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
