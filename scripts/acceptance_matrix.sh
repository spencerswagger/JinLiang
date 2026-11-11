#!/usr/bin/env bash
# 三组合验收矩阵（可重复执行）：MySQL→PG / PG→MySQL / MySQL→MySQL。
#
#   scripts/acceptance_matrix.sh                                   # 全部三组合
#   scripts/acceptance_matrix.sh --only mysql-postgres --rows 20000
#   scripts/acceptance_matrix.sh --kill                            # 追加 kill -9 续跑（MySQL→PG）
#   scripts/acceptance_matrix.sh --json .localdb/acceptance.json
#   scripts/acceptance_matrix.sh --logdir notes/matrix3            # 原始日志落盘
#
# 每格报：目标表名 / 源行数 / 目标行数 / 源 md5 / 目标 md5 / **快照段秒数 / 增量段秒数**。
# 判据本身走
# `scripts/gate_checks.py`（行数、指纹、0 行重搬），所以「比对被改松」会被它的
# selftest + `scripts/mutation_gate.py` 抓住，而不是靠这里再写一遍 if。
#
# 环境（缺了就是 78 = 红，不静默跳过）：
#   JINLIANG_TEST_MYSQL_SRC_DSN   MySQL 源      （mysql→*）
#   JINLIANG_TEST_MYSQL_DST_DSN   MySQL 目标    （*→mysql）
#   JINLIANG_TEST_PG_SRC_DSN      PG 源         （pg→mysql）
#   JINLIANG_TEST_PG_DSN          PG 目标       （mysql→pg）
#   MYSQL_VERSION                 标签用（如 5.7 / 8.0 / 8.4）；不填就问服务器
#
# 输出：每格一行 `RESULT combo=… rows=… md5=… seconds=… status=…`，最后一张 markdown 表。
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
GC="$HERE/gate_checks.py"
EXITC="$HERE/exit_codes.py"
BIN=${BIN:-$ROOT/.localdb/bin/jinliang}
ROWS=${ROWS:-20000}
KILL=0
ONLY=""
JSON=""
LOG_DIR=""

while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="${2:-}"; shift 2 ;;
    --rows) ROWS="${2:-}"; shift 2 ;;
    --kill) KILL=1; shift ;;
    --json) JSON="${2:-}"; shift 2 ;;
    --logdir) LOG_DIR="${2:-}"; shift 2 ;;
    --bin) BIN="${2:-}"; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
    *) echo "acceptance: 不认识的参数 $1" >&2; exit 2 ;;
  esac
done

command -v mysql >/dev/null || { echo "EX_CONFIG: 没有 mysql 客户端" >&2; exit 78; }
command -v psql >/dev/null || { echo "EX_CONFIG: 没有 psql 客户端" >&2; exit 78; }
[ -x "$BIN" ] || { echo "EX_CONFIG: 没有可执行文件 $BIN（先 go build -o $BIN ./cmd/jinliang）" >&2; exit 78; }

RESULTS=()
FAILED=0

# ── DSN 拆解 ─────────────────────────────────────────────────
split_mysql() { # $1=mysql://u:p@h:port/db → 全局 MUSER MPASS MHOST MPORT MDB
  local s=${1#mysql://} creds rest hp db
  creds=${s%%@*}; rest=${s#*@}; hp=${rest%%/*}; db=${rest#*/}; db=${db%%\?*}
  MUSER=${creds%%:*}; MPASS=${creds#*:}; [ "$MPASS" = "$creds" ] && MPASS=""
  MHOST=${hp%%:*}; MPORT=${hp#*:}; [ "$MPORT" = "$hp" ] && MPORT=3306
  MDB=$db
}
# 所有外部命令都带 timeout：CI 上「某一步卡住」会变成一条明确的失败，而不是把 job
# 拖到 6 小时超时（第一次跑就是这么丢的日志——in_progress 永远看不到输出）。
myq() { # $1=sql（用当前 split 出来的连接）
  timeout 60 mysql --protocol=TCP -h "$MHOST" -P "$MPORT" -u"$MUSER" "--password=$MPASS" \
        --batch --skip-column-names -e "$1" "$MDB" 2>/dev/null
}
pgq() { timeout 30 psql "$1" -v ON_ERROR_STOP=1 -tAq -c "$2" 2>/dev/null; }
runbin() { timeout 300 "$BIN" "$@"; }
# 退出码口径（失败 1 > 停止 130 > 有暂停表 3 > 0）：3 是**欠账不是失败**，报表要标出来。
classify_rc() { python3 "$EXITC" classify --code "$1" --log "$2" --kind-only 2>/dev/null; }
mark() { echo "-- $(date -u +%H:%M:%S) $*"; }

fp_mysql() { # $1=表名（当前连接）
  myq "SET SESSION group_concat_max_len=1073741824;
       SELECT CONCAT(COUNT(*),':',COALESCE(MD5(GROUP_CONCAT(h ORDER BY id SEPARATOR '')),'-'))
       FROM (SELECT id, MD5(CONCAT_WS('|', id, payload)) h FROM \`$1\`) x"
}
fp_pg() { # $1=dsn $2=表名
  pgq "$1" "SELECT count(*)::text||':'||COALESCE(md5(string_agg(h,'' ORDER BY id)),'-')
            FROM (SELECT id, md5(concat_ws('|', id, payload)) h FROM $2) x"
}

mysql_server_version() { myq "SELECT VERSION()" | head -1; }

# RESULT 行两侧都给：rows/md5/seconds 是旧字段（CI/日报还在读），保留；
# 新字段 src_rows/dst_rows/src_md5/dst_md5/snapshot_seconds/incremental_seconds 是矩阵要的。
emit() { # combo 目标表 源行 目标行 源md5 目标md5 快照s 增量s status extra
  local total; total=$(add_s "$7" "$8")
  local line="RESULT combo=$1 target=$2 rows=$4 md5=$6 seconds=$total"
  line="$line src_rows=$3 dst_rows=$4 src_md5=$5 dst_md5=$6"
  line="$line snapshot_seconds=$7 incremental_seconds=$8 status=$9 ${10:-}"
  echo "$line"
  RESULTS+=("$line")
  [ "$9" = "pass" ] || FAILED=$((FAILED + 1))
}

# 给了 --logdir 就把每格的原始日志落在那儿（不落 /tmp：execute 每次换沙盒，/tmp 不跨调用）。
workdir() { # $1=组合名
  if [ -n "${LOG_DIR:-}" ]; then mkdir -p "$LOG_DIR/$1"; printf '%s' "$LOG_DIR/$1"; else mktemp -d; fi
}

# 造数：MySQL 侧（id 1..N，payload=row-N）。
#
# **不用 CTE**：MySQL 5.7 没有 WITH RECURSIVE（8.0 才有），而矩阵要在 5.7 上跑。
# 用一个 10 万行的数字表（5 层 0..9 交叉连接）代替——5.7 / 8.0 / 8.4 都能跑。
nums_mysql() { # 幂等：建好 _acc_nums（100000 行）
  myq "CREATE TABLE IF NOT EXISTS _acc_nums (n INT NOT NULL PRIMARY KEY) ENGINE=InnoDB"
  if [ "$(myq "SELECT COUNT(*) FROM _acc_nums")" != "100000" ]; then
    myq "DROP TABLE IF EXISTS _acc_nums"
    myq "CREATE TABLE _acc_nums (n INT NOT NULL PRIMARY KEY) ENGINE=InnoDB"
    myq "INSERT INTO _acc_nums(n)
         SELECT a.i + b.i*10 + c.i*100 + d.i*1000 + e.i*10000 + 1
         FROM (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
               UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
              (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
               UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
              (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
               UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
              (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
               UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
              (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
               UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e"
  fi
}

seed_mysql() { # $1=表名 $2=行数
  myq "DROP TABLE IF EXISTS \`$1\`"
  myq "CREATE TABLE \`$1\` (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
  nums_mysql
  myq "INSERT INTO \`$1\`(id,payload) SELECT n, CONCAT('row-', n) FROM _acc_nums WHERE n <= $2 ORDER BY n"
}

seed_pg() { # $1=dsn $2=表名 $3=行数
  pgq "$1" "DROP TABLE IF EXISTS $2" >/dev/null
  pgq "$1" "CREATE TABLE $2 (id bigint PRIMARY KEY, payload text)" >/dev/null
  pgq "$1" "INSERT INTO $2 SELECT g, 'row-'||g FROM generate_series(1,$3) g" >/dev/null
}

now_s() { date +%s.%N; }
elapsed() { python3 -c "print(f'{$2-$1:.2f}')"; }

# ── 口径：报表里的两列时间、两侧指纹都按这里算 ──────────────────
# 指纹（跨端可比）：两端各自按主键升序，逐行算 md5(concat_ws('|', id, payload))，
#   把每行的 32 位十六进制拼成长串再取一次 md5；行数单列 count(*)。
#   这是**同一套算式在两端各算一次**，所以数字可以直接对——不是 verify 里那种
#   「跨端指纹 scope 不同、不可直接比」（docs/VERIFY.md §1）。整表导出取 md5 等价，
#   20k 行时逐行拼接更省内存，也能顺带报行数。
#   复算方法（任一端）：SELECT COUNT(*), MD5(GROUP_CONCAT(h ORDER BY id SEPARATOR ''))
#                       FROM (SELECT id, MD5(CONCAT_WS('|', id, payload)) h FROM t) x;   -- MySQL
#                       SELECT count(*), md5(string_agg(h,'' ORDER BY id))
#                       FROM (SELECT id, md5(concat_ws('|', id, payload)) h FROM t) x;   -- PG
# 耗时（两段分开，都用「追平」口径）：
#   快照段 = 首轮 run 从启动 → 目标端行数 == 源行数 的秒数；
#   增量段 = 源端写下三笔（增/改/删）→ 目标端三笔全部可见 的秒数。
#   两段都**不含**引擎 --stop-when-idle 的收尾等待——否则 300 行和 20000 行都显示 6.3s，
#   那不是搬运耗时，是「空闲 5 秒后收尾」的常量（第一版就是这么写的，一眼假）。
#   run 的墙钟单独记在 extra（snapshot_wall），要算总账时用它。
add_s() { python3 -c "print(f'{$1+$2:.2f}')"; }

# 交叉校验（第二判据）：md5 是「两端同一算式」，verify 是「应用侧逐行全表比对」。
# 两条独立路径都过才算这一格过。verify 的跨端指纹 scope 不同、不可比，所以这里
# 只跑 structure+data 级（data 级本身就是全表逐行，不是抽样）。
verify_cross() { # $1=源 DSN $2=目标 DSN $3=表名 $4=label $5=日志文件
  # DSN 含口令：命令行不打进日志，只留 verify 自己的输出。
  timeout 300 "$BIN" verify --source "$1" --target "$2" --tables "$3" --level data >"$5" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then
    echo "VERIFY FAIL: $4（rc=$rc）"; sed -n '1,40p' "$5"; return 1
  fi
  grep -E "结构级|数据级" "$5" | tail -2
  return 0
}

# ── 组合 1：MySQL → PG（全量 + 增量 + kill -9 续跑） ─────────────
combo_mysql_postgres() {
  local src=${JINLIANG_TEST_MYSQL_SRC_DSN:-} dst=${JINLIANG_TEST_PG_DSN:-}
  [ -n "$src" ] && [ -n "$dst" ] || { echo "EX_CONFIG: 缺 JINLIANG_TEST_MYSQL_SRC_DSN / JINLIANG_TEST_PG_DSN" >&2; return 78; }
  local T=acc_mp W; W=$(workdir mysql-postgres)
  split_mysql "$src"; local sdb=$MDB; local ver; ver=$(mysql_server_version)
  local job=$W/job.yaml data; data=$(mktemp -d)   # 引擎状态放 /tmp，notes 只留证据（不含 .db/密钥）

  seed_mysql "$T" "$ROWS"
  pgq "$dst" "DROP TABLE IF EXISTS $T CASCADE" >/dev/null
  pgq "$dst" "DROP TABLE IF EXISTS _jl_meta,_jl_chunk,_jl_lease,_jl_object,_jl_instance CASCADE" >/dev/null

  cat > "$job" <<YAML
version: 1
job: acc-mysql-pg
source:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_SRC_DSN
  scope: {schema: $sdb}
  snapshot: {chunk_rows: ${CHUNK_ROWS:-5000}}
target:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_DSN
  schema: public
  on_exists: append
content:
  tables:
    $T: {}
mode: {frequency: continuous+initial}
reliability: {batch_rows: 5000}
YAML
  timeout 120 "$BIN" import --file "$job" --data-dir "$data" >/dev/null 2>&1 || { echo "FAIL: import 失败"; return 1; }

  local t0 snap_seconds incr_seconds rc s_fp d_fp lat got pid KIND paused=0 wall
  mark "mysql→pg 快照开始（$ROWS 行）"
  t0=$(now_s)
  runbin run --job acc-mysql-pg --data-dir "$data" --stop-when-idle 5s >"$W/snapshot.log" 2>&1 &
  pid=$!
  snap_seconds=""
  for _ in $(seq 1 3000); do   # 上限 300s：搬不完就是红，不静默
    got=$(pgq "$dst" "SELECT count(*) FROM $T")
    [ "${got:-0}" = "$ROWS" ] && { snap_seconds=$(elapsed "$t0" "$(now_s)"); break; }
    sleep 0.1
  done
  wait $pid 2>/dev/null; rc=$?
  wall=$(elapsed "$t0" "$(now_s)")
  [ -n "$snap_seconds" ] || { echo "FAIL: 快照 300s 内没搬完（${got:-无}/$ROWS）"; tail -5 "$W/snapshot.log"; return 1; }
  KIND=$(classify_rc "$rc" "$W/snapshot.log")
  case "$KIND" in needs_action) paused=1 ;; esac
  case "$KIND" in
    ok|needs_action|stopped) : ;;  # 3 = 有暂停表（欠账），数据面照样校验
    *) echo "FAIL: 快照段退出码 $rc（$KIND）"; tail -5 "$W/snapshot.log"; return 1 ;;
  esac

  s_fp=$(fp_mysql "$T"); d_fp=$(fp_pg "$dst" "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "mysql→pg 快照" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }
  mark "mysql→pg 快照完成 ${snap_seconds}s"

  # 增量三笔（增/改/删）→ 追平时延
  myq "INSERT INTO \`$T\` VALUES ($((ROWS+1)),'inc-insert')"
  myq "UPDATE \`$T\` SET payload='inc-update' WHERE id=2"
  myq "DELETE FROM \`$T\` WHERE id=3"
  mark "mysql→pg 增量三笔"
  t0=$(now_s); lat=""
  runbin run --job acc-mysql-pg --data-dir "$data" --stop-when-idle 3s >"$W/incremental.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 600); do
    got=$(pgq "$dst" "SELECT (SELECT count(*) FROM $T WHERE id=$((ROWS+1)))::text||','||
                     (SELECT count(*) FROM $T WHERE id=2 AND payload='inc-update')::text||','||
                     (SELECT count(*) FROM $T WHERE id=3)::text" 2>/dev/null)
    [ "$got" = "1,1,0" ] && { lat=$(elapsed "$t0" "$(now_s)"); break; }
    sleep 0.1
  done
  wait $pid 2>/dev/null
  [ -n "$lat" ] || { echo "FAIL: 增量 60s 内没追平（${got:-无}）"; tail -5 "$W/incremental.log"; return 1; }
  incr_seconds=$lat
  s_fp=$(fp_mysql "$T"); d_fp=$(fp_pg "$dst" "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "mysql→pg 增量" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }
  mark "mysql→pg 增量追平 ${incr_seconds}s"
  verify_cross "$src" "$dst" "$T" "mysql→pg" "$W/verify.log" || return 1

  local extra="mysql=$ver paused=$paused snapshot_wall=${wall}s"
  if [ "$KILL" = 1 ]; then
    # kill -9 打在**增量**中间：此时全量已完成，重启只许从落库位点重放，重搬必须为 0。
    nums_mysql
    myq "INSERT INTO \`$T\`(id,payload) SELECT n, CONCAT('stream-',n) FROM _acc_nums
         WHERE n BETWEEN $((ROWS+2)) AND $((ROWS+2001)) ORDER BY n"
    local before
    before=$(pgq "$dst" "SELECT count(*) FROM $T")
    mark "mysql→pg 增量中（准备 kill -9）"
    # 这条**不能**套 timeout：我们要 kill -9 的是引擎自己的 pid，套一层 wrapper 杀的是
    # wrapper，引擎会变成孤儿继续流——重启那一轮就会撞租约。
    "$BIN" run --job acc-mysql-pg --data-dir "$data" --stop-after 600s >"$W/kill1.log" 2>&1 &
    pid=$!
    for _ in $(seq 1 1200); do
      got=$(pgq "$dst" "SELECT count(*) FROM $T WHERE id > $((ROWS+200))")
      [ "${got:-0}" -gt 0 ] 2>/dev/null && break
      sleep 0.05
    done
    kill -9 $pid 2>/dev/null; wait $pid 2>/dev/null
    local mid; mid=$(pgq "$dst" "SELECT count(*) FROM $T")
    echo "  kill -9（增量中）：目标端 $mid 行（中断前 $before）"
    # 停机期间再改三笔：只能靠「重启后从位点重放」补上
    myq "UPDATE \`$T\` SET payload='while-down' WHERE id BETWEEN 10 AND 20"
    myq "DELETE FROM \`$T\` WHERE id BETWEEN 30 AND 34"
    mark "kill -9 后重启（只许从位点重放）"
    t0=$(now_s)
    runbin run --job acc-mysql-pg --data-dir "$data" --stop-when-idle 5s >"$W/kill2.log" 2>&1
    rc=$?; local restart_seconds; restart_seconds=$(elapsed "$t0" "$(now_s)")
    [ $rc -eq 0 ] || { echo "FAIL: 重启退出码 $rc"; tail -5 "$W/kill2.log"; return 1; }
    local copied
    copied=$(grep -oE "全量完成：[0-9]+ 行" "$W/kill2.log" | tail -1 | grep -oE "[0-9]+")
    copied=${copied:-0}
    # 口径①【整轮】：`rowsCopied==0`（重启没有重新搬存量）。这是验收主口径。
    python3 "$GC" recopy --scope whole --copied "$copied" --label "kill -9 重启（整轮口径）" || return 1
    s_fp=$(fp_mysql "$T"); d_fp=$(fp_pg "$dst" "$T")
    python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "kill -9 重启后" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }
    extra="$extra kill_recopy=${copied}rows kill_restart=${restart_seconds}s"
  fi

  local s_n=${s_fp%%:*} s_md5=${s_fp#*:} d_n=${d_fp%%:*} d_md5=${d_fp#*:}
  emit mysql-postgres "public.$T" "$s_n" "$d_n" "$s_md5" "$d_md5" "$snap_seconds" "$incr_seconds" pass "$extra"
}

# 增量验收（三组合共用）：按 #93 之后的实现判，**不再把「被拒」当通过**。
#
# 旧脚本的口径是「这三个方向只支持全量，增量必须明确拒绝」——那是 #73/#89 之前的
# 事实。现在三个方向都接通了增量，`--stop-after` 会把一个正常流式跑的任务杀掉
# （退出码 130），脚本却把它当成「被拒」，于是 #92 在 #93 合入后红在两格上：
# 真正的结论是「增量跑起来了，只是判据还在等一句拒绝文案」。
#
# 判据三条，都可证伪：
#   ① 目标端三笔（增/改/删）全部可见——用轮询取追平时刻，顺便得到增量段耗时；
#   ② rc == 0：任务自己追平后收尾，不是被 timeout / Ctrl-C 打断；
#   ③ 源与目标的内容指纹一致（行数 + 逐行 md5）——不是「看起来动了」。
# 目标指纹**跑完再取**：跑前那份还是快照的指纹，拿它比等于拿「跑之前」比「跑之前」，
# 增量一行没写也会绿（这条最早就是这么写错的，红了两格才发现）。
INCR_SECONDS=0
incr_check() { # $1=job $2=data-dir $3=log $4=label $5=源指纹 $6=目标表名 $7=基线行数
  local job=$1 data=$2 log=$3 label=$4 s_fp=$5 T=$6 base=$7
  local rc d_fp t0 got pid=""
  t0=$(now_s); INCR_SECONDS=0
  timeout -k 5 180 "$BIN" run --job "$job" --data-dir "$data" --stop-when-idle 3s >"$log" 2>&1 &
  pid=$!
  for _ in $(seq 1 600); do
    got=$(myq "SELECT CONCAT(
                 (SELECT COUNT(*) FROM \`$T\` WHERE id=$((base+1))),',',
                 (SELECT COUNT(*) FROM \`$T\` WHERE id=2 AND payload='inc-update'),',',
                 (SELECT COUNT(*) FROM \`$T\` WHERE id=3))")
    [ "$got" = "1,1,0" ] && { INCR_SECONDS=$(elapsed "$t0" "$(now_s)"); break; }
    sleep 0.1
  done
  wait $pid 2>/dev/null; rc=$?
  [ "$got" = "1,1,0" ] || { echo "FAIL: $label 60s 内没追平（${got:-无}）"; tail -20 "$log" 2>/dev/null; return 1; }
  if [ $rc -ne 0 ]; then
    echo "FAIL: $label 退出码 $rc（增量没跑成；本版三个方向都已接通，拒绝=回归）"
    echo "--- $log ---"; tail -20 "$log" 2>/dev/null; echo "--- end ---"
    return 1
  fi
  d_fp=$(fp_mysql "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "$label" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }
  return 0
}

# ── 组合 2：PG → MySQL（全量 + 增量追平） ───────────────────────
combo_postgres_mysql() {
  local src=${JINLIANG_TEST_PG_SRC_DSN:-} dst=${JINLIANG_TEST_MYSQL_DST_DSN:-${JINLIANG_TEST_MYSQL_DSN:-}}
  [ -n "$src" ] && [ -n "$dst" ] || { echo "EX_CONFIG: 缺 JINLIANG_TEST_PG_SRC_DSN / JINLIANG_TEST_MYSQL_DST_DSN" >&2; return 78; }
  local T=acc_pm W; W=$(workdir postgres-mysql)
  split_mysql "$dst"; local ddb=$MDB; local ver; ver=$(mysql_server_version)
  local job=$W/job.yaml data; data=$(mktemp -d)   # 引擎状态放 /tmp，notes 只留证据（不含 .db/密钥）

  seed_pg "$src" "$T" "$ROWS"
  myq "DROP TABLE IF EXISTS \`$T\`"
  myq "DROP TABLE IF EXISTS _jl_meta,_jl_chunk,_jl_lease,_jl_object,_jl_instance"

  # 一开始就用 continuous+initial：书签/流在造增量之前就建好，三笔变更一定在流里。
  # 不能「先 once 全量、再 import 成 continuous」——once 收尾会把位点推到当前，
  # 两次 import 之间那三笔会落在位点后面，看起来像「增量 0 行」。
  cat > "$job" <<YAML
version: 1
job: acc-pg-mysql
source:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_SRC_DSN
  scope: {schema: public}
target:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_DST_DSN
  schema: $ddb
  on_exists: append
content:
  tables:
    $T: {}
mode: {frequency: continuous+initial}
YAML
  [ -n "${JINLIANG_TEST_MYSQL_DST_DSN:-}" ] || export JINLIANG_TEST_MYSQL_DST_DSN=$dst
  timeout 120 "$BIN" import --file "$job" --data-dir "$data" >/dev/null 2>&1 || { echo "FAIL: import 失败"; return 1; }

  local t0 snap_seconds incr_seconds rc s_fp d_fp KIND paused=0 got pid wall cnt
  mark "pg→mysql 快照开始（$ROWS 行）"
  t0=$(now_s)
  runbin run --job acc-pg-mysql --data-dir "$data" --stop-when-idle 5s >"$W/snapshot.log" 2>&1 &
  pid=$!
  snap_seconds=""
  for _ in $(seq 1 3000); do
    cnt=$(myq "SELECT COUNT(*) FROM \`$T\`")
    [ "${cnt:-0}" = "$ROWS" ] && { snap_seconds=$(elapsed "$t0" "$(now_s)"); break; }
    sleep 0.1
  done
  wait $pid 2>/dev/null; rc=$?
  wall=$(elapsed "$t0" "$(now_s)")
  [ -n "$snap_seconds" ] || { echo "FAIL: 快照 300s 内没搬完（${cnt:-无}/$ROWS）"; tail -20 "$W/snapshot.log"; return 1; }
  KIND=$(classify_rc "$rc" "$W/snapshot.log"); [ "$KIND" = "needs_action" ] && paused=1 || paused=0
  case "$KIND" in ok|needs_action|stopped) : ;; *) echo "FAIL: 快照段退出码 $rc（$KIND）"; tail -20 "$W/snapshot.log"; return 1 ;; esac
  mark "pg→mysql 快照完成 ${snap_seconds}s"

  s_fp=$(fp_pg "$src" "$T"); d_fp=$(fp_mysql "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "pg→mysql 快照" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }

  pgq "$src" "INSERT INTO $T VALUES ($((ROWS+1)),'inc-insert')" >/dev/null
  pgq "$src" "UPDATE $T SET payload='inc-update' WHERE id=2" >/dev/null
  pgq "$src" "DELETE FROM $T WHERE id=3" >/dev/null
  mark "pg→mysql 增量三笔"
  s_fp=$(fp_pg "$src" "$T")
  split_mysql "$dst"
  incr_check acc-pg-mysql "$data" "$W/incremental.log" "pg→mysql 增量" "$s_fp" "$T" "$ROWS" || return 1
  incr_seconds=$INCR_SECONDS
  mark "pg→mysql 增量追平 ${incr_seconds}s"
  split_mysql "$dst"
  d_fp=$(fp_mysql "$T")
  verify_cross "$src" "$dst" "$T" "pg→mysql" "$W/verify.log" || return 1

  local s_n=${s_fp%%:*} s_md5=${s_fp#*:} d_n=${d_fp%%:*} d_md5=${d_fp#*:}
  emit postgres-mysql "$ddb.$T" "$s_n" "$d_n" "$s_md5" "$d_md5" "$snap_seconds" "$incr_seconds" pass "mysql=$ver paused=$paused snapshot_wall=${wall}s"
}

# ── 组合 3：MySQL → MySQL（全量 + 增量追平） ────────────────────
combo_mysql_mysql() {
  local src=${JINLIANG_TEST_MYSQL_SRC_DSN:-} dst=${JINLIANG_TEST_MYSQL_DST_DSN:-}
  [ -n "$src" ] && [ -n "$dst" ] || { echo "EX_CONFIG: 缺 JINLIANG_TEST_MYSQL_SRC_DSN / JINLIANG_TEST_MYSQL_DST_DSN" >&2; return 78; }
  local T=acc_mm W; W=$(workdir mysql-mysql)
  local job=$W/job.yaml data; data=$(mktemp -d)   # 引擎状态放 /tmp，notes 只留证据（不含 .db/密钥）

  split_mysql "$src"; local sdb=$MDB; local ver; ver=$(mysql_server_version)
  seed_mysql "$T" "$ROWS"; local s_fp; s_fp=$(fp_mysql "$T")

  split_mysql "$dst"; local ddb=$MDB
  myq "DROP TABLE IF EXISTS \`$T\`"
  myq "DROP TABLE IF EXISTS _jl_meta,_jl_chunk,_jl_lease,_jl_object,_jl_instance"

  cat > "$job" <<YAML
version: 1
job: acc-mysql-mysql
source:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_SRC_DSN
  scope: {schema: $sdb}
target:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_DST_DSN
  schema: $ddb
  on_exists: append
content:
  tables:
    $T: {}
mode: {frequency: continuous+initial}
YAML
  timeout 120 "$BIN" import --file "$job" --data-dir "$data" >/dev/null 2>&1 || { echo "FAIL: import 失败"; return 1; }

  local t0 snap_seconds incr_seconds rc d_fp KIND paused=0 got pid wall cnt
  mark "mysql→mysql 快照开始（$ROWS 行）"
  t0=$(now_s)
  runbin run --job acc-mysql-mysql --data-dir "$data" --stop-when-idle 5s >"$W/snapshot.log" 2>&1 &
  pid=$!
  snap_seconds=""
  for _ in $(seq 1 3000); do
    cnt=$(myq "SELECT COUNT(*) FROM \`$T\`")
    [ "${cnt:-0}" = "$ROWS" ] && { snap_seconds=$(elapsed "$t0" "$(now_s)"); break; }
    sleep 0.1
  done
  wait $pid 2>/dev/null; rc=$?
  wall=$(elapsed "$t0" "$(now_s)")
  [ -n "$snap_seconds" ] || { echo "FAIL: 快照 300s 内没搬完（${cnt:-无}/$ROWS）"; tail -20 "$W/snapshot.log"; return 1; }
  KIND=$(classify_rc "$rc" "$W/snapshot.log"); [ "$KIND" = "needs_action" ] && paused=1 || paused=0
  case "$KIND" in ok|needs_action|stopped) : ;; *) echo "FAIL: 快照段退出码 $rc（$KIND）"; tail -20 "$W/snapshot.log"; return 1 ;; esac
  mark "mysql→mysql 快照完成 ${snap_seconds}s"

  d_fp=$(fp_mysql "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "mysql→mysql 快照" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }

  # 源端三笔（增/改/删），与 MySQL→PG / PG→MySQL 同一套动作。
  split_mysql "$src"
  myq "INSERT INTO \`$T\` VALUES ($((ROWS+1)),'inc-insert')"
  myq "UPDATE \`$T\` SET payload='inc-update' WHERE id=2"
  myq "DELETE FROM \`$T\` WHERE id=3"
  s_fp=$(fp_mysql "$T")
  split_mysql "$dst"
  mark "mysql→mysql 增量三笔"
  incr_check acc-mysql-mysql "$data" "$W/incremental.log" "mysql→mysql 增量" "$s_fp" "$T" "$ROWS" || return 1
  incr_seconds=$INCR_SECONDS
  mark "mysql→mysql 增量追平 ${incr_seconds}s"
  split_mysql "$dst"
  d_fp=$(fp_mysql "$T")
  verify_cross "$src" "$dst" "$T" "mysql→mysql" "$W/verify.log" || return 1

  local s_n=${s_fp%%:*} s_md5=${s_fp#*:} d_n=${d_fp%%:*} d_md5=${d_fp#*:}
  emit mysql-mysql "$ddb.$T" "$s_n" "$d_n" "$s_md5" "$d_md5" "$snap_seconds" "$incr_seconds" pass "mysql=$ver paused=$paused snapshot_wall=${wall}s"
}

LIST="mysql-postgres postgres-mysql mysql-mysql"
[ -n "$ONLY" ] && LIST="$ONLY"

for combo in $LIST; do
  echo "== $combo =="
  case "$combo" in
    mysql-postgres) combo_mysql_postgres ;;
    postgres-mysql) combo_postgres_mysql ;;
    mysql-mysql) combo_mysql_mysql ;;
    *) echo "acceptance: 不认识的组合 $combo" >&2; exit 2 ;;
  esac
  rc=$?
  if [ $rc -ne 0 ]; then
    echo "RESULT combo=$combo status=fail rc=$rc"
    RESULTS+=("RESULT combo=$combo status=fail rc=$rc")
    FAILED=$((FAILED + 1))
  fi
done

# 0 格 / 少跑格一律判失败：`--only " "` 时 for 一次都不进，旧脚本会打印
# 「全部通过」并 exit 0——门没开却说门是好的（Owl II 实测）。
set -- $LIST
WANT=$#
if [ "$WANT" -eq 0 ]; then
  echo "acceptance: --only 没给出任何组合（0 格可跑）——这不是通过" >&2
  exit 2
fi
if [ ${#RESULTS[@]} -ne "$WANT" ]; then
  echo "acceptance: 只产出 ${#RESULTS[@]} 格，期望 $WANT 格——判失败" >&2
  exit 1
fi

echo
echo "| 组合 | 目标表名 | 源行数 | 目标行数 | 源 md5 | 目标 md5 | 快照(s) | 增量(s) | 结论 |"
echo "|---|---|---|---|---|---|---|---|---|"
printf '%s\n' "${RESULTS[@]}" | python3 -c '
import sys
for line in sys.stdin:
    p = dict(x.split("=", 1) for x in line.split()[1:] if "=" in x)
    if p.get("combo") and p.get("status") == "pass":
        print("| {combo} | {target} | {src_rows} | {dst_rows} | `{src_md5}` | `{dst_md5}` | "
              "{snapshot_seconds} | {incremental_seconds} | pass |".format(**p))
    elif p.get("combo"):
        print("| {combo} | - | - | - | - | - | - | - | **FAIL** {rc} |".format(
            combo=p.get("combo", "?"), rc=p.get("rc", "")))
'

if [ -n "$JSON" ]; then
  { printf '[\n'; first=1
    for line in "${RESULTS[@]}"; do
      [ $first = 1 ] || printf ',\n'; first=0
      printf '%s' "$line" | python3 -c 'import json,sys; l=sys.stdin.read().strip(); print(json.dumps(dict(x.split("=",1) for x in l.split()[1:] if "=" in x), ensure_ascii=False))'
    done; printf '\n]\n'; } > "$JSON"
  echo "JSON：$JSON"
fi

if [ $FAILED -ne 0 ]; then
  echo "acceptance: $FAILED 格失败"
  exit 1
fi
echo "acceptance: 全部通过"
