#!/usr/bin/env bash
# 三组合验收矩阵（可重复执行）：MySQL→PG / PG→MySQL / MySQL→MySQL。
#
#   scripts/acceptance_matrix.sh                                   # 全部三组合
#   scripts/acceptance_matrix.sh --only mysql-postgres --rows 20000
#   scripts/acceptance_matrix.sh --kill                            # 追加 kill -9 续跑（MySQL→PG）
#   scripts/acceptance_matrix.sh --json .localdb/acceptance.json
#
# 每格只报三件事：**行数 / 内容 md5 / 耗时**；判据本身走
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
BIN=${BIN:-$ROOT/.localdb/bin/jinliang}
ROWS=${ROWS:-20000}
KILL=0
ONLY=""
JSON=""

while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="${2:-}"; shift 2 ;;
    --rows) ROWS="${2:-}"; shift 2 ;;
    --kill) KILL=1; shift ;;
    --json) JSON="${2:-}"; shift 2 ;;
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
myq() { # $1=sql（用当前 split 出来的连接）
  mysql --protocol=TCP -h "$MHOST" -P "$MPORT" -u"$MUSER" "--password=$MPASS" \
        --batch --skip-column-names -e "$1" "$MDB" 2>/dev/null
}
pgq() { psql "$1" -v ON_ERROR_STOP=1 -tAq -c "$2" 2>/dev/null; }

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

emit() { # combo rows md5 seconds status extra
  local line="RESULT combo=$1 rows=$2 md5=$3 seconds=$4 status=$5 ${6:-}"
  echo "$line"
  RESULTS+=("$line")
  [ "$5" = "pass" ] || FAILED=$((FAILED + 1))
}

workdir() { mktemp -d; }

# 造数：MySQL 侧（id 1..N，payload=row-N）
seed_mysql() { # $1=表名 $2=行数
  myq "DROP TABLE IF EXISTS \`$1\`"
  myq "CREATE TABLE \`$1\` (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
  local i=1 step=${INSERT_STEP:-10000} hi
  while [ "$i" -le "$2" ]; do
    hi=$((i + step - 1)); [ "$hi" -gt "$2" ] && hi=$2
    myq "SET SESSION cte_max_recursion_depth=1000000;
         INSERT INTO \`$1\`(id,payload)
         SELECT n, CONCAT('row-', n) FROM
           (WITH RECURSIVE seq AS (SELECT $i AS n UNION ALL SELECT n+1 FROM seq WHERE n < $hi)
            SELECT n FROM seq) s"
    i=$((hi + 1))
  done
}

seed_pg() { # $1=dsn $2=表名 $3=行数
  pgq "$1" "DROP TABLE IF EXISTS $2" >/dev/null
  pgq "$1" "CREATE TABLE $2 (id bigint PRIMARY KEY, payload text)" >/dev/null
  pgq "$1" "INSERT INTO $2 SELECT g, 'row-'||g FROM generate_series(1,$3) g" >/dev/null
}

now_s() { date +%s.%N; }
elapsed() { python3 -c "print(f'{$2-$1:.2f}')"; }

# ── 组合 1：MySQL → PG（全量 + 增量 + kill -9 续跑） ─────────────
combo_mysql_postgres() {
  local src=${JINLIANG_TEST_MYSQL_SRC_DSN:-} dst=${JINLIANG_TEST_PG_DSN:-}
  [ -n "$src" ] && [ -n "$dst" ] || { echo "EX_CONFIG: 缺 JINLIANG_TEST_MYSQL_SRC_DSN / JINLIANG_TEST_PG_DSN" >&2; return 78; }
  local T=acc_mp W; W=$(workdir)
  split_mysql "$src"; local sdb=$MDB; local ver; ver=$(mysql_server_version)
  local job=$W/job.yaml data=$W/data

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
  "$BIN" import --file "$job" --data-dir "$data" >/dev/null 2>&1 || { echo "FAIL: import 失败"; return 1; }

  local t0 t1 secs s_fp d_fp
  t0=$(now_s)
  "$BIN" run --job acc-mysql-pg --data-dir "$data" --stop-when-idle 5s >"$W/run1.log" 2>&1
  local rc=$?
  t1=$(now_s); secs=$(elapsed "$t0" "$t1")
  [ $rc -eq 0 ] || { echo "FAIL: 全量+追平退出码 $rc"; tail -5 "$W/run1.log"; return 1; }

  s_fp=$(fp_mysql "$T"); d_fp=$(fp_pg "$dst" "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "mysql→pg 全量" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }

  # 增量三笔（增/改/删）→ 追平时延
  myq "INSERT INTO \`$T\` VALUES ($((ROWS+1)),'inc-insert')"
  myq "UPDATE \`$T\` SET payload='inc-update' WHERE id=2"
  myq "DELETE FROM \`$T\` WHERE id=3"
  t0=$(now_s)
  "$BIN" run --job acc-mysql-pg --data-dir "$data" --stop-when-idle 3s >"$W/run2.log" 2>&1 &
  local pid=$! got="" lat=""
  for _ in $(seq 1 600); do
    got=$(pgq "$dst" "SELECT (SELECT count(*) FROM $T WHERE id=$((ROWS+1)))::text||','||
                     (SELECT count(*) FROM $T WHERE id=2 AND payload='inc-update')::text||','||
                     (SELECT count(*) FROM $T WHERE id=3)::text" 2>/dev/null)
    [ "$got" = "1,1,0" ] && { lat=$(elapsed "$t0" "$(now_s)"); break; }
    sleep 0.1
  done
  wait $pid 2>/dev/null
  [ -n "$lat" ] || { echo "FAIL: 增量 60s 内没追平（${got:-无}）"; tail -5 "$W/run2.log"; return 1; }
  s_fp=$(fp_mysql "$T"); d_fp=$(fp_pg "$dst" "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "mysql→pg 增量" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }

  local extra="latency=${lat}s"
  if [ "$KILL" = 1 ]; then
    # kill -9 打在**增量**中间：此时全量已完成，重启只许从落库位点重放，重搬必须为 0。
    myq "SET SESSION cte_max_recursion_depth=1000000;
         INSERT INTO \`$T\`(id,payload) SELECT n, CONCAT('stream-',n) FROM
           (WITH RECURSIVE seq AS (SELECT $((ROWS+2)) AS n UNION ALL SELECT n+1 FROM seq WHERE n < $((ROWS+2001)))
            SELECT n FROM seq) s"
    local before
    before=$(pgq "$dst" "SELECT count(*) FROM $T")
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
    t0=$(now_s)
    "$BIN" run --job acc-mysql-pg --data-dir "$data" --stop-when-idle 5s >"$W/kill2.log" 2>&1
    rc=$?; t1=$(now_s); secs=$(elapsed "$t0" "$t1")
    [ $rc -eq 0 ] || { echo "FAIL: 重启退出码 $rc"; tail -5 "$W/kill2.log"; return 1; }
    local copied
    copied=$(grep -oE "全量完成：[0-9]+ 行" "$W/kill2.log" | tail -1 | grep -oE "[0-9]+")
    copied=${copied:-0}
    python3 "$GC" recopy --copied "$copied" --label "kill -9 重启" || return 1
    s_fp=$(fp_mysql "$T"); d_fp=$(fp_pg "$dst" "$T")
    python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "kill -9 重启后" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }
    extra="$extra kill=recopy-${copied}-rows restart=${secs}s"
  fi

  local n; n=$(pgq "$dst" "SELECT count(*) FROM $T")
  emit "mysql-postgres" "$n" "${s_fp#*:}" "$secs" pass "mysql=$ver $extra"
}

# ── 组合 2：PG → MySQL（本版只全量；增量必须明确拒绝） ───────────
combo_postgres_mysql() {
  local src=${JINLIANG_TEST_PG_SRC_DSN:-} dst=${JINLIANG_TEST_MYSQL_DST_DSN:-${JINLIANG_TEST_MYSQL_DSN:-}}
  [ -n "$src" ] && [ -n "$dst" ] || { echo "EX_CONFIG: 缺 JINLIANG_TEST_PG_SRC_DSN / JINLIANG_TEST_MYSQL_DST_DSN" >&2; return 78; }
  local T=acc_pm W; W=$(workdir)
  split_mysql "$dst"; local ddb=$MDB; local ver; ver=$(mysql_server_version)
  local job=$W/job.yaml data=$W/data

  seed_pg "$src" "$T" "$ROWS"
  myq "DROP TABLE IF EXISTS \`$T\`"
  myq "DROP TABLE IF EXISTS _jl_meta,_jl_chunk,_jl_lease,_jl_object,_jl_instance"

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
mode: {frequency: once}
YAML
  [ -n "${JINLIANG_TEST_MYSQL_DST_DSN:-}" ] || export JINLIANG_TEST_MYSQL_DST_DSN=$dst
  "$BIN" import --file "$job" --data-dir "$data" >/dev/null 2>&1 || { echo "FAIL: import 失败"; return 1; }

  local t0 t1 secs s_fp d_fp n rc
  t0=$(now_s)
  "$BIN" run --job acc-pg-mysql --data-dir "$data" >"$W/run1.log" 2>&1
  rc=$?
  t1=$(now_s); secs=$(elapsed "$t0" "$t1")
  [ $rc -eq 0 ] || { echo "FAIL: 全量退出码 $rc"; tail -5 "$W/run1.log"; return 1; }

  s_fp=$(fp_pg "$src" "$T"); d_fp=$(fp_mysql "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "pg→mysql 全量" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }

  # 增量必须**明确拒绝**（不静默降级成「只搬一次」）
  sed 's/frequency: once/frequency: continuous+initial/' "$job" > "$W/job2.yaml"
  "$BIN" import --file "$W/job2.yaml" --data-dir "$data" >/dev/null 2>&1 || true
  if "$BIN" run --job acc-pg-mysql --data-dir "$data" >"$W/incr.log" 2>&1; then
    echo "FAIL: PG→MySQL 增量居然跑了（本版应明确拒绝）"; tail -3 "$W/incr.log"; return 1
  fi
  grep -q "只搬全量\|还没接上" "$W/incr.log" || { echo "FAIL: 增量被拒但没说清原因"; tail -3 "$W/incr.log"; return 1; }

  n=$(myq "SELECT COUNT(*) FROM \`$T\`")
  emit "postgres-mysql" "$n" "${s_fp#*:}" "$secs" pass "mysql=$ver incremental=refused"
}

# ── 组合 3：MySQL → MySQL（本版只全量；增量必须明确拒绝） ────────
combo_mysql_mysql() {
  local src=${JINLIANG_TEST_MYSQL_SRC_DSN:-} dst=${JINLIANG_TEST_MYSQL_DST_DSN:-}
  [ -n "$src" ] && [ -n "$dst" ] || { echo "EX_CONFIG: 缺 JINLIANG_TEST_MYSQL_SRC_DSN / JINLIANG_TEST_MYSQL_DST_DSN" >&2; return 78; }
  local T=acc_mm W; W=$(workdir)
  local job=$W/job.yaml data=$W/data

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
mode: {frequency: once}
YAML
  "$BIN" import --file "$job" --data-dir "$data" >/dev/null 2>&1 || { echo "FAIL: import 失败"; return 1; }

  local t0 t1 secs d_fp n rc
  t0=$(now_s)
  "$BIN" run --job acc-mysql-mysql --data-dir "$data" >"$W/run1.log" 2>&1
  rc=$?
  t1=$(now_s); secs=$(elapsed "$t0" "$t1")
  [ $rc -eq 0 ] || { echo "FAIL: 全量退出码 $rc"; tail -5 "$W/run1.log"; return 1; }

  d_fp=$(fp_mysql "$T")
  python3 "$GC" fp --src "$s_fp" --dst "$d_fp" --label "mysql→mysql 全量" || { echo "  源 $s_fp / 目标 $d_fp"; return 1; }

  sed 's/frequency: once/frequency: continuous+initial/' "$job" > "$W/job2.yaml"
  "$BIN" import --file "$W/job2.yaml" --data-dir "$data" >/dev/null 2>&1 || true
  if "$BIN" run --job acc-mysql-mysql --data-dir "$data" >"$W/incr.log" 2>&1; then
    echo "FAIL: MySQL→MySQL 增量居然跑了（本版应明确拒绝）"; tail -3 "$W/incr.log"; return 1
  fi
  grep -q "只搬全量\|还没接上\|只支持全量" "$W/incr.log" || { echo "FAIL: 增量被拒但没说清原因"; tail -3 "$W/incr.log"; return 1; }

  n=$(myq "SELECT COUNT(*) FROM \`$T\`")
  emit "mysql-mysql" "$n" "${s_fp#*:}" "$secs" pass "mysql=$ver incremental=refused"
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

echo
echo "| 组合 | MySQL | 行数 | md5 | 耗时 | 备注 |"
echo "|---|---|---|---|---|---|"
for line in "${RESULTS[@]}"; do
  combo="" rows="" md5="" seconds="" status="" latency="" kill="" incremental="" rc="" mysql=""
  eval "$(printf '%s' "$line" | sed -E 's/^RESULT //; s/([a-z-]+)=([^ ]*)/\1="\2"/g')"
  if [ "${status:-fail}" = "pass" ]; then
    echo "| ${combo:-?} | ${mysql:-?} | ${rows:-?} | \`${md5:-?}\` | ${seconds:-?}s | ${latency:-} ${kill:-} ${incremental:-} |"
  else
    echo "| ${combo:-?} | - | - | - | - | **FAIL** ${rc:-} |"
  fi
done

if [ -n "$JSON" ]; then
  { printf '[\n'; first=1
    for line in "${RESULTS[@]}"; do
      [ $first = 1 ] || printf ',\n'; first=0
      printf '%s' "$line" | python3 -c 'import json,sys; l=sys.stdin.read().strip(); p=dict(x.split("=",1) for x in l.split()[1:]); print(json.dumps(p, ensure_ascii=False))'
    done; printf '\n]\n'; } > "$JSON"
  echo "JSON：$JSON"
fi

if [ $FAILED -ne 0 ]; then
  echo "acceptance: $FAILED 格失败"
  exit 1
fi
echo "acceptance: 全部通过"
