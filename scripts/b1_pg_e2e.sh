#!/usr/bin/env bash
# B1 验收：PostgreSQL → PostgreSQL 真链路。
#
#   ROWS=1000000 scripts/b1_pg_e2e.sh ./dist/jinliang            # continuous+initial：全量+增量+校验
#   ROWS=1000000 scripts/b1_pg_e2e.sh ./dist/jinliang once       # once：只搬存量，跑完即终态
#   ROWS=1000000 scripts/b1_pg_e2e.sh ./dist/jinliang kill         # 快照中途 kill -9 再重启
#   ROWS=10000000 scripts/b1_pg_e2e.sh ./dist/jinliang             # 1000 万行
#
# 需要两个**不同**的库（源与目标）：JINLIANG_TEST_PG_SRC_DSN / JINLIANG_TEST_PG_DSN，
# 以及本机的 psql。没有 DSN 时脚本以 78 退出（CI 里当作「跳过」而不是「通过」）。
set -euo pipefail

BIN=${1:-./dist/jinliang}
MODE=${2:-full}
ROWS=${ROWS:-1000000}
SRC=${JINLIANG_TEST_PG_SRC_DSN:-}
DST=${JINLIANG_TEST_PG_DSN:-}

# 缺 DSN 一律 exit 78（EX_CONFIG）：CI 里这就是**红的**，不是"跳过即通过"。
# 变量名以 scripts/e2e_headless.sh 与 docs/BUILD.md 为准：
#   JINLIANG_TEST_PG_DSN / JINLIANG_TEST_PG_SRC_DSN / JINLIANG_TEST_MYSQL_DSN / JINLIANG_TEST_MYSQL_SRC_DSN
if [ -z "$SRC" ] || [ -z "$DST" ]; then
  echo "EX_CONFIG: 缺 JINLIANG_TEST_PG_SRC_DSN / JINLIANG_TEST_PG_DSN（B1 验收要两个不同的库）" >&2
  echo "           本脚本缺 DSN 时以 78 退出 = 失败，不会静默通过；CI 的 b1-engine job 负责把两个库备齐。" >&2
  exit 78
fi
if ! command -v psql >/dev/null 2>&1; then
  echo "EX_CONFIG: 没有 psql（B1 验收要连两个库核对指纹）" >&2
  exit 78
fi
if [ "$SRC" = "$DST" ]; then
  echo "EX_CONFIG: 源库和目标库是同一个（会把表同步到自己身上）" >&2
  exit 78
fi

psql_src() { psql "$SRC" -v ON_ERROR_STOP=1 -tAq -c "$1"; }
psql_dst() { psql "$DST" -v ON_ERROR_STOP=1 -tAq -c "$1"; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
JOB="$WORK/job.yaml"
DATA="$WORK/data"

fingerprint() { # $1 = psql 函数名
  "$1" "SELECT count(*)::text || ':' || COALESCE(sum(hashtextextended(t::text,0)),0)::text FROM orders t"
}

echo "== 1. 造数：源端 $ROWS 行 =="
psql_src "DROP TABLE IF EXISTS orders CASCADE" >/dev/null
psql_src "CREATE TABLE orders (
            id         bigint PRIMARY KEY,
            payload    text NOT NULL,
            amount     numeric(12,2),
            created_at timestamptz)" >/dev/null
psql_src "INSERT INTO orders(id,payload,amount,created_at)
          SELECT g, 'row-'||g, (g%10000)::numeric/100, now() - (g||' seconds')::interval
          FROM generate_series(1,$ROWS) g" >/dev/null
echo "   源端指纹 $(fingerprint psql_src)"

echo "== 2. 清空目标端与残留对象 =="
psql_dst "DROP TABLE IF EXISTS orders CASCADE" >/dev/null
psql_dst "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance CASCADE" >/dev/null
psql_src "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'jl_%'" >/dev/null
psql_src "SELECT 1 FROM pg_publication WHERE pubname LIKE 'jl_%'" | while read -r _; do :; done
psql_src "DO \$\$ DECLARE p text; BEGIN FOR p IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'jl_%' LOOP EXECUTE 'DROP PUBLICATION '||quote_ident(p); END LOOP; END \$\$" >/dev/null

# CHUNK_ROWS：块大小。kill 模式要打到"快照第 2 块"，块太多/太少都不稳，
# 所以 CI 里把它调小（行数少也能有好几块）。
CHUNK=${CHUNK_ROWS:-100000}
FREQ=continuous+initial
[ "$MODE" = "once" ] && FREQ=once
cat > "$JOB" <<YAML
version: 1
job: b1-orders
source:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_SRC_DSN
  scope:
    schema: public
  snapshot:
    chunk_rows: $CHUNK
target:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_DSN
  schema: public
  on_exists: append
mode:
  frequency: $FREQ
reliability:
  batch_rows: 5000
YAML

echo "== 3. 导入任务 =="
"$BIN" import --file "$JOB" --data-dir "$DATA"

if [ "$MODE" = "once" ]; then
  echo "== 4. once：只搬存量，跑完即终态 =="
  time "$BIN" run --job b1-orders --data-dir "$DATA" --verify > "$WORK/run1.log" 2>&1
  grep -E "全量完成|校验|结果：" "$WORK/run1.log" | tail -4
  psql_dst "SELECT phase FROM _jl_meta" | sed 's/^/   目标端阶段：/'
  SRCFP=$(fingerprint psql_src)
  DSTFP=$(fingerprint psql_dst)
  echo "   源端 $SRCFP"
  echo "   目标 $DSTFP"
  if [ "$SRCFP" != "$DSTFP" ]; then
    echo "FAIL: once 搬完之后源与目标不一致"
    exit 1
  fi
  echo "PASS: $ROWS 行，模式 once（只搬存量），指纹一致 $SRCFP"
  exit 0
elif [ "$MODE" = "kill" ]; then
  echo "== 4. 全量中途 kill -9 =="
  "$BIN" run --job b1-orders --data-dir "$DATA" --stop-after 600s > "$WORK/run1.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 120); do
    grep -q "第 2 块" "$WORK/run1.log" 2>/dev/null && break
    sleep 0.5
  done
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  echo "   已 kill -9（快照进行中）；目标端此刻行数 $(psql_dst 'SELECT count(*) FROM orders' 2>/dev/null || echo 0)"
  echo "   残留状态：$(psql_dst "SELECT phase||' '||COALESCE(position,'(无位点)') FROM _jl_meta" 2>/dev/null || echo '无')"
  # 停机期间源端删掉一行（这一行已经在第 1 块里搬过去了）：
  # 重搬全量必须先清掉「上次搬到一半的行」，否则会留下源端已经没有的行。
  psql_src "DELETE FROM orders WHERE id=5" >/dev/null
  echo "   停机期间源端删除了 id=5（重搬时必须消失）"
  echo "== 5. 重启：应当识别「全量没搬完」并重搬 =="
  timeout 600 "$BIN" run --job b1-orders --data-dir "$DATA" --stop-when-idle 5s > "$WORK/run2.log" 2>&1 || true
  grep -E "全量完成|跳过全量|重建了同步书签|清空了上次" "$WORK/run2.log" || true
else
  echo "== 4. 第一次跑（全量 → 增量 → 收尾）=="
  "$BIN" run --job b1-orders --data-dir "$DATA" --stop-after 10s --verify > "$WORK/run1.log" 2>&1 || true
  tail -4 "$WORK/run1.log"
fi

echo "== 6. 增量：插入 / 更新 / 删除各一笔，跑到追平为止 =="
psql_src "INSERT INTO orders(id,payload,amount,created_at) VALUES ($ROWS+1,'inserted-after-snapshot',1.23,now())" >/dev/null
psql_src "UPDATE orders SET payload='updated-after-snapshot', amount=9.87 WHERE id=2" >/dev/null
psql_src "DELETE FROM orders WHERE id=3" >/dev/null
# --stop-when-idle：追平并安静 5s 后正常收尾（一次性的增量同步就是这么用）
IDLE=${IDLE:-5s}
CAP=${CAP:-600s}
timeout "$CAP" "$BIN" run --job b1-orders --data-dir "$DATA" --stop-when-idle "$IDLE" > "$WORK/run3.log" 2>&1 || true
grep -E "增量：|追平|收尾|error" "$WORK/run3.log" | tail -6 || true

echo "== 7. 对比（行数 + 内容指纹）=="
SRCFP=$(fingerprint psql_src)
DSTFP=$(fingerprint psql_dst)
echo "   源端 $SRCFP"
echo "   目标 $DSTFP"
if [ "$SRCFP" != "$DSTFP" ]; then
  echo "FAIL: 源与目标不一致（应当不重不丢）"
  exit 1
fi

echo "== 8. status 一眼看 =="
"$BIN" status --job b1-orders --data-dir "$DATA" || true

echo "PASS: $ROWS 行，模式 $MODE，指纹一致 $SRCFP"
