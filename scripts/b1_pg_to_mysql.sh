#!/usr/bin/env bash
# PG 源 → MySQL 目标：**全量**验收（本版只做全量；增量在引擎侧明确报错）。
#
#   ROWS=20000 scripts/b1_pg_to_mysql.sh ./dist/jinliang
#
# 两个 DSN 从环境读：JINLIANG_TEST_PG_SRC_DSN（源）、JINLIANG_TEST_MYSQL_DSN（目标）。
# 缺任何一个 → exit 78（EX_CONFIG）= CI 红，不静默跳过。
set -euo pipefail

BIN=${1:-./dist/jinliang}
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

echo "== 1. 造数：源端 $ROWS 行 =="
pg "DROP TABLE IF EXISTS my_orders" >/dev/null
pg "CREATE TABLE my_orders (id bigint PRIMARY KEY, payload text, amount numeric(12,2), created_at timestamptz)" >/dev/null
pg "INSERT INTO my_orders SELECT g, 'row-'||g, (g%10000)::numeric/100, now() - (g||' seconds')::interval
    FROM generate_series(1,$ROWS) g" >/dev/null

echo "== 2. 清空目标库 =="
my -e "DROP TABLE IF EXISTS \`$MY_DB\`.\`my_orders\`" || true

cat > "$WORK/job.yaml" <<YAML
version: 1
job: pg-to-mysql
source:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_SRC_DSN
  scope:
    schema: public
target:
  kind: mysql
  dsn: env:JINLIANG_TEST_MYSQL_DSN
  on_exists: append
content:
  tables:
    my_orders: {}
mode:
  frequency: once
YAML

echo "== 3. 导入并跑全量 =="
"$BIN" import --file "$WORK/job.yaml" --data-dir "$DATA" >/dev/null
"$BIN" run --job pg-to-mysql --data-dir "$DATA" | tail -4

echo "== 4. 行数与 md5 对比 =="
SRC_N=$(pg "SELECT count(*) FROM my_orders")
DST_N=$(my -e "SELECT count(*) FROM \`$MY_DB\`.\`my_orders\`")
SRC_MD5=$(pg "SELECT md5(string_agg(id||'|'||payload||'|'||amount::text||'|'||
    to_char(created_at at time zone 'UTC','YYYY-MM-DD HH24:MI:SS.US'), E'\n' order by id)) FROM my_orders")
DST_MD5=$(my -e "SET SESSION group_concat_max_len=100000000;
    SELECT md5(group_concat(concat_ws('|', id, payload, amount,
      date_format(created_at,'%Y-%m-%d %H:%i:%s.%f')) ORDER BY id SEPARATOR '\n'))
    FROM \`$MY_DB\`.\`my_orders\`")
echo "   行数 源=$SRC_N 目标=$DST_N"
echo "   md5  源=$SRC_MD5"
echo "       目标=$DST_MD5"
if [ "$SRC_N" != "$DST_N" ] || [ "$SRC_MD5" != "$DST_MD5" ]; then
  echo "FAIL: 源与目标不一致"
  exit 1
fi

echo "== 5. 增量在本版必须明确拒绝（不静默降级）=="
sed 's/frequency: once/frequency: continuous+initial/' "$WORK/job.yaml" > "$WORK/job2.yaml"
"$BIN" import --file "$WORK/job2.yaml" --data-dir "$DATA" >/dev/null 2>&1 || true
OUT=$("$BIN" run --job pg-to-mysql --data-dir "$DATA" 2>&1 || true)
if printf '%s' "$OUT" | grep -q "只支持全量"; then
  echo "   ✓ 增量被明确拒绝并说明原因"
else
  echo "FAIL: 增量没有明确拒绝"; printf '%s\n' "$OUT" | tail -3; exit 1
fi

echo "PASS: $ROWS 行，PG → MySQL 全量一致（行数 + md5）"
