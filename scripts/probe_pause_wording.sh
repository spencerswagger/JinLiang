#!/usr/bin/env bash
# 三种 mode（continuous+initial / interval / full_interval）的**轮次提示**探针。
#
# 目的：验证「结构对不上」时用户看到的 note 与 DECISIONS #19 / 契约 §1.6 例外段一致——
# interval 只出现「interval 模式不做按表暂停」那一条，不再出现 pgsink 的
# 「本轮暂停这张表，其它表继续」（那是批式模式的说法）。
#
# 依赖：JINLIANG_TEST_PG_SRC_DSN / JINLIANG_TEST_PG_DSN（两个**不同**的库）+ psql。
#
# 用法：BIN=./dist/jinliang scripts/probe_pause_wording.sh
# 期望（与 DECISIONS #19 / 契约 §1.6 例外段一致）：
#   continuous+initial / full_interval → 出现「本轮暂停这张表，其它表继续」+「本轮不搬它的存量」
#   interval                          → **不出现**前者；出现「interval 模式不做按表暂停」+ PAUSE-01 warn 那条
set -uo pipefail

BIN=${BIN:-./dist/jinliang}
SRC=${JINLIANG_TEST_PG_SRC_DSN:-}
DST=${JINLIANG_TEST_PG_DSN:-}
# 缺 DSN / 缺 psql 一律 78（EX_CONFIG）：CI 里是红的，不是「跳过即通过」。
if [ -z "$SRC" ] || [ -z "$DST" ]; then
  echo "EX_CONFIG: 缺 JINLIANG_TEST_PG_SRC_DSN / JINLIANG_TEST_PG_DSN（要两个不同的库）" >&2
  exit 78
fi
if [ "$SRC" = "$DST" ]; then
  echo "EX_CONFIG: 源库和目标库是同一个（会把表同步到自己身上）" >&2
  exit 78
fi
PSQL=${PSQL:-psql}
command -v "$PSQL" >/dev/null 2>&1 || { echo "EX_CONFIG: 没有 psql" >&2; exit 78; }
WORK=${WORK:-$(mktemp -d)}
mkdir -p "$WORK"

qs() { $PSQL "$SRC" -v ON_ERROR_STOP=1 -qtAc "$1"; }
qd() { $PSQL "$DST" -v ON_ERROR_STOP=1 -qtAc "$1"; }

reset_all() {
  qs "DROP TABLE IF EXISTS t_ok, t_drift CASCADE" >/dev/null
  qd "DROP TABLE IF EXISTS t_ok, t_drift CASCADE" >/dev/null
  qd "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance CASCADE" >/dev/null
  qs "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'jl_%'" >/dev/null
  qs "DO \$\$ DECLARE p text; BEGIN FOR p IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'jl_%' LOOP EXECUTE 'DROP PUBLICATION '||quote_ident(p); END LOOP; END \$\$" >/dev/null
}

make_source() {
  qs "CREATE TABLE t_ok (id bigint PRIMARY KEY, payload text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())" >/dev/null
  qs "CREATE TABLE t_drift (id bigint PRIMARY KEY, amount numeric(12,2), updated_at timestamptz NOT NULL DEFAULT now())" >/dev/null
  qs "INSERT INTO t_ok (id,payload) SELECT g, 'row-'||g FROM generate_series(1,50) g" >/dev/null
  qs "INSERT INTO t_drift (id,amount) SELECT g, (g%100)::numeric/100 FROM generate_series(1,20) g" >/dev/null
}

job_yaml() { # $1 = frequency
  cat <<YAML
version: 1
job: pausenotes-$1
source:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_SRC_DSN
  scope:
    schema: public
  snapshot:
    chunk_rows: 1000
target:
  kind: postgres
  dsn: env:JINLIANG_TEST_PG_DSN
  schema: public
  on_exists: append
content:
  tables:
    t_ok: {}
    t_drift: {}
mode:
  frequency: $1
  interval: 5m
  incremental_column: updated_at
  lookback: 5m
reliability:
  batch_rows: 500
YAML
}

run_mode() {
  local mode="$1" dir="$WORK/$1" job="$WORK/$1.yaml"
  echo "############ mode=$mode ############"
  reset_all
  make_source
  rm -rf "$dir"; mkdir -p "$dir"
  job_yaml "$mode" > "$job"
  "$BIN" import --file "$job" --data-dir "$dir" 2>&1 | tail -2

  # ① 先让本任务自己建出目标表（这样才能拿到 resume=recopy；append 下用户表是 manual）
  "$BIN" run --job "pausenotes-$mode" --data-dir "$dir" --stop-when-idle 3s --stop-after 25s \
    > "$WORK/$mode.setup.log" 2>&1
  echo "-- setup rc=$? （目标表由本任务建）"

  # ② 把目标端 t_drift.amount 改成 text → 与源端 numeric(12,2) 对不上 = 结构漂移
  qd "ALTER TABLE t_drift ALTER COLUMN amount TYPE text USING amount::text" >/dev/null

  # ③ 第二轮：观察 note
  "$BIN" run --job "pausenotes-$mode" --data-dir "$dir" --stop-when-idle 3s --stop-after 20s \
    > "$WORK/$mode.probe.log" 2>&1
  echo "-- probe rc=$? ；note 行："
  grep -aE "暂停|结构|interval|定时增量|整表重搬|不再写入" "$WORK/$mode.probe.log" | sed 's/^/   /'
  echo "---- paused_tables（目标库）:"
  qd "SELECT COALESCE(paused_tables,'(空)') FROM _jl_meta" 2>/dev/null | sed 's/^/   /' || echo "   (没有 _jl_meta)" 
  echo
}

for m in continuous+initial interval full_interval; do run_mode "$m"; done
