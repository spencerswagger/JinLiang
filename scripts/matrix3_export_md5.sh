#!/usr/bin/env bash
# 第二口径复核：**整表导出后取 md5**（与 acceptance_matrix 的「逐行 md5 拼接」不同算式）。
#
#   ROWS=20000 scripts/matrix3_export_md5.sh
#
# 两个口径都对上，才算「逐字节一致」：
#   口径①（矩阵主口径）逐行 md5(concat_ws('|', id, payload)) 拼接后再 md5；
#   口径②（本脚本）两端各按主键导出 `id<TAB>payload` 文本，对文件取 md5。
# 口径②没有任何哈希函数参与规范化，直接比字节，缺点是大表吃内存/磁盘、特殊字符需要
# 两端转义规则一致（本数据集 payload 只有 ASCII row-N / inc-* / stream-N，够用）。
#
# 需要本地栈在跑（scripts/local_stack.sh up）与已跑过 scripts/matrix3.sh。
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/.localdb/stack.env"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

split_mysql() { # mysql://u:p@h:port/db → MUSER MPASS MHOST MPORT MDB
  local s=${1#mysql://} creds rest hp db
  creds=${s%%@*}; rest=${s#*@}; hp=${rest%%/*}; db=${rest#*/}; db=${db%%\?*}
  MUSER=${creds%%:*}; MPASS=${creds#*:}; [ "$MPASS" = "$creds" ] && MPASS=""
  MHOST=${hp%%:*}; MPORT=${hp#*:}; [ "$MPORT" = "$hp" ] && MPORT=3306
  MDB=$db
}

my_dump() { # $1=dsn $2=表 → 文件
  split_mysql "$1"
  MYSQL_PWD="$MPASS" timeout 120 mysql --protocol=TCP -h "$MHOST" -P "$MPORT" -u"$MUSER" \
    --batch --skip-column-names -e "SELECT id, payload FROM \`$2\` ORDER BY id" "$MDB"
}
pg_dump_t() { # $1=dsn $2=表 → 文件
  timeout 120 psql "$1" -tA -F $'\t' -c "SELECT id, payload FROM $2 ORDER BY id"
}

md5f() { md5sum "$1" | cut -d' ' -f1; }

check() { # $1=label $2=源类型(mysql|pg) $3=源DSN $4=源表 $5=目标类型 $6=目标DSN $7=目标表
  local label=$1 st=$2 sd=$3 stb=$4 tt=$5 td=$6 ttb=$7 sf=$TMP/s tf=$TMP/d s d
  case $st in mysql) my_dump "$sd" "$stb" >"$sf" ;; pg) pg_dump_t "$sd" "$stb" >"$sf" ;; esac
  case $tt in mysql) my_dump "$td" "$ttb" >"$tf" ;; pg) pg_dump_t "$td" "$ttb" >"$tf" ;; esac
  s=$(md5f "$sf"); d=$(md5f "$tf")
  printf 'EXPORT_MD5 combo=%s source=%s target=%s rows_src=%s rows_dst=%s equal=%s\n' \
    "$label" "$s" "$d" "$(wc -l <"$sf")" "$(wc -l <"$tf")" "$([ "$s" = "$d" ] && echo yes || echo NO)"
  [ "$s" = "$d" ]
}

rc=0
check mysql-mysql  mysql "$JINLIANG_TEST_MYSQL_SRC_DSN" acc_mm \
                   mysql "$JINLIANG_TEST_MYSQL_DST_DSN" acc_mm || rc=1
check mysql-pg     mysql "$JINLIANG_TEST_MYSQL_SRC_DSN" acc_mp \
                   pg    "$JINLIANG_TEST_PG_DSN"       public.acc_mp || rc=1
check pg-mysql     pg    "$JINLIANG_TEST_PG_SRC_DSN"   acc_pm \
                   mysql "$JINLIANG_TEST_MYSQL_DST_DSN" acc_pm || rc=1
exit $rc
