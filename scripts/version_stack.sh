#!/usr/bin/env bash
# 起两个**指定版本**的 MySQL 实例（版本档矩阵用：5.7 / 8.0 / 8.4 / 9.x）。
#
#   scripts/version_stack.sh up 9.7.4 /.toolchain/mysql-9.7.4/usr 53346 53347
#   eval "$(scripts/version_stack.sh env 9.7.4 53346 53347)"     # 导出两条 DSN
#   MATRIX3_NO_STACK=1 ROWS=20000 OUT=notes/matrix3/versions/9.7.4 scripts/matrix3.sh
#
# 数据目录在 `.localdb/ver-<label>/{my,my2}`，**不碰**默认栈的 `.localdb/my*`：
# 版本档跑完再跑 8.0 默认栈，两套数据目录互不影响（同端口复用会拿到别人的库，最难查）。
#
# 为什么不用 docker：沙盒里没有 docker；官方通用包/ deb 装出来的 mysqld 一样能开
# binlog ROW/FULL，矩阵需要的只是「两个独立实例 + 行镜像 FULL」。
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cmd=${1:-up}; label=${2:-}
case "$cmd" in
  up|down) basedir=${3:-}; port1=${4:-}; port2=${5:-} ;;
  env)     port1=${3:-}; port2=${4:-} ;;          # env 不需要 basedir
  *) echo "用法: $0 up|down <label> <basedir> <port1> <port2> | env <label> <port1> <port2>" >&2; exit 2 ;;
esac
[ -n "$label" ] || { echo "缺 <label>（如 9.7.4）" >&2; exit 2; }
BASE="$ROOT/.localdb/ver-$label"

log() { printf '[ver-stack %s] %s\n' "$label" "$*" >&2; }

if [ "$cmd" = "env" ]; then
  [ -n "$port1" ] && [ -n "$port2" ] || { echo "缺端口" >&2; exit 2; }
  echo "export JINLIANG_TEST_MYSQL_SRC_DSN=mysql://jinliang:jinliang@127.0.0.1:$port1/jinliang_src"
  echo "export JINLIANG_TEST_MYSQL_DST_DSN=mysql://jinliang:jinliang@127.0.0.1:$port2/jinliang_target"
  echo "export JINLIANG_TEST_MYSQL_DSN=mysql://jinliang:jinliang@127.0.0.1:$port1/jinliang_test"
  exit 0
fi

[ "$cmd" = "env" ] || [ -n "$basedir" ] || { echo "缺 basedir" >&2; exit 2; }
[ "$cmd" = "env" ] || [ -x "$basedir/bin/mysqld" ] || { echo "没有 $basedir/bin/mysqld" >&2; exit 78; }

# 沙盒 /etc 是 700 root：切到非 root 后读不到 /etc/passwd（见 scripts/local_stack.sh）
chmod 755 /etc 2>/dev/null || true

ping_() { "$basedir/bin/mysqladmin" --socket="$1/mysqld.sock" -uroot ping >/dev/null 2>&1; }

up_one() { # $1=目录名 $2=端口 $3=server-id
  local dir="$BASE/$1" port="$2" sid="$3"
  if [ ! -d "$dir/mysql" ]; then
    log "初始化 $dir（$("$basedir/bin/mysqld" --version | head -1)）"
    rm -rf "$dir"; mkdir -p "$dir"
    "$basedir/bin/mysqld" --initialize-insecure --user=root --basedir="$basedir" --datadir="$dir" \
      --log-error="$BASE/$1-init.log" >/dev/null 2>&1 || {
        log "初始化失败"; tail -5 "$BASE/$1-init.log" >&2; exit 1; }
  fi
  if ! ping_ "$dir"; then
    rm -f "$dir/mysqld.sock" "$dir/mysqld.sock.lock" "$dir/mysqld.pid"
    setsid nohup "$basedir/bin/mysqld" \
      --user=root --basedir="$basedir" --datadir="$dir" \
      --port="$port" --socket="$dir/mysqld.sock" --pid-file="$dir/mysqld.pid" \
      --bind-address=127.0.0.1 --mysqlx=OFF --skip-name-resolve \
      --server-id="$sid" --log-bin="$dir/mysql-bin" --binlog-format=ROW --binlog-row-image=FULL \
      --log-error="$dir/error.log" </dev/null >>"$dir/stdout.log" 2>&1 &
    for _ in $(seq 1 180); do ping_ "$dir" && break; sleep 0.5; done
  fi
  ping_ "$dir" || { log "MySQL:$port 没起来"; tail -8 "$dir/error.log" >&2; exit 1; }
  "$basedir/bin/mysql" --socket="$dir/mysqld.sock" -uroot -e "
    CREATE USER IF NOT EXISTS 'jinliang'@'%' IDENTIFIED BY 'jinliang';
    GRANT ALL PRIVILEGES ON *.* TO 'jinliang'@'%' WITH GRANT OPTION;
    FLUSH PRIVILEGES;" >/dev/null
  for db in jinliang_src jinliang_test jinliang_target; do
    "$basedir/bin/mysql" --socket="$dir/mysqld.sock" -uroot \
      -e "CREATE DATABASE IF NOT EXISTS $db CHARACTER SET utf8mb4" >/dev/null
  done
  log ":$port $("$basedir/bin/mysql" --socket="$dir/mysqld.sock" -uroot -N -e \
    "select concat(version(),' binlog=',@@log_bin,'/',@@binlog_format,'/',@@binlog_row_image)")"
}

case "$cmd" in
  up)
    [ -n "$port1" ] && [ -n "$port2" ] || { echo "缺端口" >&2; exit 2; }
    up_one my "$port1" 101
    up_one my2 "$port2" 102
    ;;
  down)
    for d in my my2; do
      [ -f "$BASE/$d/mysqld.sock" ] && "$basedir/bin/mysqladmin" --socket="$BASE/$d/mysqld.sock" -uroot shutdown >/dev/null 2>&1 || true
    done
    log "已停止"
    ;;
esac
