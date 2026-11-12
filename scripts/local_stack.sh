#!/usr/bin/env bash
# 本地栈：PG（逻辑复制）+ 两个 MySQL 实例，给门禁的集成用例/验收脚本用。
#
# 为什么不是 docker：沙盒里没有 docker。为什么不是「起一次就够」：每次 execute 都是新沙盒，
# 进程不一定跨调用存活；数据目录放在工作区（.localdb/），所以只有第一次 initdb/--initialize，
# 之后每次只是启动（几秒）。
#
# 端口故意不用默认值（5432/3306）：宿主网络是多个 agent 共用的，默认端口上常年有别人的实例，
# 撞上去会得到「连上了但不是我的库」这种最难查的错。
#
#   scripts/local_stack.sh up      # 起栈并把 DSN 写进 .localdb/stack.env
#   scripts/local_stack.sh down
#   . .localdb/stack.env           # 之后直接用 $JINLIANG_TEST_*
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="$ROOT/.localdb"
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
MYBASE=${MYBASE:-/.toolchain/mysql}
# 端口再挪开一档：宿主网络是多个 agent 共用的，55432/53306 这一组也被别人占过
# （撞上别人的实例比连不上更难查：库名对不上、wal_level 是 replica，报错还指向别处）。
PGPORT=${PGPORT:-55442}
MYPORT=${MYPORT:-53316}
MYPORT2=${MYPORT2:-53317}
ENVF="$BASE/stack.env"

log() { printf '[stack] %s\n' "$*" >&2; }

# 沙盒里 /etc 是 700 root：切到非 root 后连 /etc/passwd 都读不到（su 直接 Critical error）。
# 只放宽目录位（一次性沙盒，退出即恢复）。
chmod 755 /etc 2>/dev/null || true
as_pg() { setpriv --reuid=postgres --regid=postgres --clear-groups "$@"; }

pg_up() {
  mkdir -p "$BASE/pg_sock"
  if [ ! -s "$BASE/pg/PG_VERSION" ]; then
    log "初始化 PG 数据目录（第一次，几秒）"
    rm -rf "$BASE/pg"; mkdir -p "$BASE/pg"
    chown -R postgres:postgres "$BASE"
    as_pg "$PGBIN/initdb" -D "$BASE/pg" -U jinliang --auth=trust --encoding=UTF8 >/dev/null
  fi
  chown -R postgres:postgres "$BASE/pg" "$BASE/pg_sock" 2>/dev/null || true
  if ! "$PGBIN/pg_isready" -h 127.0.0.1 -p "$PGPORT" -q 2>/dev/null; then
    if [ -f "$BASE/pg/postmaster.pid" ]; then
      pid=$(head -1 "$BASE/pg/postmaster.pid" 2>/dev/null || echo 0)
      kill -0 "$pid" 2>/dev/null || rm -f "$BASE/pg/postmaster.pid"
    fi
    as_pg "$PGBIN/pg_ctl" -D "$BASE/pg" -l "$BASE/pg.log" -w -t 60 \
      -o "-p $PGPORT -c listen_addresses=127.0.0.1 -c wal_level=logical \
           -c max_replication_slots=40 -c max_wal_senders=40 -c max_connections=200 \
           -c unix_socket_directories=$BASE/pg_sock" start >/dev/null
  fi
  for db in jinliang_test jinliang_src; do
    "$PGBIN/psql" "postgres://jinliang@127.0.0.1:$PGPORT/postgres" -tAc "select 1 from pg_database where datname='$db'" 2>/dev/null | grep -q 1 \
      || "$PGBIN/createdb" -h 127.0.0.1 -p "$PGPORT" -U jinliang "$db"
  done
  local wl
  wl=$("$PGBIN/psql" "postgres://jinliang@127.0.0.1:$PGPORT/postgres" -tAc 'show wal_level' 2>/dev/null)
  # 端口上「有人应答」不等于「应答的是我的库」：撞到别人的实例时，最典型的症状就是
  # wal_level 不是 logical（于是 PG→* 的增量直接报「源库没开逻辑变更流」）。
  if [ "$wl" != "logical" ]; then
    log "PG :$PGPORT 上不是本栈的库（wal_level=$wl）——端口被别的实例占了，换 PGPORT 再来"
    exit 1
  fi
  log "pg :$PGPORT wal_level=$wl"
}

my_ping() { "$MYBASE/bin/mysqladmin" --socket="$1/mysqld.sock" -uroot ping >/dev/null 2>&1; }

my_up() { # $1=目录名 $2=端口 $3=server-id
  local dir="$BASE/$1" port="$2" sid="$3"
  if [ ! -d "$dir/mysql" ]; then
    log "初始化 MySQL:$port 数据目录（第一次，~10s）"
    rm -rf "$dir"; mkdir -p "$dir"
    "$MYBASE/bin/mysqld" --initialize-insecure --user=root --basedir="$MYBASE" --datadir="$dir" \
      --log-error="$BASE/$1-init.log" >/dev/null 2>&1
  fi
  if ! my_ping "$dir"; then
    # 上一次沙盒留下的 socket/pid 锁：进程没了但文件还在，MySQL 会拒绝启动
    # （"Another process with pid N is using unix socket file"）。
    rm -f "$dir/mysqld.sock" "$dir/mysqld.sock.lock" "$dir/mysqld.pid"
    setsid nohup "$MYBASE/bin/mysqld" \
      --user=root --basedir="$MYBASE" --datadir="$dir" \
      --port="$port" --socket="$dir/mysqld.sock" --pid-file="$dir/mysqld.pid" \
      --bind-address=127.0.0.1 --mysqlx=OFF --skip-name-resolve \
      --server-id="$sid" --log-bin="$dir/mysql-bin" --binlog-format=ROW --binlog-row-image=FULL \
      --log-error="$dir/error.log" </dev/null >>"$dir/stdout.log" 2>&1 &
    for _ in $(seq 1 120); do my_ping "$dir" && break; sleep 0.5; done
  fi
  my_ping "$dir" || { log "MySQL:$port 没起来，看 $dir/error.log"; tail -5 "$dir/error.log" >&2; exit 1; }
  # root 只走 socket（--initialize-insecure 只建 root@localhost，TCP 会被 1130 拒）。
  "$MYBASE/bin/mysql" --socket="$dir/mysqld.sock" -uroot -e "
    CREATE USER IF NOT EXISTS 'jinliang'@'%' IDENTIFIED BY 'jinliang';
    GRANT ALL PRIVILEGES ON *.* TO 'jinliang'@'%' WITH GRANT OPTION;
    FLUSH PRIVILEGES;" >/dev/null
  for db in jinliang_src jinliang_test jinliang_target; do
    "$MYBASE/bin/mysql" --socket="$dir/mysqld.sock" -uroot \
      -e "CREATE DATABASE IF NOT EXISTS $db CHARACTER SET utf8mb4" >/dev/null
  done
  log "mysql :$port $("$MYBASE/bin/mysql" --socket="$dir/mysqld.sock" -uroot -N -e \
    "select concat(version(),' binlog=',@@log_bin,'/',@@binlog_format,'/',@@binlog_row_image)")"
}

case "${1:-up}" in
  up)
    pg_up
    my_up my  "$MYPORT"  101
    my_up my2 "$MYPORT2" 102
    mkdir -p "$BASE"
    cat > "$ENVF" <<ENV
export JINLIANG_TEST_PG_DSN=postgres://jinliang:jinliang@127.0.0.1:$PGPORT/jinliang_test?sslmode=disable
export JINLIANG_TEST_PG_SRC_DSN=postgres://jinliang:jinliang@127.0.0.1:$PGPORT/jinliang_src?sslmode=disable
export JINLIANG_TEST_MYSQL_DSN=mysql://jinliang:jinliang@127.0.0.1:$MYPORT/jinliang_test
export JINLIANG_TEST_MYSQL_SRC_DSN=mysql://jinliang:jinliang@127.0.0.1:$MYPORT/jinliang_src
export JINLIANG_TEST_MYSQL_DST_DSN=mysql://jinliang:jinliang@127.0.0.1:$MYPORT2/jinliang_target
export PATH=$MYBASE/bin:$PGBIN:\$PATH
ENV
    log "写好 $ENVF"
    ;;
  down)
    "$MYBASE/bin/mysqladmin" --socket="$BASE/my/mysqld.sock" -uroot shutdown >/dev/null 2>&1 || true
    "$MYBASE/bin/mysqladmin" --socket="$BASE/my2/mysqld.sock" -uroot shutdown >/dev/null 2>&1 || true
    as_pg "$PGBIN/pg_ctl" -D "$BASE/pg" -m fast stop >/dev/null 2>&1 || true
    log "已停止"
    ;;
  status)
    "$PGBIN/pg_isready" -h 127.0.0.1 -p "$PGPORT" || true
    my_ping "$BASE/my"  && echo "mysql :$MYPORT 在跑"  || echo "mysql :$MYPORT 没跑"
    my_ping "$BASE/my2" && echo "mysql :$MYPORT2 在跑" || echo "mysql :$MYPORT2 没跑"
    ;;
  *) echo "用法: $0 {up|down|status}" >&2; exit 2 ;;
esac
