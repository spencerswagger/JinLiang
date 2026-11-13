#!/usr/bin/env bash
# 为无主键变体矩阵（test/nopk-matrix.yaml）起真库。
#
# 只有 CI 用（release 矩阵 49 个 job 共用这一段，免得把容器编排抄 49 遍）。
# 本地要跑矩阵格用 `scripts/local_stack.sh up`，别用这个。
#
#   nopk_dbs.sh cross <mysql版> <pg版>   起 PG 源/目标（同版本）+ MySQL 源/目标（同版本）
#   nopk_dbs.sh pg    <源版>   <目标版>  只起 PG 源/目标（版本可以不同）
#   nopk_dbs.sh mysql <mysql版>          只起 MySQL 源/目标
#
# 三个子命令用的是同一段容器编排：cross = pg + mysql 的组合。PR 门禁跑 pg + mysql，
# release/nightly 跑 cross 与 pg —— 所以这段编排每个 PR 都会被真跑一遍，不是「只在打 tag 时
# 才有机会发现写错」。
#
# 端口固定（一个 job 一个 runner，不会撞）：PG 55442/55443、MySQL 55306/55307。
# 就绪判据都是「真查询」，不用 pg_isready / mysqladmin ping：
#   - PG：initdb 期间会先起一个临时实例应答，所以要求**连续两次**真查询都成功；
#   - MySQL：mysqladmin ping 在「拒绝访问」时也返回 0，所以用 select 1。
set -euo pipefail

PG_SRC_PORT=55442
PG_DST_PORT=55443
MY_SRC_PORT=55306
MY_DST_PORT=55307

wait_pg() { # <容器名>
  local c=$1 ok=0
  for _ in $(seq 1 90); do
    if docker exec "$c" psql -U jinliang -d postgres -tAc 'select 1' >/dev/null 2>&1; then
      sleep 2
      if docker exec "$c" psql -U jinliang -d postgres -tAc 'select 1' >/dev/null 2>&1; then ok=1; break; fi
    fi
    sleep 2
  done
  if [ "$ok" != 1 ]; then echo "--- $c 没起来 ---"; docker logs "$c"; exit 1; fi
  local wl
  wl=$(docker exec "$c" psql -U jinliang -d postgres -tAc 'show wal_level')
  echo "$c: wal_level=$wl"
  [ "$wl" = "logical" ] || { echo "❌ $c 的 wal_level=$wl（要 logical）"; exit 1; }
}

wait_mysql() { # <容器名>
  local c=$1 ok=0
  for _ in $(seq 1 120); do
    if docker exec "$c" mysql -h127.0.0.1 -P3306 -uroot -proot -N -e 'select 1' >/dev/null 2>&1; then ok=1; break; fi
    sleep 2
  done
  if [ "$ok" != 1 ]; then echo "--- $c 没起来 ---"; docker logs "$c"; exit 1; fi
  # 源端必须能解出无主键表的整行旧镜像，否则测的是 MySQL 配置而不是我们的行为。
  local img
  img=$(docker exec "$c" mysql -h127.0.0.1 -P3306 -uroot -proot -N -B -e 'select @@binlog_row_image')
  echo "$c: binlog_row_image=$img"
  [ "$img" = "FULL" ] || { echo "❌ $c 的 binlog_row_image=$img（要 FULL）"; exit 1; }
}

pg_up() { # <容器名> <版本> <宿主端口> <库名>
  local c=$1 ver=$2 port=$3 db=$4
  docker rm -f "$c" >/dev/null 2>&1 || true
  docker run -d --name "$c" -p "$port:5432" \
    -e POSTGRES_USER=jinliang -e POSTGRES_PASSWORD=jinliang -e POSTGRES_DB="$db" \
    "postgres:$ver" \
    -c wal_level=logical -c max_replication_slots=40 -c max_wal_senders=40
  wait_pg "$c"
}

mysql_up() { # <容器名> <版本> <宿主端口> <库名> <server-id>
  local c=$1 ver=$2 port=$3 db=$4 sid=$5
  for attempt in 1 2 3; do
    docker rm -f "$c" >/dev/null 2>&1 || true
    if docker run -d --name "$c" -p "$port:3306" \
      -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE="$db" \
      -e MYSQL_USER=jinliang -e MYSQL_PASSWORD=jinliang \
      "mysql:$ver" \
      --server-id="$sid" --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL; then
      break
    fi
    echo "第 $attempt 次起 $c 失败，5s 后重试"; sleep 5
  done
  wait_mysql "$c"
  docker exec "$c" mysql -h127.0.0.1 -P3306 -uroot -proot -e "
    GRANT ALL PRIVILEGES ON *.* TO 'jinliang'@'%' WITH GRANT OPTION;
    CREATE DATABASE IF NOT EXISTS $db; FLUSH PRIVILEGES;" >/dev/null
}

case "${1:-}" in
  pg)
    pg_up pg-src "${2:?源版本}" "$PG_SRC_PORT" jinliang_src
    pg_up pg-dst "${3:?目标版本}" "$PG_DST_PORT" jinliang_test
    ;;
  mysql)
    mysql_up my-src "${2:?MySQL 版本}" "$MY_SRC_PORT" jinliang_src 1
    mysql_up my-dst "${2:?MySQL 版本}" "$MY_DST_PORT" jinliang_target 2
    ;;
  cross)
    pg_up pg-src "${3:?PG 版本}" "$PG_SRC_PORT" jinliang_src
    pg_up pg-dst "${3:?PG 版本}" "$PG_DST_PORT" jinliang_test
    mysql_up my-src "${2:?MySQL 版本}" "$MY_SRC_PORT" jinliang_src 1
    mysql_up my-dst "${2:?MySQL 版本}" "$MY_DST_PORT" jinliang_target 2
    ;;
  *)
    echo "用法: $0 {pg <源版> <目标版> | mysql <mysql版> | cross <mysql版> <pg版>}" >&2
    exit 64
    ;;
esac
echo "nopk_dbs: 起好了（PG $PG_SRC_PORT/$PG_DST_PORT，MySQL $MY_SRC_PORT/$MY_DST_PORT）"
