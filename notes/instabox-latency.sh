#!/bin/bash
# 端到端延迟探测：源端插一行带标记的 order，轮询目标端直到可见，记录毫秒。
# 注意：每次探测走 `docker exec psql`，单次查询本身有 ~30-40ms 进程开销，
# 所以这里测到的是「真延迟 + 探测开销」的上界，不是引擎内 lag_ms。
set -u
N=${1:-30}
psrc() { docker exec -i pgsrc psql -U jinliang -d src -tAc "$1"; }
ptgt() { docker exec -i pgreport psql -U jinliang -d report -tAc "$1"; }
ms() { date +%s%3N; }
: > /tmp/lat.txt
for i in $(seq 1 $N); do
  ID=$((400000 + i))
  psrc "insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) values ($ID, 1, 999, 0, 'cn', 'probe-$i', now()) on conflict (id) do update set note='probe-$i', updated_at=now();" >/dev/null
  T0=$(ms)
  for j in $(seq 1 200); do
    C=$(ptgt "select count(*) from orders where id=$ID")
    [ "$C" = "1" ] && break
    sleep 0.005
  done
  T1=$(ms)
  echo $((T1-T0)) >> /tmp/lat.txt
done
python3 - <<'PY'
vals=sorted(int(l) for l in open('/tmp/lat.txt'))
n=len(vals)
def pct(p):
    k=max(0,min(n-1,int(round(p/100*(n-1)))))
    return vals[k]
print(f"n={n} min={vals[0]}ms p50={pct(50)}ms p90={pct(90)}ms p99={pct(99)}ms max={vals[-1]}ms mean={sum(vals)/n:.1f}ms")
PY
