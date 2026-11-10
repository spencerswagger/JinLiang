#!/usr/bin/env python3
"""InstaBox 真机测试harness（在沙箱里跑）。

子命令：
  s5          不支持矩阵（PG→MySQL / MySQL→PG / MySQL→MySQL）点 run 的原文
  ops         verify 三级（含故意改/删一行）、doctor、gc --dry-run、冲突/死信
  ui          once 模式走完 建连接→建任务→preflight→run→SSE 到 done；重启 serve 后状态
  s2          kill -9 中途杀 → 重启续跑（重搬量 / 重复 / md5）
  s3          DDL 白名单四类 + 非白名单
  s4          大事务 5 万行 + 超限诚实报错
  stab        3 轮全量+增量，看 goroutine/内存/租约残留
  err         打印最近一次 run 的 last_error
"""
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

B = "http://127.0.0.1:8080"
TOK = "demo"
HOME = "/data/home"
SRC = "postgres://jinliang:jinliang@127.0.0.1:55432/src?sslmode=disable"
CTL = f"{HOME}/data"


def api(method, path, body=None, timeout=30):
    req = urllib.request.Request(B + path, method=method)
    req.add_header("Authorization", "Bearer " + TOK)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw or b"{}")
        except Exception:
            return e.code, {"raw": raw.decode(errors="replace")}


def sh(cmd, timeout=120):
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    return (p.stdout or "") + (p.stderr or "")


def psql(ctr, db, sql):
    return sh(f'docker exec -i {ctr} psql -U jinliang -d {db} -tAc {sq(sql)}').strip()


def sq(s):
    return "'" + s.replace("'", "'\\''") + "'"


def mysql(ctr, sql, db="report"):
    return sh(f"docker exec -i {ctr} mysql -ujinliang -pjinliang -N -B {db} -e {sq(sql)}").strip()


def newdb(db):
    psql("pgreport", "report", f"SELECT 1")  # touch
    sh(f'docker exec -i pgreport psql -U jinliang -d report -tAc "DROP DATABASE IF EXISTS {db}"')
    sh(f'docker exec -i pgreport psql -U jinliang -d report -tAc "CREATE DATABASE {db}"')


def conn(name, dsn):
    st, d = api("POST", "/api/connections", {"name": name, "dsn": dsn})
    if st not in (200, 201):
        print("!! conn failed", st, d)
    return d["connection"]["id"], d.get("test", {})


def job(name, src_id, tgt_id, tables, mode="continuous+initial", extra=None, target_schema="public"):
    spec = {
        "version": 1,
        "source": {"kind": "postgres", "dsn": "env:X",
                   "scope": {"schema": "public", "include": tables}},
        "target": {"kind": "postgres", "dsn": "env:X", "schema": target_schema,
                   "on_exists": "append", "create_indexes": "defer"},
        "mode": {"frequency": mode},
        "content": {"tables": {t: {} for t in tables}},
    }
    if extra:
        for k, v in extra.items():
            spec.setdefault(k, {}).update(v) if isinstance(v, dict) else spec.__setitem__(k, v)
    st, d = api("POST", "/api/jobs", {"name": name, "source_id": src_id,
                                     "target_id": tgt_id, "enabled": True, "spec": spec})
    if st not in (200, 201):
        print("!! job failed", st, json.dumps(d, ensure_ascii=False))
        return None, d
    return d["id"], d


def runs(job_id):
    st, d = api("GET", f"/api/jobs/{job_id}")
    return (d.get("runs") or [{}])[0]


def wait_phase(job_id, want, timeout=180, min_rows=0):
    t0 = time.time()
    last = {}
    while time.time() - t0 < timeout:
        last = runs(job_id)
        ph = last.get("phase")
        if ph == want and (last.get("rows_done") or 0) >= min_rows:
            return last, time.time() - t0
        if ph == "failed":
            return last, time.time() - t0
        time.sleep(0.05)
    return last, time.time() - t0


def run_job(job_id):
    return api("POST", f"/api/jobs/{job_id}/run", {})


def sse(job_id, seconds=15, stop_on_done=True):
    req = urllib.request.Request(B + f"/api/jobs/{job_id}/events")
    req.add_header("Authorization", "Bearer " + TOK)
    evs = []
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            while time.time() - t0 < seconds:
                line = r.readline().decode(errors="replace").strip()
                if not line.startswith("data: "):
                    continue
                try:
                    ev = json.loads(line[6:])
                except Exception:
                    continue
                evs.append(ev)
                if stop_on_done and ev.get("type") in ("done",):
                    break
    except Exception:
        pass
    return evs


def serve_pid():
    out = sh("pgrep -x jinliang | head -1").strip()
    return int(out) if out.isdigit() else None


def start_serve():
    sh(f'cd {HOME} && setsid nohup ./jinliang serve --listen 127.0.0.1:8080 '
       f'--data-dir {HOME}/data --api-token demo </dev/null >>{HOME}/serve.log 2>&1 & disown')
    for _ in range(60):
        time.sleep(0.3)
        try:
            with urllib.request.urlopen(B + "/api/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
    return False


def stop_serve(sig=9):
    sh(f"pkill -{sig} -x jinliang")
    for _ in range(40):
        time.sleep(0.15)
        if serve_pid() is None:
            return True
    return serve_pid() is None


def proc_stats():
    pid = serve_pid()
    if not pid:
        return {}
    st = open(f"/proc/{pid}/status").read()
    rss = re.search(r"VmRSS:\s+(\d+)", st)
    thr = re.search(r"Threads:\s+(\d+)", st)
    fd = len(os.listdir(f"/proc/{pid}/fd"))
    return {"pid": pid, "rss_mb": round(int(rss.group(1)) / 1024, 1) if rss else None,
            "threads": int(thr.group(1)) if thr else None, "fds": fd}


def counts(ctr, db):
    return psql(ctr, db, "select count(*) from orders")


def md5(ctr, db):
    return psql(ctr, db,
                "select md5(string_agg(id||':'||customer_id||':'||amount_cents||':'||status||':'||region, ',' order by id)) from orders")


def report(title, **kw):
    print(json.dumps({"case": title, **kw}, ensure_ascii=False))


# ─────────────────────────── 场景 ───────────────────────────

def cmd_s5():
    """PG→MySQL / MySQL→PG / MySQL→MySQL 各点一次 run。"""
    out = []
    pg = conn("s5-pg-src", SRC)[0]
    my_sink = conn("s5-my-report", "mysql://jinliang:jinliang@127.0.0.1:53306/report")[0]
    my_src = conn("s5-my-src", "mysql://jinliang:jinliang@127.0.0.1:53306/srcdb")[0]

    # MySQL 源库准备（同一个容器，建个库当"MySQL 源"）
    mysql("mysink", "CREATE DATABASE IF NOT EXISTS srcdb")
    mysql("mysink", "CREATE TABLE IF NOT EXISTS srcdb.t1 (id INT PRIMARY KEY, v INT)", "srcdb")
    mysql("mysink", "INSERT IGNORE INTO srcdb.t1 VALUES (1,1),(2,2)", "srcdb")

    combos = [("pg->mysql", pg, my_sink, ["orders"]),
              ("mysql->pg", my_src, pg, ["t1"]),
              ("mysql->mysql", my_src, my_sink, ["t1"])]
    for label, s, t, tables in combos:
        jid, jd = job(f"s5-{label}", s, t, tables)
        if not jid:
            out.append({"combo": label, "job_create": "failed", "detail": jd})
            continue
        st, d = api("POST", f"/api/jobs/{jid}/run", {})
        out.append({"combo": label, "http": st, "resp": d})
    print(json.dumps(out, ensure_ascii=False, indent=1))


def cmd_ops():
    """verify 三级 + 故意改/删一行 + doctor + gc + 冲突/死信。"""
    res = {}
    tgt = "postgres://jinliang:jinliang@127.0.0.1:55433/report?sslmode=disable"
    res["verify_clean"] = sh(f'cd {HOME} && ./jinliang verify --source "{SRC}" --target "{tgt}" '
                             f'--tables orders,customers --job-key lqgimr4rdl 2>&1 | tail -12')
    # 故意改一行（目标端）
    psql("pgreport", "report", "update orders set amount_cents = amount_cents + 1 where id = 12345")
    res["verify_one_row_changed"] = sh(f'cd {HOME} && ./jinliang verify --source "{SRC}" --target "{tgt}" '
                                       f'--tables orders --job-key lqgimr4rdl 2>&1 | tail -14')
    psql("pgreport", "report", "update orders set amount_cents = amount_cents - 1 where id = 12345")
    # 故意删一行（目标端）
    row = psql("pgreport", "report", "select id from orders where id = 777")
    psql("pgreport", "report", "delete from orders where id = 777")
    res["verify_one_row_deleted"] = sh(f'cd {HOME} && ./jinliang verify --source "{SRC}" --target "{tgt}" '
                                       f'--tables orders --job-key lqgimr4rdl 2>&1 | tail -14')
    # 补回来，别污染后面的用例
    psql("pgreport", "report",
         "insert into orders select * from dblink('dbname=src host=127.0.0.1 port=55432 user=jinliang password=jinliang',"
         "'select * from orders where id=777') as t(id bigint,customer_id bigint,amount_cents bigint,status smallint,region text,note text,updated_at timestamptz) "
         "on conflict do nothing") if False else None
    res["doctor"] = sh(f'cd {HOME} && ./jinliang doctor --data-dir {CTL} 2>&1 | tail -22')
    # 孤儿 slot：造一个不在 _jl_object 里的 slot
    psql("pgsrc", "src", "select pg_drop_replication_slot('jl_orphan_1') from pg_replication_slots where slot_name='jl_orphan_1'")
    psql("pgsrc", "src", "select pg_create_logical_replication_slot('jl_orphan_1','pgoutput')")
    res["gc_dryrun_with_orphan_slot"] = sh(f'cd {HOME} && ./jinliang gc --target "{tgt}" --source "{SRC}" '
                                           f'--instance 3uajpyntdjfydgeehylixoef5a 2>&1 | tail -20')
    res["conflicts_cli"] = sh(f'cd {HOME} && ./jinliang conflicts list --target "{tgt}" --job-key lqgimr4rdl 2>&1 | tail -6')
    res["deadletter_cli"] = sh(f'cd {HOME} && ./jinliang dead-letter list --target "{tgt}" 2>&1 | tail -6')
    res["target_state_tables"] = psql("pgreport", "report",
                                      "select table_name from information_schema.tables where table_schema='public' and table_name like '\\_jl\\_%' order by 1")
    print(json.dumps(res, ensure_ascii=False, indent=1))


def cmd_ui():
    """once 模式：SSE 到 done + 重启 serve 后状态还在。"""
    res = {}
    newdb("rep_ui")
    dsn = "postgres://jinliang:jinliang@127.0.0.1:55433/rep_ui?sslmode=disable"
    tgt, t = conn("ui-tgt", dsn)
    jid, jd = job("ui-once", conn("ui-src", SRC)[0], tgt, ["orders", "customers"], mode="once")
    res["preflight"] = api("POST", f"/api/jobs/{jid}/preflight", {})[1].get("can_start")
    t0 = time.time()
    res["run"] = run_job(jid)[1]
    evs = sse(jid, seconds=90)
    res["sse_count"] = len(evs)
    res["sse_tail"] = [(e.get("type"), e.get("phase"), e.get("rows_done"), e.get("finish_reason")) for e in evs[-4:]]
    res["elapsed_s"] = round(time.time() - t0, 2)
    res["run_after_done"] = runs(jid)
    # 重启 serve，状态是否还在
    res["stop_ok"] = stop_serve(15)
    res["start_ok"] = start_serve()
    st, d = api("GET", f"/api/jobs/{jid}")
    res["after_restart"] = {"job_readable": st == 200,
                            "runs": [(r.get("phase"), r.get("rows_done"), r.get("finish_reason")) for r in d.get("runs", [])][:3],
                            "spec_tables": list((d.get("spec", {}).get("content", {}).get("tables") or {}).keys())}
    res["counts_after_restart"] = {"src": counts("pgsrc", "src"), "tgt": counts("pgreport", "rep_ui")}
    print(json.dumps(res, ensure_ascii=False, indent=1))


def cmd_s2():
    """kill -9 打断全量 → 重启续跑：重搬量 / 重复 / md5。"""
    res = {}
    newdb("rep_kill")
    dsn = "postgres://jinliang:jinliang@127.0.0.1:55433/rep_kill?sslmode=disable"
    tgt, _ = conn("kill-tgt", dsn)
    # 放慢全量：chunk 1000 行、单连接
    jid, jd = job("kill-mid-snapshot", conn("kill-src", SRC)[0], tgt, ["orders", "customers"],
                  extra={"source": {"kind": "postgres", "dsn": "env:X",
                                    "scope": {"schema": "public", "include": ["orders", "customers"]},
                                    "snapshot": {"chunk_rows": 1000, "conns": 1}}})
    res["job"] = jid
    run_job(jid)
    # 等进入 snapshot 且有进度
    t0 = time.time()
    while time.time() - t0 < 60:
        r = runs(jid)
        if r.get("phase") == "snapshot" and (r.get("rows_done") or 0) > 0:
            break
        time.sleep(0.05)
    res["phase_before_kill"] = {k: r.get(k) for k in ("phase", "rows_done", "lag_bytes")}
    res["target_rows_at_kill"] = counts("pgreport", "rep_kill")
    res["chunks_done_at_kill"] = psql("pgreport", "rep_kill",
                                      "select count(*) from _jl_chunk where done") if True else None
    res["kill_ok"] = stop_serve(9)
    res["target_rows_after_kill"] = counts("pgreport", "rep_kill")
    res["start_ok"] = start_serve()
    time.sleep(1)
    res["run_state_after_restart"] = {k: runs(jid).get(k) for k in ("phase", "rows_done", "finish_reason", "last_error")}
    # 再跑一次
    t1 = time.time()
    res["rerun"] = run_job(jid)[1]
    r, dt = wait_phase(jid, "streaming", timeout=180)
    res["rerun_phase"] = {k: r.get(k) for k in ("phase", "rows_done", "finish_reason")}
    res["rerun_seconds"] = round(dt, 2)
    time.sleep(1)
    res["final"] = {"src": counts("pgsrc", "src"), "tgt": counts("pgreport", "rep_kill"),
                    "md5_src": md5("pgsrc", "src"), "md5_tgt": md5("pgreport", "rep_kill")}
    res["dup_check"] = psql("pgreport", "rep_kill",
                            "select count(*), count(distinct id) from orders")
    res["chunks_left"] = psql("pgreport", "rep_kill", "select count(*), count(*) filter (where done) from _jl_chunk")
    print(json.dumps(res, ensure_ascii=False, indent=1))


def cmd_s3():
    """DDL 白名单四类 + 非白名单。"""
    res = {}
    newdb("rep_ddl")
    dsn = "postgres://jinliang:jinliang@127.0.0.1:55433/rep_ddl?sslmode=disable"
    tgt, _ = conn("ddl-tgt", dsn)
    jid, _ = job("ddl-watch", conn("ddl-src", SRC)[0], tgt, ["orders", "customers"])
    res["job"] = jid
    run_job(jid)
    r, _ = wait_phase(jid, "streaming", timeout=90)
    res["baseline"] = {k: r.get(k) for k in ("phase", "rows_done")}

    def step(label, sql, probe_sql_src, probe_sql_tgt):
        psql("pgsrc", "src", sql)
        time.sleep(0.5)
        psql("pgsrc", "src", probe_sql_src)
        time.sleep(1.5)
        r2 = runs(jid)
        res[label] = {
            "phase": r2.get("phase"),
            "last_error": (r2.get("last_error") or "")[:300],
            "tgt_cols": psql("pgreport", "rep_ddl",
                             "select string_agg(column_name||':'||data_type, ',' order by ordinal_position) "
                             "from information_schema.columns where table_schema='public' and table_name='orders'"),
            "probe_tgt": probe_sql_tgt,
            "probe_val": psql("pgreport", "rep_ddl", probe_sql_tgt),
        }

    step("add_column",
         "alter table orders add column vip boolean default false",
         "update orders set vip = true, updated_at = now() where id = 5000",
         "select vip from orders where id = 5000")
    step("widen_column",
         "alter table orders alter column note type varchar(500)",
         "update orders set note = repeat('x', 300), updated_at = now() where id = 5001",
         "select length(note) from orders where id = 5001")
    step("create_index",
         "create index idx_orders_customer on orders (customer_id)",
         "insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) values (900001,7,1,0,'cn','idx',now())",
         "select count(*) from orders where id = 900001")
    step("type_change_reject",
         "alter table orders alter column amount_cents type numeric(12,2)",
         "insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) values (900002,7,12.34,0,'cn','tc',now())",
         "select count(*) from orders where id = 900002")
    step("rename_reject",
         "alter table customers rename column city to town",
         "insert into customers (id,name,town) values (990001,'n','sh')",
         "select count(*) from customers where id = 990001")
    print(json.dumps(res, ensure_ascii=False, indent=1))


def cmd_s4():
    """大事务 5 万行 + 超限诚实报错。"""
    res = {}
    # A：正常上限，5 万行单事务
    newdb("rep_tx")
    dsn = "postgres://jinliang:jinliang@127.0.0.1:55433/rep_tx?sslmode=disable"
    tgt, _ = conn("tx-tgt", dsn)
    jid, _ = job("tx-50k", conn("tx-src", SRC)[0], tgt, ["orders"])
    run_job(jid)
    r, _ = wait_phase(jid, "streaming", timeout=90)
    res["baseline"] = {k: r.get(k) for k in ("phase", "rows_done")}
    t0 = time.time()
    psql("pgsrc", "src", "begin; insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) "
                        "select 500000+g, 1, 1, 0, 'cn', 'bigtx', now() from generate_series(1,50000) g; commit;")
    t_commit = time.time()
    exp = psql("pgsrc", "src", "select count(*) from orders")
    got = ""
    for _ in range(600):
        got = counts("pgreport", "rep_tx")
        if got == exp:
            break
        time.sleep(0.1)
    res["bigtx"] = {"sql_seconds": round(t_commit - t0, 2), "catchup_seconds": round(time.time() - t_commit, 2),
                    "src": exp, "tgt": got, "phase": runs(jid).get("phase"),
                    "last_error": (runs(jid).get("last_error") or "")[:200]}

    # B：把单事务上限压到 1 MiB，再发一个 ~3 MB 的事务，看是否诚实报错且不落半截
    newdb("rep_tx2")
    dsn2 = "postgres://jinliang:jinliang@127.0.0.1:55433/rep_tx2?sslmode=disable"
    tgt2, _ = conn("tx2-tgt", dsn2)
    jid2, jd2 = job("tx-oversize", conn("tx2-src", SRC)[0], tgt2, ["orders"],
                    extra={"reliability": {"batch_rows": 500, "tx_oversize": {"mode": "fail", "bytes": 1048576}}})
    run_job(jid2)
    r2, _ = wait_phase(jid2, "streaming", timeout=90)
    res["oversize_baseline"] = {k: r2.get(k) for k in ("phase", "rows_done")}
    before = counts("pgreport", "rep_tx2")
    psql("pgsrc", "src", "begin; insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) "
                        "select 600000+g, 1, 1, 0, 'cn', repeat('y',60), now() from generate_series(1,50000) g; commit;")
    time.sleep(6)
    after = counts("pgreport", "rep_tx2")
    r3 = runs(jid2)
    res["oversize_tx"] = {"rows_before": before, "rows_after": after,
                          "phase": r3.get("phase"), "rows_done": r3.get("rows_done"),
                          "last_error": (r3.get("last_error") or "")[:400]}
    print(json.dumps(res, ensure_ascii=False, indent=1))


def cmd_stab():
    """连续 3 轮全量+增量：泄漏观察。"""
    res = {"rounds": []}
    res["before"] = proc_stats()
    for i in range(1, 4):
        db = f"rep_stab{i}"
        newdb(db)
        dsn = f"postgres://jinliang:jinliang@127.0.0.1:55433/{db}?sslmode=disable"
        tgt, _ = conn(f"stab{i}-tgt", dsn)
        jid, _ = job(f"stab-{i}", conn(f"stab{i}-src", SRC)[0], tgt, ["orders", "customers"])
        t0 = time.time()
        run_job(jid)
        r, dt = wait_phase(jid, "streaming", timeout=180)
        full = round(time.time() - t0, 2)
        # 增量
        psql("pgsrc", "src", "insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) "
                             f"select 700000+{i}*100+g, 1, 5, 0, 'cn', 'stab', now() from generate_series(1,20) g;"
                             "update orders set amount_cents = amount_cents + 1, updated_at = now() "
                             f"where id between 2000 and 2009;"
                             "delete from orders where id between 300 and 304;")
        exp = psql("pgsrc", "src", "select count(*) from orders")
        t1 = time.time()
        got = ""
        for _ in range(300):
            if counts("pgreport", db) == exp:
                break
            time.sleep(0.05)
        got = counts("pgreport", db)
        res["rounds"].append({"round": i, "job": jid, "full_seconds": full,
                              "rows": r.get("rows_done"), "phase": r.get("phase"),
                              "incr_seconds": round(time.time() - t1, 2),
                              "src": exp, "tgt": got,
                              "md5_equal": md5("pgsrc", "src") == md5("pgreport", db),
                              "stats": proc_stats()})
    res["after"] = proc_stats()
    res["lease_rows"] = psql("pgreport", "rep_stab3", "select count(*) from _jl_lease")
    res["orphan_slots"] = psql("pgsrc", "src", "select count(*) from pg_replication_slots")
    res["slot_list"] = psql("pgsrc", "src", "select string_agg(slot_name||':'||coalesce(active::text,'?'), ',') from pg_replication_slots")
    res["job_count"] = api("GET", "/api/jobs")[1].get("jobs") and len(api("GET", "/api/jobs")[1]["jobs"])
    print(json.dumps(res, ensure_ascii=False, indent=1))


def cmd_err():
    js = api("GET", "/api/jobs")[1].get("jobs", [])
    out = {}
    for j in js:
        r = runs(j["id"])
        if r:
            out[j.get("name")] = {k: r.get(k) for k in ("phase", "rows_done", "last_error")}
    print(json.dumps(out, ensure_ascii=False, indent=1))




def cmd_s3b():
    """每类 DDL 单独一个干净任务，避免前一条把任务跑死影响后一条。"""
    cases = [
        ("add_column", "alter table orders add column vip boolean default false",
         "update orders set vip = true, updated_at = now() where id = 5000",
         "select coalesce(vip::text,'null') from orders where id=5000",
         "select count(*) from information_schema.columns where table_schema='public' and table_name='orders' and column_name='vip'"),
        ("widen_column", "alter table orders alter column note type varchar(500)",
         "update orders set note = repeat('x',300), updated_at = now() where id = 5001",
         "select length(note) from orders where id=5001",
         "select data_type||':'||coalesce(character_maximum_length::text,'-') from information_schema.columns where table_schema='public' and table_name='orders' and column_name='note'"),
        ("create_index", "create index idx_orders_customer on orders (customer_id)",
         "insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) values (900001,7,1,0,'cn','idx',now())",
         "select count(*) from orders where id=900001",
         "select count(*) from pg_indexes where schemaname='public' and indexname='idx_orders_customer'"),
        ("type_change", "alter table orders alter column amount_cents type numeric(12,2)",
         "insert into orders (id,customer_id,amount_cents,status,region,note,updated_at) values (900002,7,12.34,0,'cn','tc',now())",
         "select count(*) from orders where id=900002",
         "select data_type from information_schema.columns where table_schema='public' and table_name='orders' and column_name='amount_cents'"),
        ("rename_column", "alter table customers rename column city to town",
         "insert into customers (id,name,town) values (990001,'n','sh')",
         "select count(*) from customers where id=990001",
         "select string_agg(column_name,',') from information_schema.columns where table_schema='public' and table_name='customers'"),
    ]
    out = []
    for label, ddl, probe, probe_tgt, tgt_schema_sql in cases:
        db = "rep_ddl_" + label
        newdb(db)
        dsn = f"postgres://jinliang:jinliang@127.0.0.1:55433/{db}?sslmode=disable"
        tgt, _ = conn(db + "-tgt", dsn)
        jid, _ = job("ddl-" + label, conn(db + "-src", SRC)[0], tgt,
                     ["orders", "customers"] if label == "rename_column" else ["orders"])
        run_job(jid)
        r, _ = wait_phase(jid, "streaming", timeout=120)
        base = {k: r.get(k) for k in ("phase", "rows_done")}
        psql("pgsrc", "src", ddl)
        time.sleep(0.6)
        psql("pgsrc", "src", probe)
        time.sleep(2.0)
        r2 = runs(jid)
        out.append({
            "ddl": label,
            "baseline": base,
            "phase_after": r2.get("phase"),
            "rows_done": r2.get("rows_done"),
            "last_error": (r2.get("last_error") or "")[:260],
            "tgt_schema": psql("pgreport", db, tgt_schema_sql),
            "probe_tgt": psql("pgreport", db, probe_tgt),
        })
        # 恢复源端 DDL，别影响后面的用例
        undo = {"add_column": "alter table orders drop column if exists vip",
                "widen_column": "alter table orders alter column note type text",
                "create_index": "drop index if exists idx_orders_customer",
                "type_change": "alter table orders alter column amount_cents type bigint using amount_cents::bigint",
                "rename_column": "alter table customers rename column town to city"}[label]
        psql("pgsrc", "src", undo)
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"s5": cmd_s5, "ops": cmd_ops, "ui": cmd_ui, "s2": cmd_s2, "s3": cmd_s3,
     "s4": cmd_s4, "s3b": cmd_s3b, "stab": cmd_stab, "err": cmd_err}[cmd]()
