# 三组合一致性矩阵（快照 / 增量分段）

- 生成：`scripts/matrix3.sh`（行数 ROWS=20000，PG 17.11 (Debian 17.11-0+deb13u1)，MySQL 8.0.28）
- 原始输出：`notes/matrix3/20261111/<组合>/console.log` + `snapshot.log` / `incremental.log` / `verify.log` / `result.json`
- 复跑：`scripts/matrix3.sh`

| 组合 | 目标表名 | 源行数 | 目标行数 | 源 md5 | 目标 md5 | 快照(s) | 增量(s) | 结论 |
|---|---|---|---|---|---|---|---|---|
| MySQL → MySQL | `jinliang_target.acc_mm` | 20000 | 20000 | `330ccea0293f33177530c2a6933dad45` | `330ccea0293f33177530c2a6933dad45` | 2.02 | 0.32 | pass（一致） |
| MySQL → PG | `public.acc_mp` | 20000 | 20000 | `330ccea0293f33177530c2a6933dad45` | `330ccea0293f33177530c2a6933dad45` | 0.48 | 0.14 | pass（一致） |
| PG → MySQL | `jinliang_target.acc_pm` | 20000 | 20000 | `330ccea0293f33177530c2a6933dad45` | `330ccea0293f33177530c2a6933dad45` | 2.12 | 0.54 | pass（一致） |
