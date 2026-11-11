# 三组合一致性矩阵（快照 / 增量分段）

- 生成：`scripts/matrix3.sh`（行数 ROWS=20000，PG 17.11 (Debian 17.11-0+deb13u1)，MySQL 8.0.28）
- 原始输出：`notes/matrix3/versions/9.7.4/<组合>/console.log` + `snapshot.log` / `incremental.log` / `verify.log` / `result.json`
- 复跑：`scripts/matrix3.sh`

| 组合 | 目标表名 | 源行数 | 目标行数 | 源 md5 | 目标 md5 | 快照(s) | 增量(s) | 结论 |
|---|---|---|---|---|---|---|---|---|
| MySQL → MySQL | `jinliang_target.acc_mm` | 20000 | 20000 | `c99f849412d7f83a8f4335fb1b546838a5843d2c1f47d677e4fcab181de9422e` | `c99f849412d7f83a8f4335fb1b546838a5843d2c1f47d677e4fcab181de9422e` | 1.59 | 0.31 | pass（一致） |
| MySQL → PG | `public.acc_mp` | 20000 | 20000 | `c99f849412d7f83a8f4335fb1b546838a5843d2c1f47d677e4fcab181de9422e` | `c99f849412d7f83a8f4335fb1b546838a5843d2c1f47d677e4fcab181de9422e` | 0.37 | 0.14 | pass（一致） |
| PG → MySQL | `jinliang_target.acc_pm` | 20000 | 20000 | `c99f849412d7f83a8f4335fb1b546838a5843d2c1f47d677e4fcab181de9422e` | `c99f849412d7f83a8f4335fb1b546838a5843d2c1f47d677e4fcab181de9422e` | 1.60 | 0.30 | pass（一致） |
