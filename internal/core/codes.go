// Package core carries the sync job state machine: one-shot initial load,
// continuous incremental apply, pause and teardown. It knows nothing about a
// specific database; it consumes source events, calls transform, and hands
// batches to a sink.
package core

// Class decides what the runtime does with an error.
type Class string

// Error classes. The first digit of every code maps to its class.
const (
	UserAction Class = "user_action" // 停在这里，等人做一件事
	Retryable  Class = "retryable"   // 退避重试，<=3 次
	Fatal      Class = "fatal"       // 不可恢复，停
	Bug        Class = "bug"         // 我们自己的问题，停并引导提 issue
)

// Code is a stable error identifier. Never renumber; add new ones.
type Code string

// Error codes. Prefix digit encodes class: 1xx user_action, 2xx retryable,
// 3xx fatal, 4xx bug.
const (
	CodeWalLevel             Code = "E1001" // 源库没开变更流水
	CodeTargetTableExists    Code = "E1002" // 目标表已存在
	CodeNoReplicationPerm    Code = "E1003" // 账号没有读变更的许可
	CodeReplicaLoop          Code = "E1004" // 连到只读副本
	CodeJobRunningElsewhere  Code = "E1005" // 另一个实例在跑
	CodeViewNoStream         Code = "E1006" // 视图没有变更流
	CodeSecretPerm           Code = "E1007" // 私钥文件权限不对（安全底线）
	CodeConnUnreachable      Code = "E1008" // 连不上对端数据库（控制面测连/枚举用）
	CodeAPINotFound          Code = "E1009" // 这个接口不存在（未实现的 /api 路径）
	CodePositionKindMismatch Code = "E1010" // 目标库里的位点属于别的数据库类型
	CodeTxTooLarge           Code = "E1011" // 单个源事务超过整段缓冲的上限
	CodeTargetReset          Code = "E2001" // 目标连接被重置
	CodeSourceTimeout        Code = "E2002" // 源库连接超时
	CodeLockTimeout          Code = "E2003" // 目标库锁等待超时
	CodePositionGap          Code = "E3001" // 同步进度已不连续
	CodeSchemaTooNew         Code = "E3002" // 状态表版本比二进制新
	CodeControlDBBroken      Code = "E3010" // 控制库读不动（sqlite 损坏/版本不符）
	CodeUnsupportedType      Code = "E4001" // 未支持的类型（应已兜底）
	CodeDecoderBug           Code = "E4002" // 解码器内部不一致
)

// Spec is everything the user sees for one error: the four-part prompt
// (what happened / why / what you can do / risk), DESIGN.md principle 7.
type Spec struct {
	Code    Code
	Class   Class
	Message string   // 发生了什么（人话，不出现内部术语）
	Why     string   // 为什么
	Next    []string // 你现在能做什么（含命令）
	Risk    string   // 这么做的风险
}

var specs = []Spec{
	{
		Code: CodeWalLevel, Class: UserAction,
		Message: "源库没有开启「变更流水」",
		Why:     "数据库目前只记录足够恢复自身的信息，不足以让外部读取“谁改了什么”",
		Next: []string{
			"执行：ALTER SYSTEM SET wal_level = 'logical';",
			"重启一次数据库实例（连接会中断 10–30 秒）",
			"回来重新运行：jinliang run job.yaml（会从断点续跑）",
		},
		Risk: "源库里给外部读取的变更流水体积会增加约 10–20%；重启期间业务连接需要重连",
	},
	{
		Code: CodeTargetTableExists, Class: UserAction,
		Message: "目标端已经有同名表，按当前设置停下来了",
		Why:     "默认不覆盖目标数据，避免误删",
		Next: []string{
			"想追加数据：把 job.yaml 的 target.on_exists 改成 append",
			"想整表重建：改成 recreate（会先提示备份）",
			"想写到别处：改 target.schema",
		},
		Risk: "recreate 会先清空目标表；append 要求表结构兼容",
	},
	{
		Code: CodeNoReplicationPerm, Class: UserAction,
		Message: "用来同步的账号没有读取变更的许可",
		Why:     "读取变更流水需要单独的复制权限，和普通查询权限不是一回事",
		Next: []string{
			"让我们代做：jinliang run job.yaml --grant（需要你有授权权限）",
			"或者自己执行提示里的 GRANT 语句，再用新账号跑",
		},
		Risk: "给账号加权限是持久变更；建议用专用账号而不是业务账号",
	},
	{
		Code: CodeReplicaLoop, Class: UserAction,
		Message: "连到的是只读副本，副本上读不到变更流水",
		Why:     "只有主库（或开启了日志的节点）才能把变更喂给外部",
		Next:    []string{"把 dsn 指向主库地址", "如果必须连副本，请改用「定时增量」模式（只需要 SELECT）"},
		Risk:    "改连主库会增加主库的连接与读压力",
	},
	{
		Code: CodeJobRunningElsewhere, Class: UserAction,
		Message: "已经有一个实例在跑这个同步",
		Why:     "同一份同步被两个进程同时写会互相覆盖；我们用目标库的租约挡住了",
		Next:    []string{"确认另一台机器上的进程是否还在：jinliang status", "如果确认是死进程，等租约过期（默认 30 秒）后重试"},
		Risk:    "强行清理租约可能导致短暂双写",
	},
	{
		Code: CodeViewNoStream, Class: UserAction,
		Message: "视图没有自己的变更流水，做不了实时同步",
		Why:     "视图是查询结果，不是存储对象，数据库不会为它记录变更",
		Next:    []string{"把 mode.frequency 改成 interval（指定一个水位列，比如 updated_at）", "或者直接同步基表"},
		Risk:    "定时增量有分钟级延迟，且依赖水位列单调可信",
	},
	{
		Code: CodeSecretPerm, Class: UserAction,
		Message: "存放密码的文件权限太宽，拒绝读取",
		Why:     "凭证文件被别的用户读到就等于密码泄露",
		Next:    []string{"chmod 600 <文件路径>", "或者改用环境变量：dsn: env:JINLIANG_DSN"},
		Risk:    "这是全产品唯一会硬停的地方（安全底线）",
	},
	{
		Code: CodeConnUnreachable, Class: UserAction,
		Message: "连不上这个数据库",
		Why:     "地址、端口、账号口令、证书或防火墙，任意一项不对都会连不上；控制面只做只读探测，不会改对端任何东西",
		Next: []string{
			"点「重新测连」看这次的具体报错（它会告诉我卡在哪一步）",
			"确认地址端口能通：在能上外网的那台机器上试 telnet 主机 端口",
			"口令或证书改过的话，重新填一次口令再测",
		},
		Risk: "没有风险：测连只做只读查询（读版本号与权限），不写数据、不建对象",
	},
	{
		Code: CodeAPINotFound, Class: UserAction,
		Message: "没有这个接口",
		Why:     "控制面的接口清单在 docs/CONTROL-PLANE.md §4；不在这张表里的路径不会假装成功，也不会被网页兜底吞掉",
		Next: []string{
			"对一下 §4 的路径（导出是 GET /api/jobs/{id}/export；/yaml 是旧别名，会保留一个周期）",
			"B1 才会补 verify / gc：在那之前它们会明确告诉你「没有这个接口」",
		},
		Risk: "无：这条只影响调用方判断，不会改动任何数据",
	},
	{
		Code: CodePositionKindMismatch, Class: UserAction,
		Message: "进度标记与当前源对不上",
		Why:     "目标库里存的进度是另一种数据库写的（比如 MySQL 的位点、这次连的却是 PostgreSQL），我们不知道该从哪儿接着读",
		Next: []string{
			"换回当初那个源再跑：进度标记只能对着产生它的那种源用",
			"确实要换源：删掉目标端的同步元数据后重跑，会重搬一次全量",
		},
		Risk: "重搬全量期间目标端数据会短暂不一致，耗时随数据量增长",
	},
	{
		Code: CodeTxTooLarge, Class: UserAction,
		Message: "一个事务太大，整段缓冲撑不住了",
		Why:     "我们按事务整段缓冲（这样位点才能落在事务边界上），占用内存约等于行数×行宽；这个事务超过了上限（默认 128 MB / 100 万事件）",
		Next: []string{
			"把大事务拆小：分批 DELETE / INSERT（每批几千行），或先建新表再切换",
			"确实拆不了：调高单事务缓冲上限（MySQL 源用环境变量 JINLIANG_TX_MAX_BYTES / JINLIANG_TX_MAX_EVENTS；PostgreSQL 源暂时没有这个开关）并确认机器内存够",
		},
		Risk: "调高上限会推高峰值内存（仍然是整段缓冲，不是流式）；改了上限等于改提交边界，正在跑的任务最好重开一轮",
	},
	{
		Code: CodeControlDBBroken, Class: Fatal,
		Message: "控制库文件读不动了",
		Why:     "本机 SQLite 文件损坏，或它是更新版本的津梁写的（结构比当前二进制新）",
		Next: []string{
			"先备份整个数据目录（含 key 文件），别覆盖它",
			"换了新版二进制才出现的话：换回新版本运行",
			"确实损坏：删除控制库后用 jinliang import 从导出的 yaml 重建任务",
		},
		Risk: "重建会丢运行历史；凭证密文依赖 key 文件，key 丢了必须重新填一次口令",
	},
	{
		Code: CodeTargetReset, Class: Retryable,
		Message: "目标库把连接断开了",
		Why:     "常见于连接被防火墙/负载均衡回收，或目标库连接数打满",
		Next:    []string{"通常会自动重试（5s/20s/60s）", "持续出现请检查目标库连接数上限与空闲超时"},
		Risk:    "重试期间源端变更会积压，注意「同步进度落后」告警",
	},
	{
		Code: CodeSourceTimeout, Class: Retryable,
		Message: "源库响应超时",
		Why:     "网络抖动，或源库正在做重负载操作",
		Next:    []string{"自动重试中", "如果频繁超时，降低 snapshot.conns 或 max_read_qps"},
		Risk:    "降低并发会拉长初始化时间",
	},
	{
		Code: CodeLockTimeout, Class: Retryable,
		Message: "目标库等锁超时",
		Why:     "目标表上有长事务或 DDL 占着锁",
		Next:    []string{"自动重试中", "检查目标库上是否有长期未提交的事务"},
		Risk:    "重试会持续占用连接",
	},
	{
		Code: CodePositionGap, Class: Fatal,
		Message: "同步进度和源库的变更流水接不上了",
		Why:     "源库把还没读到的变更清理掉了（保留时间太短或书签被删）",
		Next:    []string{"重新初始化一次：jinliang run job.yaml --resync", "长远解决：把源库变更流水保留时间调长"},
		Risk:    "resync 会重搬全量，期间目标端数据可能短暂不一致",
	},
	{
		Code: CodeSchemaTooNew, Class: Fatal,
		Message: "状态表是更新版本写的，当前二进制读不懂",
		Why:     "升级过二进制又回退了",
		Next:    []string{"换回新版本二进制", "或者用 --resync 重建状态表（会重搬全量）"},
		Risk:    "降级不回退状态表结构",
	},
	{
		Code: CodeUnsupportedType, Class: Bug,
		Message: "遇到字典里没有的类型，已按文本兜底",
		Why:     "类型映射字典还没覆盖这个类型",
		Next:    []string{"如果数据看着不对，在 job.yaml 里手动指定该列的类型", "把类型名反馈给我们，补进字典"},
		Risk:    "文本兜底会丢掉类型语义（比如无法再按数值比较）",
	},
	{
		Code: CodeDecoderBug, Class: Bug,
		Message: "解码器内部状态不一致",
		Why:     "这是我们自己的 bug，不是你的数据问题",
		Next:    []string{"保留日志目录并提 issue", "临时绕过：删除该任务后重新初始化"},
		Risk:    "重搬全量耗时，且会短暂影响目标端可用性",
	},
}

// Lookup returns the user-facing spec for a code.
func Lookup(c Code) (Spec, bool) {
	for _, s := range specs {
		if s.Code == c {
			return s, true
		}
	}
	return Spec{}, false
}

// Catalogue returns every spec, for docs generation and tests.
func Catalogue() []Spec { return specs }

// ClassOf derives the class from the code's first digit.
func ClassOf(c Code) Class {
	if len(c) < 2 {
		return Bug
	}
	switch c[1] {
	case '1':
		return UserAction
	case '2':
		return Retryable
	case '3':
		return Fatal
	case '4':
		return Bug
	}
	return Bug
}
