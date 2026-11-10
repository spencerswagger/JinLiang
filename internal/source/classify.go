package source

// 连接器给引擎的**语义错误**（契约 docs/CONTRACT-PAUSE-AND-POSITION.md §4.4）。
//
// 为什么要有这一层：引擎原来在 `classifyError` 里直接调 `pgsource.IsNoPrivilege` /
// `pgsource.IsPositionGap` —— 那是**具体连接器**的知识，异构（MySQL 源）之后
// 那些判断一个都不命中，用户的 MySQL 错误一律落到「未识别的数据库错误，请把日志发给维护者」。
//
// 现在的分工：
//   - 连接器知道「这条错误是什么意思」，就在返回前**标记**它（`Mark` / 实现 `SourceClass`）；
//   - 引擎只问 `ClassOf(err)`，不 import 任何连接器包。
//
// 兜底那层（SQLSTATE）仍然留在这里：SQLSTATE 是协议层的公共词汇（42501 / 42P01 / 53400…），
// 不是某一家连接器的私有约定；连接器没标记时按它判，比落「未识别」强。

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/pgwire"
)

// Class 是「这条错误属于哪一类」的稳定枚举。引擎按它挂码，连接器按它标记。
type Class int

const (
	ClassUnknown Class = iota
	// ClassNoPrivilege：账号没有被授予这个动作（SQLSTATE 42501 / MySQL 1045、1142）。
	ClassNoPrivilege
	// ClassObjectMissing：schema / 表 / 列不存在（3F000 / 42P01 / 42703 / MySQL 1146）。
	ClassObjectMissing
	// ClassResourceLimit：源端资源或上限已用满（53400：书签/连接数到顶）。
	ClassResourceLimit
	// ClassWalLevel：源库没开「变更流水」（wal_level / binlog_format）。
	ClassWalLevel
	// ClassPositionGap：位点已经追不回来（WAL 段没了、binlog 被 purge、书签失效）。
	ClassPositionGap
	// ClassPositionForeign：目标库里的位点来自另一个源库（换了实例/主从切换）。
	ClassPositionForeign
	// ClassRetryable：可重试的传输类错误（连接被重置、读流中断）。
	ClassRetryable
)

func (c Class) String() string {
	switch c {
	case ClassNoPrivilege:
		return "no_privilege"
	case ClassObjectMissing:
		return "object_missing"
	case ClassResourceLimit:
		return "resource_limit"
	case ClassWalLevel:
		return "wal_level"
	case ClassPositionGap:
		return "position_gap"
	case ClassPositionForeign:
		return "position_foreign"
	case ClassRetryable:
		return "retryable"
	}
	return "unknown"
}

// Classified 由**连接器**实现（也可以只包一层，见 Mark）。
type Classified interface {
	SourceClass() Class
}

// Marked 是「同一个错误，外加一个语义类」的包装：不改变 `errors.Is/As` 链
// （Unwrap 到底下那条），只是让 `ClassOf` 认得出来。
type Marked struct {
	Class Class
	Err   error
}

func (m *Marked) Error() string      { return m.Err.Error() }
func (m *Marked) Unwrap() error      { return m.Err }
func (m *Marked) SourceClass() Class { return m.Class }

// Mark 给一条错误打上语义类。err 为 nil 时返回 nil（好让调用方直接 `return Mark(err, …)`）。
// 已经有标记的不重复包——外层知道得更多，保留外层。
func Mark(err error, c Class) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*Marked); ok {
		return err
	}
	return &Marked{Class: c, Err: err}
}

// ClassOf 是引擎唯一该用的入口：先看连接器的标记，再退回 SQLSTATE。
func ClassOf(err error) Class {
	if err == nil {
		return ClassUnknown
	}
	var c Classified
	if errors.As(err, &c) {
		return c.SourceClass()
	}
	state := pgwire.SQLState(err)
	if cls, ok := classFromSQLState(state); ok {
		return cls
	}
	// 兜底措辞（连接器没标记时）：55000/0A000 里只有带 wal_level 才是「没开变更流水」。
	// 这条判据原来在 pgsource 里，现在是「连接器没来得及标记」的最后一道保险；
	// MySQL 侧的经验是：标记（`source.Mark`）比措辞可靠，新代码请用标记。
	if state == "55000" || state == "0A000" {
		if strings.Contains(strings.ToLower(err.Error()), "wal_level") {
			return ClassWalLevel
		}
	}
	return ClassUnknown
}

// classFromSQLState 是**协议层**的公共词汇，不是某家连接器的私有约定。
// 连接器自己标记得更好（能带上 MySQL 的 1146 这类），这里只是兜底。
func classFromSQLState(state string) (Class, bool) {
	switch state {
	case "42501": // insufficient_privilege
		return ClassNoPrivilege, true
	case "3F000", "42P01", "42703": // 缺 schema / 缺表 / 缺列
		return ClassObjectMissing, true
	case "53400": // configuration_limit_exceeded（书签/连接数到顶）
		return ClassResourceLimit, true
	case "58P01": // undefined_file：WAL 段已被删除
		return ClassPositionGap, true
	case "55000", "0A000": // object_not_in_prerequisite_state / feature_not_supported
		// 这两个只有配上「wal_level」字样才是 E1001；连接器会先标记，这里不猜。
		return ClassUnknown, false
	}
	return ClassUnknown, false
}

// ClassError 是连接器内部用来「带类返回」的轻量错误（不值得为每种情况定义新类型时）。
func ClassError(c Class, format string, args ...any) error {
	return &Marked{Class: c, Err: fmt.Errorf(format, args...)}
}
