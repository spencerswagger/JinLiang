package source

import (
	"errors"
	"fmt"
	"strings"
)

// PositionFormatVersion is the position wire-format version in "<kind>:v1:<payload>".
//
// 位点是写在目标库里的，要跨版本、跨重启存活：格式一变，旧库就读不懂。
// 把版本号放进串里是唯一能提前做的保险——现在零成本，以后是迁移。
const PositionFormatVersion = "v1"

// FormatPosition renders a position string in the versioned wire format.
// kind 用 Endpoint.Kind() 的取值（postgres / mysql）。
func FormatPosition(kind, payload string) string {
	return kind + ":" + PositionFormatVersion + ":" + payload
}

// SplitPosition parses "<kind>:v1:<payload>".
//
// 两种失败必须分开报（上层给不同错误码）：位点格式不认识是数据损坏；
// kind 与当前源对不上是「换错了源」（E1010，不要用 E3001 把人引去重搬全量）。
func SplitPosition(raw string) (kind, payload string, err error) {
	parts := strings.SplitN(raw, ":", 3)
	if len(parts) != 3 {
		return "", "", fmt.Errorf("%w：位点串缺少 <kind>:<版本>:<载荷> 三段（收到 %q）", ErrPositionMalformed, clipPosition(raw))
	}
	kind, ver, payload := parts[0], parts[1], parts[2]
	if kind == "" || payload == "" {
		return "", "", fmt.Errorf("%w：位点的 kind 或载荷是空的（收到 %q）", ErrPositionMalformed, clipPosition(raw))
	}
	if ver != PositionFormatVersion {
		return "", "", fmt.Errorf("%w：位点格式版本是 %s，本二进制只认 %s（收到 %q）",
			ErrPositionMalformed, ver, PositionFormatVersion, clipPosition(raw))
	}
	return kind, payload, nil
}

// ErrPositionMalformed means the stored position cannot be parsed at all.
var ErrPositionMalformed = errors.New("位点串格式不对")

func clipPosition(s string) string {
	const max = 48
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// ValidateBatch checks the transaction invariants of one batch of change events
// before the engine hands it to a sink (docs/CONTROL-PLANE.md 之外的事务边界规则；
// 见 sink.Sink.Apply 的注释)。
//
// 能被机械校验的三条：
//  1. 事务不嵌套：一个 EvBegin 未收尾时不得再出现 EvBegin；
//  2. 批次不得以未提交的事务结尾：开了事务就必须在本批内被 EvCommit 关掉；
//  3. 以 EvBegin 开头的批次必须是完整事务（末尾必须是 EvCommit）——切开大事务的
//     **续段**（前半段在上一批）不许伪造成一个新事务的开头。
//
// 允许的形态：空批次（只推进位点）、纯心跳批次、事务外的 EvDDL、
// 「续段 + EvCommit」（前半段在上一批、本批收尾）。
//
// 查不出来的一条写在文档里：续段的第一条数据事件属于上一批那个事务，
// 单看本批无法与「凭空出现的数据事件」区分——那要靠引擎按 TxID 自己拦（见 Event.TxID）。
func ValidateBatch(evs []Event) error {
	open := 0
	for i, ev := range evs {
		switch ev.Kind {
		case EvBegin:
			if open > 0 {
				return fmt.Errorf("第 %d 个事件又开了一个事务：上一个事务还没收到 EvCommit（事务不能嵌套）", i)
			}
			open++
		case EvCommit:
			if open > 0 {
				open--
			}
			// open == 0 且遇到 EvCommit：本批只承载「续段的收尾」，合法。
		case EvHeartbeat:
			// 心跳不参与事务配对。
		}
	}
	if open != 0 {
		return fmt.Errorf("批次以 %d 个未提交的事务结尾：必须以 EvCommit 收尾，半个事务的批次不能推进位点", open)
	}
	// 以 EvBegin 开头的批次必须在本批内闭合；上面的 open 判定已覆盖，
	// 这里再挡一种更隐蔽的写法：只有 EvBegin、后面什么都没有。
	if n := len(evs); n > 0 && evs[n-1].Kind == EvBegin {
		return errors.New("批次以 EvBegin 结尾：事务没开始就已经结束了")
	}
	return nil
}
