package control

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// TxOversizeDoc 承载 reliability.tx_oversize 的**两种写法**（2026-11-06 定形状）：
//
//	tx_oversize: fail              # 旧写法：spill（默认）| fail —— 继续有效，行为不变
//	tx_oversize: {mode: fail, bytes: 134217728, events: 1000000}   # 新写法
//
// 向后兼容的关键不只在"能解析"，还在"能原样写回"：旧写法导出后必须仍是字符串，
// 不能因为内部换成了结构体就悄悄变成对象——那样用户导出再导入就莫名为另一种写法，
// 与"配置文件只是导入导出格式"这条口径冲突。所以 legacy 记住了原始形态。
type TxOversizeDoc struct {
	Mode   string `json:"mode,omitempty" yaml:"mode,omitempty"`
	Bytes  *int   `json:"bytes,omitempty" yaml:"bytes,omitempty"`
	Events *int   `json:"events,omitempty" yaml:"events,omitempty"`

	// legacy 非空表示用户写的是字符串形态；序列化时按它写回，不做"升级"。
	legacy string
}

// TxOversize 的取值。
const (
	TxOversizeSpill = "spill"
	TxOversizeFail  = "fail"
)

// 缓冲上限的合法下限与内置默认（与 docs/STREAMING.md §1 一致）。
const (
	MinTxMaxBytes  = 1 << 20 // 1 MiB
	MinTxMaxEvents = 1000

	DefaultTxMaxBytes  int64 = 128 << 20 // 128 MB
	DefaultTxMaxEvents int64 = 1_000_000 // 100 万事件

	envTxMaxBytes  = "JINLIANG_TX_MAX_BYTES"
	envTxMaxEvents = "JINLIANG_TX_MAX_EVENTS"
)

// Legacy 返回用户写的字符串形态（空表示写的是对象或没写）。
func (t *TxOversizeDoc) Legacy() string { return t.legacy }

// ModeString 返回生效的 mode：对象里没写 mode 时按旧默认 spill。
func (t *TxOversizeDoc) ModeString() string {
	if t == nil {
		return ""
	}
	if t.Mode != "" {
		return t.Mode
	}
	return t.legacy
}

// UnmarshalYAML 接受标量字符串或映射（strict：映射里的未知键直接报错，
// 与 ParseJobYAML 的 KnownFields(true) 保持一致——不能因为自定义解析就开后门）。
func (t *TxOversizeDoc) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		t.legacy = strings.TrimSpace(node.Value)
		t.Mode, t.Bytes, t.Events = "", nil, nil
		return nil
	case yaml.MappingNode:
		var raw struct {
			Mode   string `yaml:"mode"`
			Bytes  *int   `yaml:"bytes"`
			Events *int   `yaml:"events"`
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := node.Content[i].Value
			switch key {
			case "mode", "bytes", "events":
			default:
				return fmt.Errorf("reliability.tx_oversize 里不认识 %q（只接受 mode / bytes / events）", key)
			}
		}
		if err := node.Decode(&raw); err != nil {
			return err
		}
		t.legacy = ""
		t.Mode, t.Bytes, t.Events = strings.TrimSpace(raw.Mode), raw.Bytes, raw.Events
		return nil
	default:
		return fmt.Errorf("reliability.tx_oversize 只能是字符串（spill/fail）或对象（mode/bytes/events）")
	}
}

// MarshalYAML 按用户原来的形态写回（legacy 优先）。
func (t TxOversizeDoc) MarshalYAML() (any, error) {
	if t.legacy != "" {
		return t.legacy, nil
	}
	out := map[string]any{}
	if t.Mode != "" {
		out["mode"] = t.Mode
	}
	if t.Bytes != nil {
		out["bytes"] = *t.Bytes
	}
	if t.Events != nil {
		out["events"] = *t.Events
	}
	return out, nil
}

// UnmarshalJSON 与 YAML 同构（spec_json 里也存这一份）。
func (t *TxOversizeDoc) UnmarshalJSON(b []byte) error {
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) == 0 {
		return nil
	}
	switch trimmed[0] {
	case '"':
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return err
		}
		t.legacy = strings.TrimSpace(s)
		t.Mode, t.Bytes, t.Events = "", nil, nil
		return nil
	case '{':
		var raw struct {
			Mode   string          `json:"mode"`
			Bytes  *int            `json:"bytes"`
			Events *int            `json:"events"`
			Extra  json.RawMessage `json:"-"`
		}
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&raw); err != nil {
			return fmt.Errorf("reliability.tx_oversize：%w", err)
		}
		t.legacy = ""
		t.Mode, t.Bytes, t.Events = strings.TrimSpace(raw.Mode), raw.Bytes, raw.Events
		return nil
	default:
		return fmt.Errorf("reliability.tx_oversize 只能是字符串或对象（收到 %s）", trimmed)
	}
}

// MarshalJSON 与 MarshalYAML 同口径。
func (t TxOversizeDoc) MarshalJSON() ([]byte, error) {
	if t.legacy != "" {
		return json.Marshal(t.legacy)
	}
	out := map[string]any{}
	if t.Mode != "" {
		out["mode"] = t.Mode
	}
	if t.Bytes != nil {
		out["bytes"] = *t.Bytes
	}
	if t.Events != nil {
		out["events"] = *t.Events
	}
	return json.Marshal(out)
}

// Problems 返回这条配置的问题（空表示合法）。与 jobspec 其它字段一样，
// 文案本身就要能直接给用户看，所以写成人话而不是"校验失败"。
func (t *TxOversizeDoc) Problems() []string {
	if t == nil {
		return nil
	}
	var errs []string
	switch m := t.ModeString(); m {
	case "", TxOversizeSpill, TxOversizeFail:
	default:
		errs = append(errs, fmt.Sprintf("reliability.tx_oversize 只能是 spill / fail（收到 %q）", m))
	}
	if t.Bytes != nil && *t.Bytes < MinTxMaxBytes {
		errs = append(errs, fmt.Sprintf("reliability.tx_oversize.bytes 至少 %d（1 MiB），收到 %d", MinTxMaxBytes, *t.Bytes))
	}
	if t.Events != nil && *t.Events < MinTxMaxEvents {
		errs = append(errs, fmt.Sprintf("reliability.tx_oversize.events 至少 %d，收到 %d", MinTxMaxEvents, *t.Events))
	}
	return errs
}

// Limits 解析出生效的单事务缓冲上限，顺序固定为
// **yaml 显式值 > 环境变量 > 内置默认**（docs/STREAMING.md §1）。
//
// lookup 为 nil 时读进程环境（测试可注入，避免并行用例互相污染）。
func (t *TxOversizeDoc) Limits(lookup func(string) (string, bool)) (maxBytes, maxEvents int64) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	maxBytes, maxEvents = DefaultTxMaxBytes, DefaultTxMaxEvents
	if v, ok := envPositiveInt(lookup, envTxMaxBytes); ok {
		maxBytes = v
	}
	if v, ok := envPositiveInt(lookup, envTxMaxEvents); ok {
		maxEvents = v
	}
	if t != nil {
		if t.Bytes != nil && *t.Bytes > 0 {
			maxBytes = int64(*t.Bytes)
		}
		if t.Events != nil && *t.Events > 0 {
			maxEvents = int64(*t.Events)
		}
	}
	return maxBytes, maxEvents
}

func envPositiveInt(lookup func(string) (string, bool), name string) (int64, bool) {
	raw, ok := lookup(name)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
