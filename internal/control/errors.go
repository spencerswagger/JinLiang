package control

import (
	"errors"
	"fmt"
)

// Sentinel errors the API layer turns into four-part cards (docs/ERRORS.md).
var (
	// ErrNotFound: 这一行不存在（可能刚被别人删掉）。
	ErrNotFound = errors.New("记录不存在")
	// ErrInUse: 还有别的东西引用它，删了会留下悬空引用。
	ErrInUse = errors.New("仍被引用")
	// ErrConflict: 唯一性冲突，通常意味着「这其实是同一个任务」（docs/IDENTITY.md）。
	ErrConflict = errors.New("冲突")
	// ErrInvalid: 输入不合法，消息本身已经是人话，可直接展示。
	ErrInvalid = errors.New("输入不合法")
	// ErrIdentityLocked: 这个连接的坐标是任务身份的一部分，被引用时不许改
	// （改了 job_key 就与已建任务脱节，同一份同步能变成两个任务）。
	ErrIdentityLocked = errors.New("连接坐标已锁定")
)

// invalid wraps a message as ErrInvalid so callers can errors.Is it.
func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}
