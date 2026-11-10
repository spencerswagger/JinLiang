package mysqlsource

// 语义错误标记（契约 docs/CONTRACT-PAUSE-AND-POSITION.md §4.4）。
//
// 引擎不再 import 本包：它只问 `source.ClassOf(err)`。所以「这条 MySQL 错误是什么意思」
// 必须在这里翻成 `source.Class`——不然异构（MySQL 源）下引擎一个都认不出，
// 用户拿到的是「未识别的数据库错误，请把日志发给维护者」，而真相是「账号没有权限」
// 或「binlog 已经被清理，需要 --resync」。
//
// 判据优先级：**错误码**（driver 给的 Number）> **消息措辞**。前者稳定，后者只是兜底：
// 同一个语义在不同版本/不同驱动上的措辞会变（5.7 与 8.4 的 binlog 相关文案就不一样）。

import (
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/spencerswagger/jinliang/internal/source"
)

// markClass 给出口错误打上语义类。
func markClass(err error) error {
	if err == nil {
		return nil
	}
	if c, ok := classOfMySQLError(err); ok {
		return source.Mark(err, c)
	}
	return err
}

// classOfMySQLError 把 MySQL 的错误码翻成语义类。
//
// 码表（只用**语义稳定**的那些；数值见 MySQL 官方 server-error 列表）：
//
//	1044/1045/1142/1227/1370 权限不足
//	1049/1051/1146           未知库 / 未知表
//	1236                     binlog 已被清理或不可读（位点追不回来）
//	2003/2013/2055           连接类（可重试）
func classOfMySQLError(err error) (source.Class, bool) {
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		// 有的路径（如 binlog 复制连接）会把错误包成普通 error：按措辞兜底。
		return classOfMessage(err)
	}
	switch me.Number {
	case 1044, 1045, 1142, 1227, 1370:
		return source.ClassNoPrivilege, true
	case 1049, 1051, 1146:
		return source.ClassObjectMissing, true
	case 1236:
		// "Could not find first log file name in binary log index file" /
		// "binlog truncated" —— 位点所在的 binlog 不在了。
		return source.ClassPositionGap, true
	case 2003, 2013, 2055:
		return source.ClassRetryable, true
	}
	return classOfMessage(err)
}

// classOfMessage 是措辞兜底：只在拿不到错误码时用。
func classOfMessage(err error) (source.Class, bool) {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "access denied"):
		return source.ClassNoPrivilege, true
	case strings.Contains(msg, "unknown table") || strings.Contains(msg, "unknown database") ||
		strings.Contains(msg, "doesn't exist"):
		return source.ClassObjectMissing, true
	case strings.Contains(msg, "could not find first log file") ||
		strings.Contains(msg, "binlog") && strings.Contains(msg, "not found") ||
		strings.Contains(msg, "purged"):
		return source.ClassPositionGap, true
	case strings.Contains(msg, "binlog_format") || strings.Contains(msg, "row format"):
		// 源库没按 ROW 记录变更：用户要做的事与预检时一样（改 binlog_format）。
		return source.ClassWalLevel, true
	}
	return source.ClassUnknown, false
}
