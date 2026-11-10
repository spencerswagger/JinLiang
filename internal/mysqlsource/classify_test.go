package mysqlsource

// MySQL 源的错误分类（契约 §4.4）。没有这一层，异构增量下所有 MySQL 错误在引擎里
// 都落到「未识别的数据库错误，请把日志发给维护者」——用户拿不到可执行的下一步。

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestMySQLDriverErrorsGetSemanticClass(t *testing.T) {
	cases := []struct {
		num  uint16
		want source.Class
	}{
		{1045, source.ClassNoPrivilege},   // Access denied
		{1142, source.ClassNoPrivilege},   // 表级权限不足
		{1146, source.ClassObjectMissing}, // Table doesn't exist
		{1049, source.ClassObjectMissing}, // Unknown database
		{1236, source.ClassPositionGap},   // binlog 已被清理
		{2013, source.ClassRetryable},     // Lost connection during query
	}
	for _, c := range cases {
		err := markClass(&mysql.MySQLError{Number: c.num, Message: "x"})
		if got := source.ClassOf(err); got != c.want {
			t.Errorf("MySQL %d 应归 %s，得到 %s", c.num, c.want, got)
		}
	}
}

func TestMySQLMessageFallbackWhenNoCode(t *testing.T) {
	// binlog 复制连接上的错误常常没有 MySQLError 包装：只能按措辞兜底。
	cases := []struct {
		msg  string
		want source.Class
	}{
		{"Could not find first log file name in binary log index file", source.ClassPositionGap},
		{"Error 1045: Access denied for user 'jinliang'@'%'", source.ClassNoPrivilege},
		{"Table 'jinliang_src.orders' doesn't exist", source.ClassObjectMissing},
		{"binlog_format must be ROW", source.ClassWalLevel},
	}
	for _, c := range cases {
		err := markClass(errors.New(c.msg))
		if got := source.ClassOf(err); got != c.want {
			t.Errorf("%q 应归 %s，得到 %s", c.msg, c.want, got)
		}
	}
	// 认不出来就别乱标：未知错误必须还是 unknown（引擎会如实说「未识别」）。
	if got := source.ClassOf(markClass(errors.New("something odd"))); got != source.ClassUnknown {
		t.Fatalf("未识别的消息不该被标类，得到 %s", got)
	}
	// 已经标过的不重复包（外层知道得更多）。
	once := markClass(&mysql.MySQLError{Number: 1146, Message: "Table 'x' doesn't exist"})
	twice := markClass(fmt.Errorf("准备源端对象失败: %w", once))
	if got := source.ClassOf(twice); got != source.ClassObjectMissing {
		t.Fatalf("包一层之后仍应认出 1146，得到 %s", got)
	}
}
