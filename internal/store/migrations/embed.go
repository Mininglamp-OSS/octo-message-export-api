// Package storeschema 通过 go:embed 暴露 batch_task / batch_task_part 的建表 SQL,
// 这样 scratch 容器和测试都能用同一份语句而不依赖文件系统路径。
package storeschema

import _ "embed"

// SchemaSQL 是 internal/store/migrations/schema.sql 的内容。
//
//go:embed schema.sql
var SchemaSQL string
