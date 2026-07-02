// Package store 定义服务核心领域类型，并提供 task / parts 元数据的 MySQL 持久化。
//
// 领域类型（Task / Part / Warning / Scope / TimeRange / Status）放本包，
// 供 submit / executor / handler / cancel 复用，避免循环依赖。
package store

// Status 任务状态（对应 batch_task.status TINYINT，api-spec §3.3 状态机）。
type Status int8

const (
	StatusQueued    Status = 0
	StatusRunning   Status = 1
	StatusCompleted Status = 2
	StatusPartial   Status = 3
	StatusFailed    Status = 4
	StatusCancelled Status = 5
)

// String 返回对外 JSON 用的状态字符串。
func (s Status) String() string {
	switch s {
	case StatusQueued:
		return "queued"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusPartial:
		return "partial"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// IsTerminal 判断是否为终态。
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusPartial, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// Channel 待查询频道。
type Channel struct {
	ChannelID string `json:"channel_id"`
}

// Scope 查询范围。
type Scope struct {
	Channels []Channel `json:"channels"`
}

// TimeRange 时间范围（秒级 unix ts，含端点）。
type TimeRange struct {
	StartTS int64 `json:"start_ts"`
	EndTS   int64 `json:"end_ts"`
}

// Warning partial 截断告警（api-spec §3.2 / dev-guide §C.6）。
type Warning struct {
	ChannelID  string `json:"channel_id,omitempty"`
	Code       string `json:"code"` // per_channel_truncated / total_truncated
	Limit      int    `json:"limit"`
	ActualSeen int    `json:"actual_seen"`
}

// Warning code 常量。
const (
	WarnPerChannelTruncated = "per_channel_truncated"
	WarnTotalTruncated      = "total_truncated"
)

// Task 任务元数据（batch_task 表）。
type Task struct {
	TaskID        string
	CallerService string
	Status        Status
	ActualCount   int64
	Scope         Scope
	TimeRange     TimeRange
	RequestID     string
	Warnings      []Warning
	ErrorCode     string
	ErrorMessage  string
	CreatedAt     int64 // 秒级 unix ts
	UpdatedAt     int64
	ExpiresAt     int64
}

// Part 分片元数据（batch_task_part 表）。
type Part struct {
	TaskID       string
	PartSeq      int
	S3Key        string
	SizeBytes    int64
	MessageCount int64
	SHA256       string
	ChannelIDs   []string
	CreatedAt    int64
}
