// Package submit 实现 POST /v1/messages/batch 的后端逻辑（dev-guide §A3）：
// 字段校验 → 逐 channel _count 求和拦截 → Gate 占槽 → store.Create → enqueue。
package submit

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/config"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
)

const (
	maxChannels  = 30
	maxRangeDays = 31
	msThreshold  = 1_000_000_000_000 // ts >= 1e12 视为毫秒（非法）
	taskIDPrefix = "ost_"
)

// Error 是 submit 的业务错误，携带对外 error_code 与 HTTP 状态码。
type Error struct {
	HTTPStatus int
	Code       string
	Message    string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newErr(status int, code, msg string) *Error {
	return &Error{HTTPStatus: status, Code: code, Message: msg}
}

// Enqueuer 是 submit 对 executor 的最小依赖（避免 import 环）。
type Enqueuer interface {
	Enqueue(taskID string) error
}

// Counter 是 submit 对 osclient 的最小依赖（便于测试）。
type Counter interface {
	Count(ctx context.Context, index, channelID string, startTS, endTS int64) (int64, error)
}

// Gate 是全局并发闸门的最小依赖。
type Gate interface {
	Acquire(ctx context.Context, wait time.Duration) bool
	Release()
}

// Request 是提交请求体（api-spec §2.1）。
type Request struct {
	Scope     store.Scope     `json:"scope"`
	TimeRange store.TimeRange `json:"time_range"`
}

// Submitter 持有 submit 路径的依赖。
type Submitter struct {
	cfg     config.ProtectConfig
	index   string
	counter Counter
	store   store.Store
	gate    Gate
	enq     Enqueuer
}

// New 构造 Submitter。
func New(cfg config.ProtectConfig, index string, counter Counter, st store.Store, gate Gate, enq Enqueuer) *Submitter {
	return &Submitter{cfg: cfg, index: index, counter: counter, store: st, gate: gate, enq: enq}
}

// Submit 处理一次提交。成功返回 task_id；失败返回 *Error（含 HTTP 状态与 error_code）。
func (s *Submitter) Submit(ctx context.Context, caller, requestID string, req *Request) (string, error) {
	if err := validate(req); err != nil {
		metrics.SubmitTotal.WithLabelValues(caller, "invalid").Inc()
		return "", err
	}

	// 逐 channel _count 求和（Phase 0 精简：不用 multi-search）。
	countStart := time.Now()
	var total int64
	for _, ch := range req.Scope.Channels {
		n, err := s.counter.Count(ctx, s.index, ch.ChannelID, req.TimeRange.StartTS, req.TimeRange.EndTS)
		if err != nil {
			metrics.SubmitTotal.WithLabelValues(caller, "count_err").Inc()
			return "", newErr(503, "service_degraded", "count check failed")
		}
		total += n
	}
	metrics.CountCheckDuration.Observe(time.Since(countStart).Seconds())

	if total > s.cfg.MaxTaskHits {
		metrics.ProtectionRejected.WithLabelValues("max_task_hits").Inc()
		metrics.SubmitTotal.WithLabelValues(caller, "too_large").Inc()
		return "", newErr(413, "single_task_too_large",
			fmt.Sprintf("estimated hits %d exceeds limit %d", total, s.cfg.MaxTaskHits))
	}

	// 占全局并发槽位（满则排队，超时 503）。
	if !s.gate.Acquire(ctx, s.cfg.SubmitWait) {
		metrics.ProtectionRejected.WithLabelValues("inflight_full").Inc()
		metrics.SubmitTotal.WithLabelValues(caller, "degraded").Inc()
		return "", newErr(503, "service_degraded", "global inflight limit reached")
	}

	taskID := NewTaskID()
	t := &store.Task{
		TaskID:        taskID,
		CallerService: caller,
		Status:        store.StatusQueued,
		ActualCount:   total,
		Scope:         req.Scope,
		TimeRange:     req.TimeRange,
		RequestID:     requestID,
	}
	if err := s.store.Create(ctx, t); err != nil {
		s.gate.Release()
		metrics.SubmitTotal.WithLabelValues(caller, "store_err").Inc()
		return "", newErr(500, "internal_error", "persist task failed")
	}

	if err := s.enq.Enqueue(taskID); err != nil {
		// 队列满：标 failed + 释放槽位 + 503。
		s.gate.Release()
		_ = s.store.Finalize(ctx, taskID, store.StatusFailed, 0, nil, "service_degraded", "queue full")
		metrics.ProtectionRejected.WithLabelValues("queue_full").Inc()
		metrics.SubmitTotal.WithLabelValues(caller, "degraded").Inc()
		return "", newErr(503, "service_degraded", "executor queue full")
	}

	metrics.SubmitTotal.WithLabelValues(caller, "ok").Inc()
	return taskID, nil
}

// validate 做字段校验（api-spec §2.1 / §6.3）。
func validate(req *Request) *Error {
	n := len(req.Scope.Channels)
	if n == 0 {
		return newErr(400, "invalid_request", "scope.channels must not be empty")
	}
	if n > maxChannels {
		return newErr(413, "single_task_too_large",
			fmt.Sprintf("scope.channels %d exceeds limit %d", n, maxChannels))
	}
	for i, ch := range req.Scope.Channels {
		if ch.ChannelID == "" {
			return newErr(400, "invalid_request", fmt.Sprintf("scope.channels[%d].channel_id is empty", i))
		}
	}
	tr := req.TimeRange
	if tr.StartTS <= 0 || tr.EndTS <= 0 {
		return newErr(400, "invalid_request", "time_range.start_ts and end_ts are required")
	}
	if tr.StartTS >= msThreshold || tr.EndTS >= msThreshold {
		return newErr(400, "invalid_request", "timestamps must be in seconds, not milliseconds")
	}
	if tr.EndTS < tr.StartTS {
		return newErr(400, "invalid_request", "time_range.end_ts must be >= start_ts")
	}
	if tr.EndTS-tr.StartTS > int64(maxRangeDays)*24*3600 {
		return newErr(422, "time_range_too_long", fmt.Sprintf("time_range exceeds %d days", maxRangeDays))
	}
	return nil
}

// base32 字母表（Crockford 风格，去掉易混字符；单调性靠时间前缀，随机部分填充）。
const base32Alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewTaskID 生成 "ost_" + 24 位 base32 id。前 8 位编码毫秒时间戳（粗略单调），
// 后 16 位随机。不追求严格 ULID（brief：单调即可）。
func NewTaskID() string {
	var b [24]byte
	ms := uint64(time.Now().UnixMilli())
	// 高位时间（取 40 bit 足够到 2100 年），编码为 8 个 base32 字符（大端）。
	for i := 7; i >= 0; i-- {
		b[i] = base32Alphabet[ms&0x1f]
		ms >>= 5
	}
	// 后 16 位随机：每字节取低 5 bit 映射到 base32。
	var rnd [16]byte
	_, _ = rand.Read(rnd[:])
	for i := 0; i < 16; i++ {
		b[8+i] = base32Alphabet[rnd[i]&0x1f]
	}
	return taskIDPrefix + string(b[:])
}
