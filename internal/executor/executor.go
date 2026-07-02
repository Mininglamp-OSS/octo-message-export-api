// Package executor 是 task 执行引擎（dev-guide §A5）：worker pool 从内存 chan 取 task，
// 开 PIT、按 channel 字典序 search_after 翻页、经 result.Writer 落 NDJSON part 到 S3，
// 写终态。全局并发上限由 Gate（gate.go）控制,与 submit 路径共享。
package executor

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/osclient"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/result"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
)

// ErrQueueFull 队列已满（submit 据此返回 503）。
var ErrQueueFull = errors.New("executor queue full")

// Config 是 executor 运行所需参数（由 main 从 config.Config 映射）。
type Config struct {
	Index             string
	PITKeepAlive      time.Duration
	Workers           int
	QueueBuffer       int
	PartMaxRows       int
	PartMaxBytes      int64
	MaxTaskHits       int64
	PerChannelHardCap int
	PageSize          int // 每页 size，默认 1000
}

// Auditor 记录执行审计的最小接口。
type Auditor interface {
	Log(ev metrics.AuditEvent)
}

// Executor 持有依赖 + 任务队列 + 运行中 task 的 cancel 注册表。
type Executor struct {
	cfg   Config
	os    osclient.Client
	store store.Store
	up    result.Uploader
	gate  *Gate
	audit Auditor

	queue chan string

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

// New 构造 Executor。
func New(cfg Config, os osclient.Client, st store.Store, up result.Uploader, gate *Gate, audit Auditor) *Executor {
	if cfg.PageSize <= 0 {
		cfg.PageSize = 1000
	}
	if cfg.QueueBuffer <= 0 {
		cfg.QueueBuffer = 1
	}
	return &Executor{
		cfg:     cfg,
		os:      os,
		store:   st,
		up:      up,
		gate:    gate,
		audit:   audit,
		queue:   make(chan string, cfg.QueueBuffer),
		cancels: make(map[string]context.CancelFunc),
	}
}

// Enqueue 把 taskID 塞入队列；满则返回 ErrQueueFull（非阻塞）。
func (e *Executor) Enqueue(taskID string) error {
	select {
	case e.queue <- taskID:
		metrics.QueueDepth.Set(float64(len(e.queue)))
		return nil
	default:
		return ErrQueueFull
	}
}

// CancelTask 触发运行中 task 的 ctx 取消（cancel 包 DELETE 时调用）。
// task 不在运行中（仍排队 / 已结束）则 no-op；排队中的 task 由 worker 取出时
// 通过 store 状态发现已 cancelled 而跳过执行。
func (e *Executor) CancelTask(taskID string) {
	e.mu.Lock()
	cancel := e.cancels[taskID]
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (e *Executor) register(taskID string, cancel context.CancelFunc) {
	e.mu.Lock()
	e.cancels[taskID] = cancel
	e.mu.Unlock()
}

func (e *Executor) unregister(taskID string) {
	e.mu.Lock()
	delete(e.cancels, taskID)
	e.mu.Unlock()
}

// Run 启动 worker pool，阻塞直到 ctx 取消后所有 worker 退出。
func (e *Executor) Run(ctx context.Context) {
	n := e.cfg.Workers
	if n <= 0 {
		n = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.worker(ctx)
		}()
	}
	wg.Wait()
}

func (e *Executor) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case taskID := <-e.queue:
			metrics.QueueDepth.Set(float64(len(e.queue)))
			e.process(ctx, taskID)
		}
	}
}

// process 执行单个 task 的完整生命周期，终态时释放 Gate 槽位。
func (e *Executor) process(parentCtx context.Context, taskID string) {
	defer e.gate.Release()

	taskCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	e.register(taskID, cancel)
	defer e.unregister(taskID)

	t, err := e.store.Get(taskCtx, taskID)
	if err != nil {
		// task 读不到（极少；可能被 GC 删）。直接放弃。
		return
	}
	// 排队期间已被取消 / 已终态：跳过执行。
	if t.Status.IsTerminal() {
		return
	}

	if err := e.store.UpdateStatus(taskCtx, taskID, store.StatusRunning); err != nil {
		_ = e.store.Finalize(parentCtx, taskID, store.StatusFailed, 0, nil,
			"internal_error", "update status running failed")
		return
	}
	metrics.Inflight.Set(float64(e.gate.InFlight()))

	start := time.Now()
	status, actualCount, warnings, errCode, errMsg := e.run(taskCtx, t)

	// 父 ctx 取消（服务关停）或 task ctx 取消（DELETE）→ cancelled。
	if status == store.StatusFailed && taskCtx.Err() != nil {
		status, errCode, errMsg = store.StatusCancelled, "", ""
	}

	// Finalize 用 parentCtx：taskCtx 已取消时仍要把终态写库。
	finCtx := parentCtx
	if finCtx.Err() != nil {
		finCtx = context.Background()
	}
	if err := e.store.Finalize(finCtx, taskID, status, actualCount, warnings, errCode, errMsg); err != nil {
		return
	}

	metrics.ExecuteDuration.WithLabelValues(t.CallerService, metrics.SizeBucket(actualCount)).
		Observe(time.Since(start).Seconds())
	metrics.TaskStatusTotal.WithLabelValues(status.String()).Inc()
	metrics.Inflight.Set(float64(e.gate.InFlight()))

	if e.audit != nil {
		e.audit.Log(metrics.AuditEvent{
			Event: "execute", TaskID: taskID, Caller: t.CallerService,
			RequestID: t.RequestID, Result: status.String(), HitCount: actualCount,
		})
	}
}

// run 执行实际抓取，返回终态信息。任何依赖错误 → failed。
func (e *Executor) run(ctx context.Context, t *store.Task) (
	status store.Status, actualCount int64, warnings []store.Warning, errCode, errMsg string) {

	pitID, err := e.os.OpenPIT(ctx, e.cfg.Index, e.cfg.PITKeepAlive)
	if err != nil {
		return store.StatusFailed, 0, nil, "internal_error", "open pit failed"
	}
	defer func() {
		// ClosePIT 用独立短 ctx，避免 task ctx 已取消时关不掉 PIT 泄漏。
		closeCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = e.os.ClosePIT(closeCtx, pitID)
	}()

	w := result.NewWriter(t.TaskID, t.CallerService, e.cfg.PartMaxRows, e.cfg.PartMaxBytes, e.up)

	// 按 channel_id 字典序遍历（api-spec §5.2：parts 按 channel_id 字典序）。
	channels := make([]string, 0, len(t.Scope.Channels))
	for _, c := range t.Scope.Channels {
		channels = append(channels, c.ChannelID)
	}
	sort.Strings(channels)

	var totalSeen int64
	for _, ch := range channels {
		if ctx.Err() != nil {
			return store.StatusFailed, 0, nil, "internal_error", "cancelled"
		}
		if err := w.StartChannel(ctx, ch); err != nil {
			return store.StatusFailed, 0, nil, "internal_error", "writer start channel failed"
		}

		chSeen, chTrunc, totalCap, chErr := e.drainChannel(ctx, w, pitID, t, ch, &totalSeen)
		if chErr != nil {
			return store.StatusFailed, 0, nil, "internal_error", "search failed"
		}
		if chTrunc {
			warnings = append(warnings, store.Warning{
				ChannelID: ch, Code: store.WarnPerChannelTruncated,
				Limit: e.cfg.PerChannelHardCap, ActualSeen: chSeen,
			})
		}
		if totalCap {
			warnings = append(warnings, store.Warning{
				ChannelID: ch, Code: store.WarnTotalTruncated,
				Limit: int(e.cfg.MaxTaskHits), ActualSeen: int(totalSeen),
			})
			break
		}
	}

	parts, written, err := w.Finish(ctx)
	if err != nil {
		return store.StatusFailed, 0, nil, "internal_error", "finish writer failed"
	}
	for i := range parts {
		if err := e.store.AppendPart(ctx, &parts[i]); err != nil {
			return store.StatusFailed, 0, nil, "internal_error", "append part failed"
		}
	}

	if len(warnings) > 0 {
		return store.StatusPartial, written, warnings, "", ""
	}
	return store.StatusCompleted, written, nil, "", ""
}

// drainChannel 翻页抓取单个 channel，写入 writer。
// 返回 (本 channel 命中数, 是否因 hard cap 被截断, 是否触发全局总命中截断, err)。
func (e *Executor) drainChannel(ctx context.Context, w *result.Writer, pitID string,
	t *store.Task, channelID string, totalSeen *int64) (chSeen int, chTrunc bool, totalCap bool, err error) {

	var after []any
	for {
		if ctx.Err() != nil {
			return chSeen, false, false, ctx.Err()
		}
		page, serr := e.os.Search(ctx, osclient.SearchRequest{
			PITID:       pitID,
			KeepAlive:   e.cfg.PITKeepAlive,
			ChannelID:   channelID,
			StartTS:     t.TimeRange.StartTS,
			EndTS:       t.TimeRange.EndTS,
			Size:        e.cfg.PageSize,
			SearchAfter: after,
		})
		if serr != nil {
			return chSeen, false, false, serr
		}
		if len(page) == 0 {
			return chSeen, false, false, nil
		}
		for i := range page {
			// 全局总命中截断（兜底；submit 已 _count 拦截）。
			if *totalSeen >= e.cfg.MaxTaskHits {
				return chSeen, false, true, nil
			}
			// 单 channel hard cap 截断：还有更多命中没读完才算截断。
			// 排序为 messageId desc，抓满 hard cap 即停 → 保留最新 PerChannelHardCap 条，
			// per_channel_truncated 语义为「丢最老」。
			if chSeen >= e.cfg.PerChannelHardCap {
				return chSeen, true, false, nil
			}
			h := page[i]
			if _, werr := w.Write(ctx, result.InputMessage{
				MessageSeq: h.MessageSeq,
				From:       h.From,
				ChannelID:  h.ChannelID,
				Timestamp:  h.Timestamp,
				Payload:    h.Payload,
				PayloadRaw: h.PayloadRaw,
			}); werr != nil {
				return chSeen, false, false, werr
			}
			chSeen++
			*totalSeen++
		}
		after = page[len(page)-1].Sort
		if len(page) < e.cfg.PageSize {
			return chSeen, false, false, nil // 最后一页
		}
	}
}
