// Package cancel 实现 DELETE /v1/messages/batch/{id} 的后端逻辑（精简版）。
//
// 精简：不做后台 GC 扫描；只在取消时 CAS 状态 + 通知 executor 取消 running task
// + best-effort 清理 S3 partial。过期 task 清理留 v0.2。
package cancel

import (
	"context"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
)

// Signaller 通知 executor 取消运行中 task（executor.CancelTask 实现）。
type Signaller interface {
	CancelTask(taskID string)
}

// Cleaner 清理 task 的 S3 partial 对象（best-effort；result.S3Uploader 实现）。
type Cleaner interface {
	DeletePrefix(ctx context.Context, prefix string) error
	// TaskPrefix 返回某 task 的完整对象前缀（含环境前缀），用于精确删除。
	TaskPrefix(day, caller, taskID string) string
}

// Auditor 记录取消审计的最小接口。
type Auditor interface {
	Log(ev metrics.AuditEvent)
}

// Canceller 持有取消路径的依赖。
type Canceller struct {
	store store.Store
	sig   Signaller
	clean Cleaner
	audit Auditor
}

// New 构造 Canceller。clean / audit 可为 nil。
func New(st store.Store, sig Signaller, clean Cleaner, audit Auditor) *Canceller {
	return &Canceller{store: st, sig: sig, clean: clean, audit: audit}
}

// Result 是取消结果（handler 据此返回 JSON）。
type Result struct {
	Status   store.Status
	NotFound bool // task 不存在或非本 caller → 404
}

// Cancel 取消一个 task（幂等）。
//   - task 不存在或 caller 不匹配 → NotFound（handler 返 404）
//   - 已终态 → 返回当前 status（200，幂等）
//   - queued/running → CAS 切 cancelled + 通知 executor + 清 S3 partial
func (c *Canceller) Cancel(ctx context.Context, taskID, caller string) (Result, error) {
	t, err := c.store.Get(ctx, taskID)
	if err == store.ErrNotFound {
		return Result{NotFound: true}, nil
	}
	if err != nil {
		return Result{}, err
	}
	// caller 不匹配：对外等同不存在（api-spec §4：404）。
	if t.CallerService != caller {
		return Result{NotFound: true}, nil
	}

	if t.Status.IsTerminal() {
		return Result{Status: t.Status}, nil // 幂等
	}

	switched, cur, err := c.store.CancelCAS(ctx, taskID)
	if err != nil {
		return Result{}, err
	}
	if !switched {
		// 竞态：CAS 期间已被 worker 切到终态，返回当前状态（幂等）。
		return Result{Status: cur}, nil
	}

	// 通知 executor 取消运行中的 task（排队中的 worker 取出时自会跳过）。
	if c.sig != nil {
		c.sig.CancelTask(taskID)
	}
	// best-effort 清理 S3 partial（失败仅记录，不影响取消结果）。
	// key 格式 {env}/octo-message-export-api/{yyyy-mm-dd}/{caller}/{task_id}/...；按 task 维度精确前缀。
	// 运行中 task 的 part 几乎都落在今天（UTC）；用今天日期拼前缀。
	// TODO(v0.2): 跨天 task 的前缀可能漏匹配；当前依赖 S3 lifecycle（3d abort / 7d 过期）兜底。
	if c.clean != nil {
		day := time.Now().UTC().Format("2006-01-02")
		prefix := c.clean.TaskPrefix(day, caller, taskID)
		_ = c.clean.DeletePrefix(ctx, prefix)
	}
	if c.audit != nil {
		c.audit.Log(metrics.AuditEvent{
			Event: "delete", TaskID: taskID, Caller: caller,
			RequestID: t.RequestID, Result: "cancelled",
		})
	}
	return Result{Status: store.StatusCancelled}, nil
}
