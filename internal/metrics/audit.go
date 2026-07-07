package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AuditEvent 一条审计记录（v1 落本地）。
type AuditEvent struct {
	TS        int64  `json:"ts"`
	Event     string `json:"event"` // submit | get | delete | auth_fail | system_protection_reject
	TaskID    string `json:"task_id,omitempty"`
	Caller    string `json:"caller,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Result    string `json:"result,omitempty"`
	// ScopeSummary / TimeRange 可选摘要。
	ChannelCount int   `json:"channel_count,omitempty"`
	TimeStart    int64 `json:"time_start,omitempty"`
	TimeEnd      int64 `json:"time_end,omitempty"`
	HitCount     int64 `json:"hit_count,omitempty"`
}

// Auditor 写审计日志：既追加到本地 jsonl，又 println 到 stdout
// （emptyDir 不持久，靠 stdout 副本被 K8s 收集）。
type Auditor struct {
	mu  sync.Mutex
	f   *os.File
	now func() time.Time
}

// NewAuditor 打开（或创建）审计文件。path 为空则只写 stdout。
func NewAuditor(path string) (*Auditor, error) {
	a := &Auditor{now: time.Now}
	if path == "" {
		return a, nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		// 尽力创建目录；失败不阻塞启动（降级到只 stdout）。
		_ = os.MkdirAll(dir, 0o755)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open audit file %q: %w", path, err)
	}
	a.f = f
	return a, nil
}

// Log 写一条审计事件。TS 未设置时自动填当前秒级时间戳。
// 单条写失败不影响主流程（best-effort）。
func (a *Auditor) Log(ev AuditEvent) {
	if ev.TS == 0 {
		ev.TS = a.now().Unix()
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return
	}
	// stdout 副本。
	fmt.Println(string(line))

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f != nil {
		_, _ = a.f.Write(append(line, '\n'))
	}
}

// Close 关闭底层文件。
func (a *Auditor) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f != nil {
		return a.f.Close()
	}
	return nil
}
