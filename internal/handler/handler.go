// Package handler 实现 HTTP 路由 + 请求解析 + 响应序列化。
//
// 精简：用标准库 net/http 1.22+ ServeMux 的 method+path 路由，不引 gorilla/mux。
// 鉴权 / X-Request-Id 由 auth.Middleware 处理（仅业务路由套；健康检查 / metrics 直通）。
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/auth"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/cancel"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/config"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/submit"
)

// Presigner 为 part S3 key 生成 presigned GET URL（result.S3Uploader 实现）。
type Presigner interface {
	Presign(ctx context.Context, key string) (url string, expiresAt int64, err error)
}

// Pinger 是 readyz 探活的依赖（store / osclient 实现 Ping）。
type Pinger interface {
	Ping(ctx context.Context) error
}

// Deps 是 handler 的依赖集合。
type Deps struct {
	Auth      config.AuthConfig
	Submitter *submit.Submitter
	Canceller *cancel.Canceller
	Store     store.Store
	Presign   Presigner
	Audit     auth.Auditor
	DBPing    Pinger
	OSPing    Pinger

	// Ready 由 main 控制：优雅关闭时置 false 让 /readyz 返回 503。
	Ready func() bool
}

// New 构造完整的 http.Handler（含路由 + 中间件装配）。
func New(d Deps) http.Handler {
	mux := http.NewServeMux()

	// 业务路由：套 auth 中间件。
	biz := http.NewServeMux()
	biz.HandleFunc("POST /v1/messages/batch", d.handleSubmit)
	biz.HandleFunc("GET /v1/messages/batch/{id}", d.handleGet)
	biz.HandleFunc("DELETE /v1/messages/batch/{id}", d.handleCancel)
	authed := auth.Middleware(d.Auth, d.Audit, biz)

	mux.Handle("/v1/", authed)

	// 健康检查 / metrics：不走 auth。
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", d.handleReadyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))

	return mux
}

func (d Deps) handleSubmit(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	caller := auth.Caller(r.Context())
	reqID := auth.RequestID(r.Context())

	var req submit.Request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, 400, "invalid_request", "malformed JSON body", reqID)
		return
	}

	taskID, err := d.Submitter.Submit(r.Context(), caller, reqID, &req)
	if err != nil {
		var se *submit.Error
		if errors.As(err, &se) {
			writeError(w, se.HTTPStatus, se.Code, se.Message, reqID)
		} else {
			writeError(w, 500, "internal_error", "submit failed", reqID)
		}
		return
	}
	metrics.SubmitDuration.WithLabelValues(caller).Observe(time.Since(start).Seconds())

	if d.Audit != nil {
		d.Audit.Log(metrics.AuditEvent{
			Event: "submit", TaskID: taskID, Caller: caller, RequestID: reqID,
			Result: "accepted", ChannelCount: len(req.Scope.Channels),
			TimeStart: req.TimeRange.StartTS, TimeEnd: req.TimeRange.EndTS,
		})
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task_id": taskID, "request_id": reqID})
}

// partOut 是 GET 响应里的单个 part（api-spec §3.2）。
type partOut struct {
	URL          string   `json:"url"`
	SizeBytes    int64    `json:"size_bytes"`
	MessageCount int64    `json:"message_count"`
	SHA256       string   `json:"sha256"`
	ExpiresAt    int64    `json:"expires_at"`
	ChannelIDs   []string `json:"channel_ids"`
}

func (d Deps) handleGet(w http.ResponseWriter, r *http.Request) {
	caller := auth.Caller(r.Context())
	reqID := auth.RequestID(r.Context())
	taskID := r.PathValue("id")

	t, err := d.Store.Get(r.Context(), taskID)
	if err == store.ErrNotFound || (t != nil && t.CallerService != caller) {
		writeError(w, 404, "not_found", "task not found", reqID)
		return
	}
	if err != nil {
		writeError(w, 500, "internal_error", "get task failed", reqID)
		return
	}

	resp := map[string]any{
		"status":     t.Status.String(),
		"request_id": reqID,
	}

	if t.Status == store.StatusCompleted || t.Status == store.StatusPartial {
		resp["actual_count"] = t.ActualCount
		parts, err := d.Store.GetParts(r.Context(), taskID)
		if err != nil {
			writeError(w, 500, "internal_error", "get parts failed", reqID)
			return
		}
		out := make([]partOut, 0, len(parts))
		for _, p := range parts {
			url, expires, err := d.Presign.Presign(r.Context(), p.S3Key)
			if err != nil {
				writeError(w, 500, "internal_error", "presign failed", reqID)
				return
			}
			out = append(out, partOut{
				URL: url, SizeBytes: p.SizeBytes, MessageCount: p.MessageCount,
				SHA256: p.SHA256, ExpiresAt: expires, ChannelIDs: p.ChannelIDs,
			})
		}
		resp["parts"] = out
		if len(t.Warnings) > 0 {
			resp["warnings"] = t.Warnings
		}
	}

	if t.Status == store.StatusFailed {
		resp["error_code"] = t.ErrorCode
		resp["error_message"] = t.ErrorMessage
	}

	writeJSON(w, http.StatusOK, resp)
}

func (d Deps) handleCancel(w http.ResponseWriter, r *http.Request) {
	caller := auth.Caller(r.Context())
	reqID := auth.RequestID(r.Context())
	taskID := r.PathValue("id")

	res, err := d.Canceller.Cancel(r.Context(), taskID, caller)
	if err != nil {
		writeError(w, 500, "internal_error", "cancel failed", reqID)
		return
	}
	if res.NotFound {
		writeError(w, 404, "not_found", "task not found", reqID)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"task_id": taskID, "status": res.Status.String(), "request_id": reqID,
	})
}

func (d Deps) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if d.Ready != nil && !d.Ready() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if d.DBPing != nil {
		if err := d.DBPing.Ping(ctx); err != nil {
			http.Error(w, "db not ready", http.StatusServiceUnavailable)
			return
		}
	}
	if d.OSPing != nil {
		if err := d.OSPing.Ping(ctx); err != nil {
			http.Error(w, "os not ready", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-OctoSearch-Proto-Version", "v0.1")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, msg, reqID string) {
	writeJSON(w, status, map[string]any{
		"error_code": code, "error_message": msg, "request_id": reqID,
	})
}
