// Package auth 实现 s2s token 鉴权中间件 + X-Request-Id 注入/回写（api-spec §1）。
//
// 鉴权模型：服务间互信 + 固定长效 token。中间件查 Authorization 头的 Bearer token
// 是否命中 cfg.CallerTokens（token→caller 反查表），命中即认定 caller 身份并注入 ctx。
// Enabled=false 时直通，caller 注入 "local-dev"。
package auth

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"strings"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/config"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
)

// ctxKey 是本包私有的 context key 类型，避免与其它包冲突。
type ctxKey int

const (
	callerKey ctxKey = iota
	requestIDKey
)

const localDevCaller = "local-dev"

// Auditor 是中间件记录 auth_fail 审计所需的最小接口。
type Auditor interface {
	Log(ev metrics.AuditEvent)
}

// Middleware 返回鉴权 + request-id 处理中间件。
// 健康检查 / metrics 路由不应套此中间件（由路由层决定）。
func Middleware(cfg config.AuthConfig, audit Auditor, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// X-Request-Id：client 传则用，否则生成；响应头一律回写。
		reqID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if reqID == "" {
			reqID = NewRequestID()
		}
		w.Header().Set("X-Request-Id", reqID)

		ctx := context.WithValue(r.Context(), requestIDKey, reqID)

		caller := localDevCaller
		if cfg.Enabled {
			token := bearerToken(r.Header.Get("Authorization"))
			name, ok := cfg.CallerTokens[token]
			if !ok || token == "" {
				if audit != nil {
					audit.Log(metrics.AuditEvent{Event: "auth_fail", RequestID: reqID, Result: "unauthorized"})
				}
				writeUnauthorized(w, reqID)
				return
			}
			caller = name
		}
		ctx = context.WithValue(ctx, callerKey, caller)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearerToken 从 "Bearer <token>" 头中取出 token（大小写不敏感前缀）。
func bearerToken(header string) string {
	header = strings.TrimSpace(header)
	if len(header) < 7 || !strings.EqualFold(header[:7], "bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}

func writeUnauthorized(w http.ResponseWriter, reqID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	fmt.Fprintf(w, `{"error_code":"unauthorized","error_message":"invalid or missing s2s token","request_id":%q}`+"\n", reqID)
}

// Caller 从 ctx 取 caller 名（未命中返回空串）。
func Caller(ctx context.Context) string {
	if v, ok := ctx.Value(callerKey).(string); ok {
		return v
	}
	return ""
}

// RequestID 从 ctx 取 request id（未命中返回空串）。
func RequestID(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// NewRequestID 生成一个 UUID v4 字符串。无第三方依赖，用 crypto/rand。
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败极罕见；退化为全 0（仍是合法格式，不阻塞请求）。
		return "00000000-0000-4000-8000-000000000000"
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
