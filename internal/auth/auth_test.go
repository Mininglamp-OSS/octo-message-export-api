package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/config"
)

func TestMiddleware_ValidToken(t *testing.T) {
	cfg := config.AuthConfig{
		Enabled:      true,
		CallerTokens: map[string]string{"sk_abc": "smart-summary"},
	}
	var gotCaller, gotReqID string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCaller = Caller(r.Context())
		gotReqID = RequestID(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := Middleware(cfg, nil, next)

	req := httptest.NewRequest(http.MethodGet, "/v1/messages/batch/x", nil)
	req.Header.Set("Authorization", "Bearer sk_abc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotCaller != "smart-summary" {
		t.Errorf("caller = %q, want smart-summary", gotCaller)
	}
	if gotReqID == "" {
		t.Error("request id not injected into ctx")
	}
	if rec.Header().Get("X-Request-Id") != gotReqID {
		t.Errorf("response X-Request-Id %q != ctx %q", rec.Header().Get("X-Request-Id"), gotReqID)
	}
}

func TestMiddleware_BadToken(t *testing.T) {
	cfg := config.AuthConfig{Enabled: true, CallerTokens: map[string]string{"sk_abc": "smart-summary"}}
	called := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	h := Middleware(cfg, nil, next)

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Error("next handler should not be called on bad token")
	}
	if rec.Header().Get("X-Request-Id") == "" {
		t.Error("X-Request-Id should be echoed even on 401")
	}
}

func TestMiddleware_DisabledInjectsLocalDev(t *testing.T) {
	cfg := config.AuthConfig{Enabled: false}
	var gotCaller string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCaller = Caller(r.Context())
	})
	h := Middleware(cfg, nil, next)
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if gotCaller != localDevCaller {
		t.Errorf("caller = %q, want %q", gotCaller, localDevCaller)
	}
}

func TestMiddleware_ClientRequestIDPreserved(t *testing.T) {
	cfg := config.AuthConfig{Enabled: false}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	h := Middleware(cfg, nil, next)
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("X-Request-Id", "client-supplied-id")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-Id") != "client-supplied-id" {
		t.Errorf("X-Request-Id = %q, want client-supplied-id", rec.Header().Get("X-Request-Id"))
	}
}

func TestNewRequestID_Format(t *testing.T) {
	id := NewRequestID()
	// UUID v4: 8-4-4-4-12 = 36 chars, version nibble '4'.
	if len(id) != 36 {
		t.Fatalf("len = %d, want 36 (%q)", len(id), id)
	}
	if id[14] != '4' {
		t.Errorf("version nibble = %c, want 4 (%q)", id[14], id)
	}
}

func TestBearerToken(t *testing.T) {
	cases := map[string]string{
		"Bearer sk_abc": "sk_abc",
		"bearer sk_abc": "sk_abc",
		"sk_abc":        "",
		"":              "",
		"Bearer ":       "",
	}
	for in, want := range cases {
		if got := bearerToken(in); got != want {
			t.Errorf("bearerToken(%q) = %q, want %q", in, got, want)
		}
	}
}
