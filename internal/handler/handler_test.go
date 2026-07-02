package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/cancel"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/config"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/submit"
)

// --- fakes ---

type fakeStore struct {
	task    *store.Task
	parts   []store.Part
	getErr  error
	created *store.Task
}

func (s *fakeStore) Get(context.Context, string) (*store.Task, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.task, nil
}
func (s *fakeStore) GetParts(context.Context, string) ([]store.Part, error) { return s.parts, nil }
func (s *fakeStore) Create(_ context.Context, t *store.Task) error          { s.created = t; return nil }
func (s *fakeStore) UpdateStatus(context.Context, string, store.Status) error {
	return nil
}
func (s *fakeStore) Finalize(context.Context, string, store.Status, int64, []store.Warning, string, string) error {
	return nil
}
func (s *fakeStore) AppendPart(context.Context, *store.Part) error { return nil }
func (s *fakeStore) CancelCAS(context.Context, string) (bool, store.Status, error) {
	return true, store.StatusCancelled, nil
}
func (s *fakeStore) ListStuck(context.Context, time.Time) ([]string, error) { return nil, nil }
func (s *fakeStore) ListExpired(context.Context, time.Time) ([]store.Task, error) {
	return nil, nil
}
func (s *fakeStore) Delete(context.Context, string) error { return nil }
func (s *fakeStore) Ping(context.Context) error           { return nil }
func (s *fakeStore) Close() error                         { return nil }

type fakePresign struct{}

func (fakePresign) Presign(_ context.Context, key string) (string, int64, error) {
	return "https://s3.local/" + key + "?sig=x", 1800000000, nil
}

type fakeCounter struct{ n int64 }

func (c fakeCounter) Count(context.Context, string, string, int64, int64) (int64, error) {
	return c.n, nil
}

type fakeGate struct{}

func (fakeGate) Acquire(context.Context, time.Duration) bool { return true }
func (fakeGate) Release()                                    {}

type fakeEnq struct{ ids []string }

func (e *fakeEnq) Enqueue(id string) error { e.ids = append(e.ids, id); return nil }

type fakeSig struct{}

func (fakeSig) CancelTask(string) {}

func testDeps(st store.Store) Deps {
	enq := &fakeEnq{}
	return Deps{
		Auth:      config.AuthConfig{Enabled: true, CallerTokens: map[string]string{"tok": "smart-summary"}},
		Submitter: submit.New(config.ProtectConfig{MaxTaskHits: 300000, SubmitWait: time.Second}, "idx", fakeCounter{n: 10}, st, fakeGate{}, enq),
		Canceller: cancel.New(st, fakeSig{}, nil, nil),
		Store:     st,
		Presign:   fakePresign{},
		Ready:     func() bool { return true },
	}
}

func authReq(method, path, body string) *http.Request {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.Header.Set("Authorization", "Bearer tok")
	return r
}

// --- tests ---

func TestSubmit_202(t *testing.T) {
	st := &fakeStore{}
	h := New(testDeps(st))
	body := `{"scope":{"channels":[{"channel_id":"c1"}]},"time_range":{"start_ts":1717862400,"end_ts":1717948800}}`
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authReq("POST", "/v1/messages/batch", body))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["task_id"] == nil || !strings.HasPrefix(resp["task_id"].(string), "ost_") {
		t.Errorf("task_id missing/bad: %v", resp)
	}
}

func TestSubmit_401NoToken(t *testing.T) {
	h := New(testDeps(&fakeStore{}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/messages/batch", strings.NewReader("{}"))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestGet_CompletedWithParts(t *testing.T) {
	st := &fakeStore{
		task: &store.Task{
			TaskID: "ost_x", CallerService: "smart-summary",
			Status: store.StatusCompleted, ActualCount: 3,
		},
		parts: []store.Part{
			{TaskID: "ost_x", PartSeq: 0, S3Key: "batch/d/c/ost_x/part-000.ndjson.gz",
				SizeBytes: 100, MessageCount: 3, SHA256: "abc", ChannelIDs: []string{"c1"}},
		},
	}
	h := New(testDeps(st))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authReq("GET", "/v1/messages/batch/ost_x", ""))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["status"] != "completed" {
		t.Errorf("status = %v, want completed", resp["status"])
	}
	parts, ok := resp["parts"].([]any)
	if !ok || len(parts) != 1 {
		t.Fatalf("parts = %v, want 1", resp["parts"])
	}
	p0 := parts[0].(map[string]any)
	if !strings.Contains(p0["url"].(string), "part-000") {
		t.Errorf("part url = %v", p0["url"])
	}
}

func TestGet_404WrongCaller(t *testing.T) {
	st := &fakeStore{task: &store.Task{TaskID: "ost_x", CallerService: "other", Status: store.StatusCompleted}}
	h := New(testDeps(st))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authReq("GET", "/v1/messages/batch/ost_x", ""))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestGet_404NotFound(t *testing.T) {
	st := &fakeStore{getErr: store.ErrNotFound}
	h := New(testDeps(st))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authReq("GET", "/v1/messages/batch/missing", ""))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestDelete_Idempotent(t *testing.T) {
	st := &fakeStore{task: &store.Task{TaskID: "ost_x", CallerService: "smart-summary", Status: store.StatusRunning}}
	h := New(testDeps(st))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authReq("DELETE", "/v1/messages/batch/ost_x", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["status"] != "cancelled" {
		t.Errorf("status = %v, want cancelled", resp["status"])
	}
}

func TestHealthz(t *testing.T) {
	h := New(testDeps(&fakeStore{}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("healthz status = %d, want 200", rec.Code)
	}
}

func TestReadyz_DrainingReturns503(t *testing.T) {
	d := testDeps(&fakeStore{})
	d.Ready = func() bool { return false }
	h := New(d)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz status = %d, want 503 while draining", rec.Code)
	}
}

func TestReadyz_DBNotReady(t *testing.T) {
	d := testDeps(&fakeStore{})
	d.DBPing = failPinger{}
	h := New(d)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz status = %d, want 503 when db down", rec.Code)
	}
}

type failPinger struct{}

func (failPinger) Ping(context.Context) error { return errors.New("down") }
