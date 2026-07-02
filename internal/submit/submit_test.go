package submit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/config"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
)

// --- fakes ---

type fakeCounter struct {
	perChannel int64
	err        error
}

func (c *fakeCounter) Count(context.Context, string, string, int64, int64) (int64, error) {
	return c.perChannel, c.err
}

type fakeGate struct {
	acquireOK bool
	released  int
}

func (g *fakeGate) Acquire(context.Context, time.Duration) bool { return g.acquireOK }
func (g *fakeGate) Release()                                    { g.released++ }

type fakeEnq struct {
	err      error
	enqueued []string
}

func (e *fakeEnq) Enqueue(id string) error {
	if e.err != nil {
		return e.err
	}
	e.enqueued = append(e.enqueued, id)
	return nil
}

type fakeStore struct {
	created   *store.Task
	createErr error
	finalized bool
}

func (s *fakeStore) Create(_ context.Context, t *store.Task) error {
	if s.createErr != nil {
		return s.createErr
	}
	s.created = t
	return nil
}
func (s *fakeStore) Finalize(context.Context, string, store.Status, int64, []store.Warning, string, string) error {
	s.finalized = true
	return nil
}

// 其余接口方法 submit 不用，留空实现满足 store.Store。
func (s *fakeStore) Get(context.Context, string) (*store.Task, error)         { return nil, nil }
func (s *fakeStore) UpdateStatus(context.Context, string, store.Status) error { return nil }
func (s *fakeStore) AppendPart(context.Context, *store.Part) error            { return nil }
func (s *fakeStore) GetParts(context.Context, string) ([]store.Part, error)   { return nil, nil }
func (s *fakeStore) CancelCAS(context.Context, string) (bool, store.Status, error) {
	return false, 0, nil
}
func (s *fakeStore) ListStuck(context.Context, time.Time) ([]string, error)       { return nil, nil }
func (s *fakeStore) ListExpired(context.Context, time.Time) ([]store.Task, error) { return nil, nil }
func (s *fakeStore) Delete(context.Context, string) error                         { return nil }
func (s *fakeStore) Ping(context.Context) error                                   { return nil }
func (s *fakeStore) Close() error                                                 { return nil }

func protectCfg() config.ProtectConfig {
	return config.ProtectConfig{
		GlobalInflightMax: 10,
		SubmitWait:        time.Second,
		MaxTaskHits:       300000,
		PerChannelHardCap: 100000,
	}
}

func validReq() *Request {
	return &Request{
		Scope:     store.Scope{Channels: []store.Channel{{ChannelID: "ch1"}, {ChannelID: "ch2"}}},
		TimeRange: store.TimeRange{StartTS: 1717862400, EndTS: 1717948800},
	}
}

// --- tests ---

func TestSubmit_OK(t *testing.T) {
	st := &fakeStore{}
	gate := &fakeGate{acquireOK: true}
	enq := &fakeEnq{}
	s := New(protectCfg(), "idx", &fakeCounter{perChannel: 100}, st, gate, enq)

	id, err := s.Submit(context.Background(), "smart-summary", "req-1", validReq())
	if err != nil {
		t.Fatalf("Submit err = %v", err)
	}
	if id == "" || id[:4] != "ost_" {
		t.Errorf("task_id = %q, want ost_ prefix", id)
	}
	if st.created == nil || st.created.ActualCount != 200 {
		t.Errorf("created task actual_count = %v, want 200 (2 channels x 100)", st.created)
	}
	if len(enq.enqueued) != 1 || enq.enqueued[0] != id {
		t.Errorf("enqueued = %v, want [%s]", enq.enqueued, id)
	}
}

func TestSubmit_TooManyChannels(t *testing.T) {
	chans := make([]store.Channel, 31)
	for i := range chans {
		chans[i] = store.Channel{ChannelID: "c"}
	}
	req := &Request{Scope: store.Scope{Channels: chans}, TimeRange: store.TimeRange{StartTS: 1, EndTS: 2}}
	s := New(protectCfg(), "idx", &fakeCounter{}, &fakeStore{}, &fakeGate{acquireOK: true}, &fakeEnq{})
	_, err := s.Submit(context.Background(), "c", "r", req)
	assertErr(t, err, 413, "single_task_too_large")
}

func TestSubmit_MillisecondTS(t *testing.T) {
	req := validReq()
	req.TimeRange.StartTS = 1717862400000 // ms
	s := New(protectCfg(), "idx", &fakeCounter{}, &fakeStore{}, &fakeGate{acquireOK: true}, &fakeEnq{})
	_, err := s.Submit(context.Background(), "c", "r", req)
	assertErr(t, err, 400, "invalid_request")
}

func TestSubmit_RangeTooLong(t *testing.T) {
	req := validReq()
	req.TimeRange.StartTS = 1700000000
	req.TimeRange.EndTS = 1700000000 + 32*24*3600
	s := New(protectCfg(), "idx", &fakeCounter{}, &fakeStore{}, &fakeGate{acquireOK: true}, &fakeEnq{})
	_, err := s.Submit(context.Background(), "c", "r", req)
	assertErr(t, err, 422, "time_range_too_long")
}

func TestSubmit_CountExceedsMax(t *testing.T) {
	cfg := protectCfg()
	cfg.MaxTaskHits = 150
	s := New(cfg, "idx", &fakeCounter{perChannel: 100}, &fakeStore{}, &fakeGate{acquireOK: true}, &fakeEnq{})
	_, err := s.Submit(context.Background(), "c", "r", validReq()) // 2x100 = 200 > 150
	assertErr(t, err, 413, "single_task_too_large")
}

func TestSubmit_GateFull(t *testing.T) {
	s := New(protectCfg(), "idx", &fakeCounter{perChannel: 10}, &fakeStore{}, &fakeGate{acquireOK: false}, &fakeEnq{})
	_, err := s.Submit(context.Background(), "c", "r", validReq())
	assertErr(t, err, 503, "service_degraded")
}

func TestSubmit_QueueFullReleasesGate(t *testing.T) {
	gate := &fakeGate{acquireOK: true}
	st := &fakeStore{}
	s := New(protectCfg(), "idx", &fakeCounter{perChannel: 10}, st, gate, &fakeEnq{err: errors.New("full")})
	_, err := s.Submit(context.Background(), "c", "r", validReq())
	assertErr(t, err, 503, "service_degraded")
	if gate.released != 1 {
		t.Errorf("gate released %d times, want 1", gate.released)
	}
	if !st.finalized {
		t.Error("task should be finalized as failed on queue full")
	}
}

func TestNewTaskID_Unique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewTaskID()
		if len(id) != 4+24 {
			t.Fatalf("len = %d, want 28 (%q)", len(id), id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func assertErr(t *testing.T, err error, wantStatus int, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var se *Error
	if !errors.As(err, &se) {
		t.Fatalf("error %v is not *submit.Error", err)
	}
	if se.HTTPStatus != wantStatus || se.Code != wantCode {
		t.Errorf("err = (%d, %s), want (%d, %s)", se.HTTPStatus, se.Code, wantStatus, wantCode)
	}
}
