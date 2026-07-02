package executor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/osclient"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/result"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
)

// --- fakes ---

type fakeOS struct {
	// hits per channel, returned in one page if <= pageSize.
	byChannel map[string][]osclient.Hit
	pageSize  int
}

func (f *fakeOS) Count(context.Context, string, string, int64, int64) (int64, error) { return 0, nil }
func (f *fakeOS) OpenPIT(context.Context, string, time.Duration) (string, error) {
	return "pit-1", nil
}
func (f *fakeOS) ClosePIT(context.Context, string) error { return nil }
func (f *fakeOS) Ping(context.Context) error             { return nil }

func (f *fakeOS) Search(_ context.Context, req osclient.SearchRequest) ([]osclient.Hit, error) {
	all := f.byChannel[req.ChannelID]
	// search_after = last sort value (we encode index in Sort[0] as float64).
	startIdx := 0
	if len(req.SearchAfter) == 1 {
		if v, ok := req.SearchAfter[0].(int); ok {
			startIdx = v + 1
		}
	}
	end := startIdx + req.Size
	if end > len(all) {
		end = len(all)
	}
	if startIdx >= len(all) {
		return nil, nil
	}
	page := make([]osclient.Hit, 0, end-startIdx)
	for i := startIdx; i < end; i++ {
		h := all[i]
		h.Sort = []any{i}
		page = append(page, h)
	}
	return page, nil
}

type fakeStore struct {
	mu    sync.Mutex
	tasks map[string]*store.Task
	parts map[string][]store.Part
}

func newFakeStore() *fakeStore {
	return &fakeStore{tasks: map[string]*store.Task{}, parts: map[string][]store.Part{}}
}

func (s *fakeStore) Create(_ context.Context, t *store.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *t
	s.tasks[t.TaskID] = &cp
	return nil
}
func (s *fakeStore) Get(_ context.Context, id string) (*store.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *t
	return &cp, nil
}
func (s *fakeStore) UpdateStatus(_ context.Context, id string, st store.Status) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.tasks[id]; t != nil {
		t.Status = st
	}
	return nil
}
func (s *fakeStore) Finalize(_ context.Context, id string, st store.Status, ac int64, w []store.Warning, ec, em string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.tasks[id]; t != nil {
		t.Status = st
		t.ActualCount = ac
		t.Warnings = w
		t.ErrorCode = ec
		t.ErrorMessage = em
	}
	return nil
}
func (s *fakeStore) AppendPart(_ context.Context, p *store.Part) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parts[p.TaskID] = append(s.parts[p.TaskID], *p)
	return nil
}
func (s *fakeStore) GetParts(_ context.Context, id string) ([]store.Part, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.parts[id], nil
}
func (s *fakeStore) CancelCAS(context.Context, string) (bool, store.Status, error) {
	return false, 0, nil
}
func (s *fakeStore) ListStuck(context.Context, time.Time) ([]string, error)       { return nil, nil }
func (s *fakeStore) ListExpired(context.Context, time.Time) ([]store.Task, error) { return nil, nil }
func (s *fakeStore) Delete(context.Context, string) error                         { return nil }
func (s *fakeStore) Ping(context.Context) error                                   { return nil }
func (s *fakeStore) Close() error                                                 { return nil }

type fakeUploader struct {
	mu    sync.Mutex
	parts map[string][]byte
}

func (u *fakeUploader) Upload(_ context.Context, taskID, caller string, seq int, day string, data []byte) (string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.parts == nil {
		u.parts = map[string][]byte{}
	}
	key := day + "/" + taskID + "/" + caller
	cp := make([]byte, len(data))
	copy(cp, data)
	u.parts[key] = cp
	return key, nil
}

func textHit(seq int64, ch, content string) osclient.Hit {
	return osclient.Hit{
		MessageSeq: seq,
		From:       "u1",
		ChannelID:  ch,
		Timestamp:  1700000000 + seq,
		Payload:    map[string]any{"type": 1, "text": map[string]any{"content": content}},
	}
}

func newTestExecutor(os osclient.Client, st store.Store, up result.Uploader, cfg Config) *Executor {
	gate := NewGate(10)
	gate.Acquire(context.Background(), 0) // 模拟 submit 已占槽
	return New(cfg, os, st, up, gate, nil)
}

// --- tests ---

func TestProcess_Completed(t *testing.T) {
	os := &fakeOS{pageSize: 2, byChannel: map[string][]osclient.Hit{
		"chanA": {textHit(1, "chanA", "a1"), textHit(2, "chanA", "a2"), textHit(3, "chanA", "a3")},
		"chanB": {textHit(1, "chanB", "b1")},
	}}
	st := newFakeStore()
	_ = st.Create(context.Background(), &store.Task{
		TaskID: "ost_x", CallerService: "c", Status: store.StatusQueued,
		Scope:     store.Scope{Channels: []store.Channel{{ChannelID: "chanB"}, {ChannelID: "chanA"}}},
		TimeRange: store.TimeRange{StartTS: 1, EndTS: 9999999999},
	})
	up := &fakeUploader{}
	e := newTestExecutor(os, st, up, Config{
		Index: "idx", Workers: 1, PartMaxRows: 1000, PartMaxBytes: 1 << 30,
		MaxTaskHits: 1000, PerChannelHardCap: 1000, PageSize: 2,
	})
	e.process(context.Background(), "ost_x")

	got, _ := st.Get(context.Background(), "ost_x")
	if got.Status != store.StatusCompleted {
		t.Fatalf("status = %s, want completed (err=%s)", got.Status, got.ErrorMessage)
	}
	if got.ActualCount != 4 {
		t.Errorf("actual_count = %d, want 4", got.ActualCount)
	}
	parts, _ := st.GetParts(context.Background(), "ost_x")
	if len(parts) != 2 {
		t.Fatalf("got %d parts, want 2 (one per channel)", len(parts))
	}
	// parts 按 channel 字典序：chanA 先于 chanB。
	if parts[0].ChannelIDs[0] != "chanA" || parts[1].ChannelIDs[0] != "chanB" {
		t.Errorf("parts order = %v / %v, want chanA then chanB", parts[0].ChannelIDs, parts[1].ChannelIDs)
	}
}

func TestProcess_PartialPerChannelTruncated(t *testing.T) {
	hits := make([]osclient.Hit, 5)
	for i := range hits {
		hits[i] = textHit(int64(i+1), "chanA", "x")
	}
	os := &fakeOS{pageSize: 2, byChannel: map[string][]osclient.Hit{"chanA": hits}}
	st := newFakeStore()
	_ = st.Create(context.Background(), &store.Task{
		TaskID: "ost_t", CallerService: "c", Status: store.StatusQueued,
		Scope:     store.Scope{Channels: []store.Channel{{ChannelID: "chanA"}}},
		TimeRange: store.TimeRange{StartTS: 1, EndTS: 9999999999},
	})
	e := newTestExecutor(os, st, &fakeUploader{}, Config{
		Index: "idx", Workers: 1, PartMaxRows: 1000, PartMaxBytes: 1 << 30,
		MaxTaskHits: 1000, PerChannelHardCap: 3, PageSize: 2,
	})
	e.process(context.Background(), "ost_t")

	got, _ := st.Get(context.Background(), "ost_t")
	if got.Status != store.StatusPartial {
		t.Fatalf("status = %s, want partial", got.Status)
	}
	if len(got.Warnings) != 1 || got.Warnings[0].Code != store.WarnPerChannelTruncated {
		t.Fatalf("warnings = %+v, want per_channel_truncated", got.Warnings)
	}
	if got.ActualCount != 3 {
		t.Errorf("actual_count = %d, want 3 (capped)", got.ActualCount)
	}
}

func TestProcess_CancelledViaCtx(t *testing.T) {
	hits := make([]osclient.Hit, 100)
	for i := range hits {
		hits[i] = textHit(int64(i+1), "chanA", "x")
	}
	os := &fakeOS{pageSize: 1, byChannel: map[string][]osclient.Hit{"chanA": hits}}
	st := newFakeStore()
	_ = st.Create(context.Background(), &store.Task{
		TaskID: "ost_c", CallerService: "c", Status: store.StatusQueued,
		Scope:     store.Scope{Channels: []store.Channel{{ChannelID: "chanA"}}},
		TimeRange: store.TimeRange{StartTS: 1, EndTS: 9999999999},
	})
	e := newTestExecutor(os, st, &fakeUploader{}, Config{
		Index: "idx", Workers: 1, PartMaxRows: 1000, PartMaxBytes: 1 << 30,
		MaxTaskHits: 100000, PerChannelHardCap: 100000, PageSize: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消
	e.process(ctx, "ost_c")

	got, _ := st.Get(context.Background(), "ost_c")
	if got.Status != store.StatusCancelled {
		t.Fatalf("status = %s, want cancelled", got.Status)
	}
}

func TestEnqueue_QueueFull(t *testing.T) {
	e := New(Config{Workers: 1, QueueBuffer: 1}, nil, nil, nil, NewGate(1), nil)
	if err := e.Enqueue("a"); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := e.Enqueue("b"); err != ErrQueueFull {
		t.Errorf("second enqueue err = %v, want ErrQueueFull", err)
	}
}
