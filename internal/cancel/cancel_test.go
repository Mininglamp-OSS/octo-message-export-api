package cancel

import (
	"context"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
)

type fakeStore struct {
	task      *store.Task
	getErr    error
	casSwitch bool
	casStatus store.Status
}

func (s *fakeStore) Get(context.Context, string) (*store.Task, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.task, nil
}
func (s *fakeStore) CancelCAS(context.Context, string) (bool, store.Status, error) {
	return s.casSwitch, s.casStatus, nil
}

func (s *fakeStore) Create(context.Context, *store.Task) error                { return nil }
func (s *fakeStore) UpdateStatus(context.Context, string, store.Status) error { return nil }
func (s *fakeStore) Finalize(context.Context, string, store.Status, int64, []store.Warning, string, string) error {
	return nil
}
func (s *fakeStore) AppendPart(context.Context, *store.Part) error          { return nil }
func (s *fakeStore) GetParts(context.Context, string) ([]store.Part, error) { return nil, nil }
func (s *fakeStore) ListStuck(context.Context, time.Time) ([]string, error) { return nil, nil }
func (s *fakeStore) ListExpired(context.Context, time.Time) ([]store.Task, error) {
	return nil, nil
}
func (s *fakeStore) Delete(context.Context, string) error { return nil }
func (s *fakeStore) Ping(context.Context) error           { return nil }
func (s *fakeStore) Close() error                         { return nil }

type fakeSig struct{ called []string }

func (f *fakeSig) CancelTask(id string) { f.called = append(f.called, id) }

func TestCancel_NotFound(t *testing.T) {
	c := New(&fakeStore{getErr: store.ErrNotFound}, &fakeSig{}, nil, nil)
	res, err := c.Cancel(context.Background(), "x", "caller")
	if err != nil {
		t.Fatal(err)
	}
	if !res.NotFound {
		t.Error("want NotFound")
	}
}

func TestCancel_WrongCaller(t *testing.T) {
	st := &fakeStore{task: &store.Task{TaskID: "x", CallerService: "other", Status: store.StatusRunning}}
	c := New(st, &fakeSig{}, nil, nil)
	res, err := c.Cancel(context.Background(), "x", "caller")
	if err != nil {
		t.Fatal(err)
	}
	if !res.NotFound {
		t.Error("wrong caller should map to NotFound (404)")
	}
}

func TestCancel_AlreadyTerminalIdempotent(t *testing.T) {
	st := &fakeStore{task: &store.Task{TaskID: "x", CallerService: "c", Status: store.StatusCompleted}}
	sig := &fakeSig{}
	c := New(st, sig, nil, nil)
	res, err := c.Cancel(context.Background(), "x", "c")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != store.StatusCompleted {
		t.Errorf("status = %s, want completed (idempotent)", res.Status)
	}
	if len(sig.called) != 0 {
		t.Error("should not signal executor for terminal task")
	}
}

func TestCancel_RunningSwitches(t *testing.T) {
	st := &fakeStore{
		task:      &store.Task{TaskID: "x", CallerService: "c", Status: store.StatusRunning},
		casSwitch: true, casStatus: store.StatusCancelled,
	}
	sig := &fakeSig{}
	c := New(st, sig, nil, nil)
	res, err := c.Cancel(context.Background(), "x", "c")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != store.StatusCancelled {
		t.Errorf("status = %s, want cancelled", res.Status)
	}
	if len(sig.called) != 1 || sig.called[0] != "x" {
		t.Errorf("signal called = %v, want [x]", sig.called)
	}
}

func TestCancel_RaceLostCAS(t *testing.T) {
	// CAS 失败（worker 已切终态）：返回当前状态，不再 signal。
	st := &fakeStore{
		task:      &store.Task{TaskID: "x", CallerService: "c", Status: store.StatusRunning},
		casSwitch: false, casStatus: store.StatusCompleted,
	}
	sig := &fakeSig{}
	c := New(st, sig, nil, nil)
	res, err := c.Cancel(context.Background(), "x", "c")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != store.StatusCompleted {
		t.Errorf("status = %s, want completed", res.Status)
	}
	if len(sig.called) != 0 {
		t.Error("should not signal when CAS lost")
	}
}
