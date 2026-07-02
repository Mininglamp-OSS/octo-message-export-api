package store

import (
	"context"
	"os"
	"testing"
	"time"
)

// mysqlDSN 从 TEST_MYSQL_DSN（优先）或 MYSQL_DSN 读取；都没有则 skip。
// 本地跑：MYSQL_DSN=... go test ./internal/store/
func mysqlDSN(t *testing.T) string {
	t.Helper()
	for _, k := range []string{"TEST_MYSQL_DSN", "MYSQL_DSN"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	t.Skip("no MYSQL_DSN / TEST_MYSQL_DSN set; skipping MySQL integration test")
	return ""
}

func newTestStore(t *testing.T) *MySQLStore {
	t.Helper()
	s, err := NewMySQLStore(mysqlDSN(t), 4)
	if err != nil {
		t.Fatalf("NewMySQLStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func uniqueTaskID() string {
	return "ost_test_" + time.Now().Format("150405.000000000")
}

func TestMySQLCRUDLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id := uniqueTaskID()

	task := &Task{
		TaskID:        id,
		CallerService: "smart-summary",
		Status:        StatusQueued,
		Scope:         Scope{Channels: []Channel{{ChannelID: "test0002@test0001"}}},
		TimeRange:     TimeRange{StartTS: 1717862400, EndTS: 1717948800},
		RequestID:     "req-123",
	}
	if err := s.Create(ctx, task); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = s.Delete(ctx, id) })

	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusQueued || got.CallerService != "smart-summary" {
		t.Errorf("unexpected task: %+v", got)
	}
	if len(got.Scope.Channels) != 1 || got.Scope.Channels[0].ChannelID != "test0002@test0001" {
		t.Errorf("scope roundtrip failed: %+v", got.Scope)
	}
	if got.ExpiresAt <= got.CreatedAt {
		t.Errorf("expires_at (%d) should be after created_at (%d)", got.ExpiresAt, got.CreatedAt)
	}

	// running
	if err := s.UpdateStatus(ctx, id, StatusRunning); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	// append part
	part := &Part{
		TaskID:       id,
		PartSeq:      0,
		S3Key:        "batch/2026-06-10/smart-summary/" + id + "/part-000.ndjson.gz",
		SizeBytes:    1234,
		MessageCount: 8,
		SHA256:       "abc123",
		ChannelIDs:   []string{"test0002@test0001"},
	}
	if err := s.AppendPart(ctx, part); err != nil {
		t.Fatalf("AppendPart: %v", err)
	}
	// idempotent re-append (ON DUPLICATE KEY UPDATE)
	part.SizeBytes = 5678
	if err := s.AppendPart(ctx, part); err != nil {
		t.Fatalf("AppendPart (dup): %v", err)
	}
	parts, err := s.GetParts(ctx, id)
	if err != nil {
		t.Fatalf("GetParts: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("got %d parts, want 1 (idempotent)", len(parts))
	}
	if parts[0].SizeBytes != 5678 {
		t.Errorf("part size_bytes = %d, want 5678 (overwritten)", parts[0].SizeBytes)
	}

	// finalize partial with warnings
	warns := []Warning{{ChannelID: "test0002@test0001", Code: WarnPerChannelTruncated, Limit: 100000, ActualSeen: 110000}}
	if err := s.Finalize(ctx, id, StatusPartial, 8, warns, "", ""); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got, _ = s.Get(ctx, id)
	if got.Status != StatusPartial || got.ActualCount != 8 {
		t.Errorf("finalize: status=%s count=%d", got.Status, got.ActualCount)
	}
	if len(got.Warnings) != 1 || got.Warnings[0].Code != WarnPerChannelTruncated {
		t.Errorf("warnings roundtrip failed: %+v", got.Warnings)
	}
}

func TestMySQLCancelCAS(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id := uniqueTaskID()

	task := &Task{
		TaskID:        id,
		CallerService: "smart-summary",
		Status:        StatusQueued,
		Scope:         Scope{Channels: []Channel{{ChannelID: "c1"}}},
		TimeRange:     TimeRange{StartTS: 1, EndTS: 2},
	}
	if err := s.Create(ctx, task); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = s.Delete(ctx, id) })

	// first cancel succeeds
	ok, st, err := s.CancelCAS(ctx, id)
	if err != nil {
		t.Fatalf("CancelCAS: %v", err)
	}
	if !ok || st != StatusCancelled {
		t.Fatalf("first cancel: ok=%v st=%s", ok, st)
	}
	// second cancel is idempotent: ok=false, terminal status
	ok, st, err = s.CancelCAS(ctx, id)
	if err != nil {
		t.Fatalf("CancelCAS 2: %v", err)
	}
	if ok || st != StatusCancelled {
		t.Errorf("second cancel: ok=%v st=%s, want false/cancelled", ok, st)
	}
}

func TestMySQLGetNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get(context.Background(), "ost_does_not_exist_xyz")
	if err != ErrNotFound {
		t.Errorf("Get missing: err = %v, want ErrNotFound", err)
	}
}
