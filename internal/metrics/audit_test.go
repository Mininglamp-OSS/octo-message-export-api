package metrics

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSizeBucket(t *testing.T) {
	cases := []struct {
		count int64
		want  string
	}{
		{0, "30k"}, {30000, "30k"}, {30001, "100k"},
		{100000, "100k"}, {100001, "300k"}, {500000, "300k"},
	}
	for _, tc := range cases {
		if got := SizeBucket(tc.count); got != tc.want {
			t.Errorf("SizeBucket(%d) = %q, want %q", tc.count, got, tc.want)
		}
	}
}

func TestAuditorWritesJSONL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "audit.jsonl")
	a, err := NewAuditor(path)
	if err != nil {
		t.Fatalf("NewAuditor: %v", err)
	}
	a.Log(AuditEvent{Event: "submit", TaskID: "ost_1", Caller: "smart-summary", Result: "async_accepted"})
	a.Log(AuditEvent{Event: "delete", TaskID: "ost_1", Caller: "smart-summary", TS: 123})
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open written file: %v", err)
	}
	defer f.Close()

	var lines []AuditEvent
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ev AuditEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			t.Fatalf("line not valid json: %v", err)
		}
		lines = append(lines, ev)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if lines[0].TS == 0 {
		t.Error("first event TS should be auto-filled")
	}
	if lines[1].TS != 123 {
		t.Errorf("second event TS = %d, want 123 (explicit)", lines[1].TS)
	}
}

func TestAuditorEmptyPathStdoutOnly(t *testing.T) {
	a, err := NewAuditor("")
	if err != nil {
		t.Fatalf("NewAuditor(\"\"): %v", err)
	}
	// 不应 panic；只写 stdout。
	a.Log(AuditEvent{Event: "get", TaskID: "ost_x"})
	if err := a.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
