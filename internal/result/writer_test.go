package result

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
)

// mockUploader 把 part 字节存内存，便于校验 sha256 / 行数 / 解压。
type mockUploader struct {
	parts map[string][]byte // key -> gzip data
	keys  []string
}

func newMockUploader() *mockUploader {
	return &mockUploader{parts: map[string][]byte{}}
}

func (m *mockUploader) Upload(_ context.Context, taskID, caller string, partSeq int, day string, data []byte) (string, error) {
	key := PartKey(day, caller, taskID, partSeq)
	cp := make([]byte, len(data))
	copy(cp, data)
	m.parts[key] = cp
	m.keys = append(m.keys, key)
	return key, nil
}

// Test5000Rows 写 5000 行 type=1 假数据，验证 sha256 + 行数 + 解压可 parse。
func Test5000Rows(t *testing.T) {
	up := newMockUploader()
	// maxRows=2000 → 5000 行应切成 3 片（2000/2000/1000）。
	w := NewWriter("ost_5k", "smart-summary", 2000, 1<<30, up)
	ctx := context.Background()

	if err := w.StartChannel(ctx, "chanA"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		ok, err := w.Write(ctx, InputMessage{
			MessageSeq: int64(i),
			From:       "u",
			ChannelID:  "chanA",
			Timestamp:  1700000000 + int64(i),
			Payload:    map[string]any{"type": 1, "text": map[string]any{"content": fmt.Sprintf("msg-%d", i)}},
		})
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("row %d unexpectedly skipped", i)
		}
	}
	parts, total, err := w.Finish(ctx)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if total != 5000 {
		t.Errorf("total rows = %d, want 5000", total)
	}
	if len(parts) != 3 {
		t.Fatalf("got %d parts, want 3 (2000/2000/1000 rollover)", len(parts))
	}
	wantCounts := []int64{2000, 2000, 1000}
	var gotRows int64
	for i, p := range parts {
		if p.MessageCount != wantCounts[i] {
			t.Errorf("part %d message_count = %d, want %d", i, p.MessageCount, wantCounts[i])
		}
		data := up.parts[p.S3Key]
		// sha256 校验。
		sum := fmt.Sprintf("%x", sha256.Sum256(data))
		if sum != p.SHA256 {
			t.Errorf("part %d sha256 mismatch: meta=%s actual=%s", i, p.SHA256, sum)
		}
		if p.SizeBytes != int64(len(data)) {
			t.Errorf("part %d size_bytes = %d, actual = %d", i, p.SizeBytes, len(data))
		}
		// 解压 + parse 每行。
		lines, err := gzipDecode(data)
		if err != nil {
			t.Fatalf("part %d gunzip: %v", i, err)
		}
		if int64(len(lines)) != p.MessageCount {
			t.Errorf("part %d decoded %d lines, meta says %d", i, len(lines), p.MessageCount)
		}
		for _, l := range lines {
			var row map[string]any
			if err := json.Unmarshal(l, &row); err != nil {
				t.Fatalf("part %d line not valid json: %v", i, err)
			}
			// snake_case 5 字段。
			for _, f := range []string{"message_seq", "from_uid", "channel_id", "timestamp", "payload"} {
				if _, ok := row[f]; !ok {
					t.Errorf("missing field %q in row: %s", f, l)
				}
			}
			// 不应出现 camelCase / 丢弃字段。
			for _, f := range []string{"messageSeq", "from", "channelId", "to", "channelType", "messageId", "payloadRaw"} {
				if _, ok := row[f]; ok {
					t.Errorf("unexpected field %q leaked into row: %s", f, l)
				}
			}
			if len(row) != 5 {
				t.Errorf("row should have exactly 5 fields, got %d: %s", len(row), l)
			}
		}
		gotRows += int64(len(lines))
	}
	if gotRows != 5000 {
		t.Errorf("decoded total %d rows, want 5000", gotRows)
	}
	if len(parts) > 0 && parts[0].ChannelIDs[0] != "chanA" {
		t.Errorf("part channel_ids = %v, want [chanA]", parts[0].ChannelIDs)
	}
}

// TestSkipMixedTypes 验证不支持 type 被跳过，且不占行数。
func TestSkipMixedTypes(t *testing.T) {
	up := newMockUploader()
	w := NewWriter("ost_mix", "c", 1000, 1<<30, up)
	ctx := context.Background()
	_ = w.StartChannel(ctx, "chanA")

	inputs := []map[string]any{
		{"type": 1, "text": map[string]any{"content": "keep1"}},
		{"type": 99, "text": map[string]any{"content": "drop"}}, // skipped
		{"type": 14, "richText": map[string]any{"searchText": "keep2"}},
		{"type": 7}, // skipped
	}
	written := 0
	for _, p := range inputs {
		ok, err := w.Write(ctx, InputMessage{ChannelID: "chanA", Payload: p})
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			written++
		}
	}
	if written != 2 {
		t.Errorf("written = %d, want 2 (two skipped)", written)
	}
	parts, total, err := w.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	lines, _ := gzipDecode(up.parts[parts[0].S3Key])
	if len(lines) != 2 {
		t.Errorf("decoded %d lines, want 2", len(lines))
	}
}

// TestChannelDoesNotCrossPart 验证不同 channel 不混入同一 part。
func TestChannelDoesNotCrossPart(t *testing.T) {
	up := newMockUploader()
	w := NewWriter("ost_ch", "c", 10000, 1<<30, up)
	ctx := context.Background()

	_ = w.StartChannel(ctx, "chanA")
	_, _ = w.Write(ctx, InputMessage{ChannelID: "chanA", Payload: map[string]any{"type": 1, "text": map[string]any{"content": "a"}}})
	_ = w.StartChannel(ctx, "chanB")
	_, _ = w.Write(ctx, InputMessage{ChannelID: "chanB", Payload: map[string]any{"type": 1, "text": map[string]any{"content": "b"}}})

	parts, _, err := w.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 {
		t.Fatalf("got %d parts, want 2 (one per channel)", len(parts))
	}
	if parts[0].ChannelIDs[0] != "chanA" || parts[1].ChannelIDs[0] != "chanB" {
		t.Errorf("parts channels = %v / %v", parts[0].ChannelIDs, parts[1].ChannelIDs)
	}
}

// TestByteRollover 验证按解压字节数触发 rollover。
func TestByteRollover(t *testing.T) {
	up := newMockUploader()
	// maxBytes 很小：每行约 >50 字节，maxBytes=100 → 大致每 1-2 行一片。
	w := NewWriter("ost_b", "c", 1000000, 100, up)
	ctx := context.Background()
	_ = w.StartChannel(ctx, "chanA")
	for i := 0; i < 10; i++ {
		_, err := w.Write(ctx, InputMessage{
			MessageSeq: int64(i), ChannelID: "chanA",
			Payload: map[string]any{"type": 1, "text": map[string]any{"content": "some longer content here padding"}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	parts, total, err := w.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if total != 10 {
		t.Errorf("total = %d, want 10", total)
	}
	if len(parts) < 2 {
		t.Errorf("expected multiple parts from byte rollover, got %d", len(parts))
	}
}
