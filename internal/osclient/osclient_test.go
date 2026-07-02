package osclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDurString(t *testing.T) {
	cases := map[time.Duration]string{
		2 * time.Minute:  "2m",
		5 * time.Minute:  "5m",
		30 * time.Second: "30s",
		90 * time.Second: "90s",
		0:                "1m",
	}
	for d, want := range cases {
		if got := durString(d); got != want {
			t.Errorf("durString(%v) = %q, want %q", d, got, want)
		}
	}
}

// TestParseHitsPreservesBigIntSort 验证 19 位 messageId 的 sort 值不丢精度。
func TestParseHitsPreservesBigIntSort(t *testing.T) {
	raw := `{"hits":{"hits":[
	  {"_source":{"messageSeq":58,"messageId":2064262096714895360,"from":"u1","channelId":"c1","timestamp":1780993410,"payload":{"type":1,"text":{"content":"hi"}}},
	   "sort":[58,2064262096714895360]}
	]}}`
	hits, err := parseHits(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("parseHits: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("got %d hits, want 1", len(hits))
	}
	h := hits[0]
	if h.MessageSeq != 58 || h.MessageID != 2064262096714895360 {
		t.Errorf("seq/id = %d/%d", h.MessageSeq, h.MessageID)
	}
	if h.From != "u1" || h.ChannelID != "c1" || h.Timestamp != 1780993410 {
		t.Errorf("unexpected fields: %+v", h)
	}
	// sort 值必须能原样 re-marshal 回精确的大整数（search_after 游标依赖此）。
	b, _ := json.Marshal(h.Sort)
	if !strings.Contains(string(b), "2064262096714895360") {
		t.Errorf("sort lost precision: %s", b)
	}
	// payload object 透传给 result 包。
	if h.Payload["type"] == nil {
		t.Error("payload not parsed")
	}
}

// TestSearchSortDesc 验证 Search 构造的 query body 里 sort 为
// [{messageId: desc}, {subSeq: desc}]，护栏：有人改回 asc 时本测试必挂。
func TestSearchSortDesc(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"hits":{"hits":[]}}`)
	}))
	defer srv.Close()

	cli, err := New([]string{srv.URL}, "", "", 1, false)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := cli.Search(context.Background(), SearchRequest{
		PITID:     "pit1",
		KeepAlive: time.Minute,
		ChannelID: "c1",
		StartTS:   1,
		EndTS:     2,
		Size:      10,
	}); err != nil {
		t.Fatalf("Search: %v", err)
	}

	var req struct {
		Sort []map[string]string `json:"sort"`
	}
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("unmarshal body %q: %v", gotBody, err)
	}
	if len(req.Sort) != 2 {
		t.Fatalf("sort len = %d, want 2; body=%s", len(req.Sort), gotBody)
	}
	if req.Sort[0]["messageId"] != "desc" {
		t.Errorf("sort[0] = %v, want messageId:desc", req.Sort[0])
	}
	if req.Sort[1]["subSeq"] != "desc" {
		t.Errorf("sort[1] = %v, want subSeq:desc", req.Sort[1])
	}
}
