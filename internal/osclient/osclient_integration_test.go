package osclient

import (
	"context"
	"os"
	"testing"
	"time"
)

// 集成测试：跑本地 OS（127.0.0.1:9201, index wukongim-messages-read）。
// 设 TEST_OS_ENDPOINTS 启用；未设则 skip。
func newTestClient(t *testing.T) (*OSClient, string) {
	t.Helper()
	eps := os.Getenv("TEST_OS_ENDPOINTS")
	if eps == "" {
		t.Skip("no TEST_OS_ENDPOINTS set; skipping OS integration test")
	}
	index := os.Getenv("TEST_OS_INDEX")
	if index == "" {
		index = "wukongim-messages-read"
	}
	c, err := New([]string{eps}, os.Getenv("TEST_OS_USERNAME"), os.Getenv("TEST_OS_PASSWORD"), 10, true)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, index
}

const (
	testChannel = "test0002@test0001"
	testStart   = 1700000000
	testEnd     = 1800000000
)

func TestOSCount(t *testing.T) {
	c, index := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := c.Count(ctx, index, testChannel, testStart, testEnd)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n <= 0 {
		t.Errorf("Count = %d, want > 0 (local OS should have docs in %s)", n, testChannel)
	}
	t.Logf("count = %d", n)
}

func TestOSPITSearchPaging(t *testing.T) {
	c, index := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pit, err := c.OpenPIT(ctx, index, 2*time.Minute)
	if err != nil {
		t.Fatalf("OpenPIT: %v", err)
	}
	defer func() { _ = c.ClosePIT(ctx, pit) }()

	// 翻页（每页 3 条）验证：严格降序 + 无重 + 与 Count 总数一致。
	var (
		searchAfter []any
		all         []Hit
		lastID      int64 = 1<<63 - 1
	)
	seen := map[int64]bool{}
	for {
		hits, err := c.Search(ctx, SearchRequest{
			PITID: pit, KeepAlive: 2 * time.Minute, ChannelID: testChannel,
			StartTS: testStart, EndTS: testEnd, Size: 3, SearchAfter: searchAfter,
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(hits) == 0 {
			break
		}
		for _, h := range hits {
			if h.MessageID > lastID {
				t.Errorf("out of order: id %d after %d", h.MessageID, lastID)
			}
			lastID = h.MessageID
			if seen[h.MessageID] {
				t.Errorf("duplicate messageId %d", h.MessageID)
			}
			seen[h.MessageID] = true
		}
		all = append(all, hits...)
		searchAfter = hits[len(hits)-1].Sort
	}

	total, _ := c.Count(ctx, index, testChannel, testStart, testEnd)
	if int64(len(all)) != total {
		t.Errorf("paged %d hits, _count says %d", len(all), total)
	}
	if len(all) > 0 && all[0].ChannelID != testChannel {
		t.Errorf("channelId = %q, want %q", all[0].ChannelID, testChannel)
	}
}
