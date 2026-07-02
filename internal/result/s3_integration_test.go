package result

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// 集成测试：跑本地 MinIO（设 TEST_S3_ENDPOINT 启用）。
func TestS3UploadPresignRoundtrip(t *testing.T) {
	ep := os.Getenv("TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("no TEST_S3_ENDPOINT set; skipping S3 integration test")
	}
	ctx := context.Background()
	cfg := S3Config{
		Endpoint:     ep,
		Bucket:       envOr("TEST_S3_BUCKET", "octo-message-export-api-results-local"),
		Region:       envOr("TEST_S3_REGION", "us-east-1"),
		AccessKey:    os.Getenv("TEST_S3_ACCESS_KEY"),
		SecretKey:    os.Getenv("TEST_S3_SECRET_KEY"),
		UsePathStyle: true,
		PresignTTL:   10 * time.Minute,
	}
	up, err := NewS3Uploader(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3Uploader: %v", err)
	}
	if err := up.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}

	// 写一个 part（用 Writer 产生真实 gzip）。
	w := NewWriter("ost_s3test", "smart-summary", 1000, 1<<30, up)
	_ = w.StartChannel(ctx, "chanA")
	for i := 0; i < 5; i++ {
		_, err := w.Write(ctx, InputMessage{
			MessageSeq: int64(i), ChannelID: "chanA",
			Payload: map[string]any{"type": 1, "text": map[string]any{"content": "hello s3"}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	parts, _, err := w.Finish(ctx)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("got %d parts, want 1", len(parts))
	}
	t.Cleanup(func() { _ = up.DeletePrefix(ctx, "octo-message-export-api/") })

	// presign + 下载 + 解压。
	url, expires, err := up.Presign(ctx, parts[0].S3Key)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}
	if expires <= time.Now().Unix() {
		t.Errorf("expires_at not in future: %d", expires)
	}
	resp, err := http.Get(url) //nolint:gosec
	if err != nil {
		t.Fatalf("GET presigned: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET status %d", resp.StatusCode)
	}
	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	raw, _ := io.ReadAll(gr)
	if n := strings.Count(string(raw), "hello s3"); n != 5 {
		t.Errorf("decompressed contains %d 'hello s3', want 5", n)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
