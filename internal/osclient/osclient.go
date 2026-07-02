// Package osclient 封装 OpenSearch 调用：_count、PIT 开/关、search_after 翻页。
//
// 只暴露 batch 服务需要的最小操作面（dev-guide §C.1/§C.4）。
// 排序口径：messageId desc + subSeq desc（(messageId,subSeq) 全局唯一 tie-breaker，富文本父+虚拟子文档去重靠 must_not virtual）。
// 对外契约为降序（保留最新优先）；消费方如需升序自行排序。
package osclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go/v2"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
)

// Hit 是一条 OS 文档命中（保留 result 包 payload 解析所需的原始字段）。
type Hit struct {
	MessageSeq int64
	MessageID  int64
	SubSeq     int64
	From       string
	ChannelID  string
	Timestamp  int64
	// Payload / PayloadRaw 为 OS doc 原始 object，交给 result 包解析为纯文本。
	Payload    map[string]any
	PayloadRaw map[string]any
	// Sort 为本条命中的 sort 值，用作下一页 search_after 游标。
	Sort []any
}

// Client 是 OS 操作接口（便于上层 mock）。
type Client interface {
	Count(ctx context.Context, index, channelID string, startTS, endTS int64) (int64, error)
	OpenPIT(ctx context.Context, index string, keepAlive time.Duration) (string, error)
	ClosePIT(ctx context.Context, pitID string) error
	// Search 返回一页命中；searchAfter 为 nil 表示首页。
	Search(ctx context.Context, req SearchRequest) ([]Hit, error)
	Ping(ctx context.Context) error
}

// SearchRequest 单页翻页请求。
type SearchRequest struct {
	PITID       string
	KeepAlive   time.Duration
	ChannelID   string
	StartTS     int64
	EndTS       int64
	Size        int
	SearchAfter []any
}

// OSClient 基于 opensearch-go v2 的实现。
type OSClient struct {
	c *opensearch.Client
}

// New 构造 OS client。
func New(endpoints []string, username, password string, maxConns int, insecureSkipVerify bool) (*OSClient, error) {
	transport := &http.Transport{
		MaxIdleConns:        maxConns,
		MaxIdleConnsPerHost: maxConns,
	}
	if insecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	}
	cfg := opensearch.Config{
		Addresses: endpoints,
		Transport: transport,
	}
	if username != "" {
		cfg.Username = username
		cfg.Password = password
	}
	c, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("new opensearch client: %w", err)
	}
	return &OSClient{c: c}, nil
}

// Ping 调 OS info（readyz 用）。
func (o *OSClient) Ping(ctx context.Context) error {
	resp, err := o.c.Info(o.c.Info.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return fmt.Errorf("os ping: %s", resp.Status())
	}
	return nil
}

// filterQuery 构造 channel + time_range 过滤的 bool query。
// must_not virtual=true：只取父 doc，去掉富文本派生的虚拟子文档，避免一条富文本被重复统计。
func filterQuery(channelID string, startTS, endTS int64) map[string]any {
	return map[string]any{
		"bool": map[string]any{
			"filter": []any{
				map[string]any{"term": map[string]any{"channelId": channelID}},
				map[string]any{"range": map[string]any{"timestamp": map[string]any{"gte": startTS, "lte": endTS}}},
			},
			"must_not": []any{
				map[string]any{"term": map[string]any{"virtual": true}},
			},
		},
	}
}

// Count 走 _count API，仅用于 submit 期单 channel 计数（dev-guide §C.1）。
func (o *OSClient) Count(ctx context.Context, index, channelID string, startTS, endTS int64) (int64, error) {
	start := time.Now()
	body, _ := json.Marshal(map[string]any{"query": filterQuery(channelID, startTS, endTS)})
	resp, err := o.c.Count(
		o.c.Count.WithContext(ctx),
		o.c.Count.WithIndex(index),
		o.c.Count.WithBody(bytes.NewReader(body)),
	)
	metrics.OSSearchDuration.WithLabelValues("count").Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.OSSearchTotal.WithLabelValues("count", "err").Inc()
		return 0, fmt.Errorf("os count: %w", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		metrics.OSSearchTotal.WithLabelValues("count", "err").Inc()
		return 0, fmt.Errorf("os count status: %s", readError(resp.Body))
	}
	var out struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		metrics.OSSearchTotal.WithLabelValues("count", "err").Inc()
		return 0, fmt.Errorf("decode count: %w", err)
	}
	metrics.OSSearchTotal.WithLabelValues("count", "ok").Inc()
	return out.Count, nil
}

// OpenPIT 在 index 上开 PIT，返回 pit_id。
func (o *OSClient) OpenPIT(ctx context.Context, index string, keepAlive time.Duration) (string, error) {
	start := time.Now()
	path := fmt.Sprintf("/%s/_search/point_in_time?keep_alive=%s", index, durString(keepAlive))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, nil)
	if err != nil {
		return "", err
	}
	resp, err := o.c.Perform(req)
	metrics.OSSearchDuration.WithLabelValues("open_pit").Observe(time.Since(start).Seconds())
	if err != nil {
		return "", fmt.Errorf("open pit: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("open pit status %d: %s", resp.StatusCode, readError(resp.Body))
	}
	var out struct {
		PitID string `json:"pit_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode pit: %w", err)
	}
	if out.PitID == "" {
		return "", fmt.Errorf("open pit: empty pit_id")
	}
	return out.PitID, nil
}

// ClosePIT 释放 PIT。
func (o *OSClient) ClosePIT(ctx context.Context, pitID string) error {
	start := time.Now()
	body, _ := json.Marshal(map[string]any{"pit_id": pitID})
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "/_search/point_in_time", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := o.c.Perform(req)
	metrics.OSSearchDuration.WithLabelValues("close_pit").Observe(time.Since(start).Seconds())
	if err != nil {
		return fmt.Errorf("close pit: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// Search 翻一页。PIT 模式下不带 index（PIT 已固定索引）。
func (o *OSClient) Search(ctx context.Context, req SearchRequest) ([]Hit, error) {
	start := time.Now()
	q := map[string]any{
		"size":  req.Size,
		"query": filterQuery(req.ChannelID, req.StartTS, req.EndTS),
		"sort": []any{
			map[string]any{"messageId": "desc"},
			map[string]any{"subSeq": "desc"},
		},
		"pit": map[string]any{"id": req.PITID, "keep_alive": durString(req.KeepAlive)},
	}
	if len(req.SearchAfter) > 0 {
		q["search_after"] = req.SearchAfter
	}
	body, _ := json.Marshal(q)

	resp, err := o.c.Search(
		o.c.Search.WithContext(ctx),
		o.c.Search.WithBody(bytes.NewReader(body)),
	)
	metrics.OSSearchDuration.WithLabelValues("search").Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.OSSearchTotal.WithLabelValues("search", "err").Inc()
		return nil, fmt.Errorf("os search: %w", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		metrics.OSSearchTotal.WithLabelValues("search", "err").Inc()
		return nil, fmt.Errorf("os search status: %s", readError(resp.Body))
	}
	metrics.OSSearchTotal.WithLabelValues("search", "ok").Inc()
	return parseHits(resp.Body)
}

func parseHits(r io.Reader) ([]Hit, error) {
	var out struct {
		Hits struct {
			Hits []struct {
				Source map[string]any `json:"_source"`
				Sort   []any          `json:"sort"`
			} `json:"hits"`
		} `json:"hits"`
	}
	// UseNumber：messageId 为 19 位 int64，超出 float64 精度；用 json.Number
	// 保证 sort 值原样回填 search_after，翻页无重无漏。
	dec := json.NewDecoder(r)
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode search: %w", err)
	}
	hits := make([]Hit, 0, len(out.Hits.Hits))
	for _, h := range out.Hits.Hits {
		src := h.Source
		hit := Hit{
			MessageSeq: toInt64(src["messageSeq"]),
			MessageID:  toInt64(src["messageId"]),
			SubSeq:     toInt64(src["subSeq"]),
			From:       toStr(src["from"]),
			ChannelID:  toStr(src["channelId"]),
			Timestamp:  toInt64(src["timestamp"]),
			Sort:       h.Sort,
		}
		if p, ok := src["payload"].(map[string]any); ok {
			hit.Payload = p
		}
		if pr, ok := src["payloadRaw"].(map[string]any); ok {
			hit.PayloadRaw = pr
		}
		hits = append(hits, hit)
	}
	return hits, nil
}

func readError(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 2048))
	return strings.TrimSpace(string(b))
}

// durString 把 duration 转成 OS keep_alive 字符串（如 "2m" / "30s"）。
func durString(d time.Duration) string {
	if d <= 0 {
		d = time.Minute
	}
	if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", int(d/time.Minute))
	}
	return fmt.Sprintf("%ds", int(d/time.Second))
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	default:
		return 0
	}
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
