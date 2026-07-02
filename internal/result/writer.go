// Package result 负责把 OS 命中转成对外 NDJSON part：
// 字段映射（camelCase→snake_case）+ payload 提取 + gzip 分片 + sha256 + S3 上传。
package result

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
)

// InputMessage 是写入 part 的单条消息（由 executor 从 osclient.Hit 映射而来）。
type InputMessage struct {
	MessageSeq int64
	From       string
	ChannelID  string
	Timestamp  int64
	Payload    map[string]any
	PayloadRaw map[string]any
}

// outLine 对外 NDJSON 行（api-spec §5.1：snake_case 5 字段，payload 为已解析字符串）。
type outLine struct {
	MessageSeq int64  `json:"message_seq"`
	FromUID    string `json:"from_uid"`
	ChannelID  string `json:"channel_id"`
	Timestamp  int64  `json:"timestamp"`
	Payload    string `json:"payload"`
}

// Uploader 抽象 part 落地（S3 / mock）。
type Uploader interface {
	// Upload 上传一个 part 的 gzip 字节，返回 S3 key。
	Upload(ctx context.Context, taskID, caller string, partSeq int, day string, data []byte) (key string, err error)
}

// Writer 是单个 task 的结果写入器。
// 用法：每个 channel 调一次 StartChannel（强制新 part，避免跨 channel 混 part），
// 然后逐条 Write；最后 Finish 关闭最后一片并返回全部 PartMeta。
type Writer struct {
	taskID  string
	caller  string
	day     string
	maxRows int
	maxByte int64
	up      Uploader

	parts   []store.Part
	partSeq int

	// 当前 part 状态。
	cur        *partBuffer
	curChannel string
}

type partBuffer struct {
	buf     bytes.Buffer
	gz      *gzip.Writer
	rows    int
	rawSize int64 // 解压后字节数
}

// NewWriter 创建一个 task 级 Writer。
func NewWriter(taskID, caller string, maxRows int, maxBytes int64, up Uploader) *Writer {
	day := time.Now().UTC().Format("2006-01-02")
	return &Writer{
		taskID:  taskID,
		caller:  caller,
		day:     day,
		maxRows: maxRows,
		maxByte: maxBytes,
		up:      up,
	}
}

func newPartBuffer() *partBuffer {
	p := &partBuffer{}
	p.gz = gzip.NewWriter(&p.buf)
	return p
}

// StartChannel 开始一个新 channel：先 flush 上一个 part（若有），保证 part 不混 channel。
func (w *Writer) StartChannel(ctx context.Context, channelID string) error {
	if w.cur != nil {
		if err := w.flush(ctx); err != nil {
			return err
		}
	}
	w.curChannel = channelID
	return nil
}

// Write 写一条消息。payload 不支持的 type / 解析失败的消息直接跳过（不写、不报错）。
// 返回是否实际写入（true=写入，false=跳过）。
func (w *Writer) Write(ctx context.Context, m InputMessage) (bool, error) {
	text, ok := extractPayload(m.Payload, m.PayloadRaw)
	if !ok {
		return false, nil // 跳过：不支持的 type 或解析失败
	}

	line, err := json.Marshal(outLine{
		MessageSeq: m.MessageSeq,
		FromUID:    m.From,
		ChannelID:  m.ChannelID,
		Timestamp:  m.Timestamp,
		Payload:    text,
	})
	if err != nil {
		metrics.PayloadParseSkip.Inc()
		return false, nil
	}
	line = append(line, '\n')

	if w.cur == nil {
		w.cur = newPartBuffer()
	}
	if _, err := w.cur.gz.Write(line); err != nil {
		return false, fmt.Errorf("gzip write: %w", err)
	}
	w.cur.rows++
	w.cur.rawSize += int64(len(line))

	// 双计数 rollover：行数 或 解压后字节 任一达阈 → flush + 开新片。
	if w.cur.rows >= w.maxRows || w.cur.rawSize >= w.maxByte {
		if err := w.flush(ctx); err != nil {
			return false, err
		}
	}
	return true, nil
}

// flush 关闭当前 part、上传、记录 PartMeta，并重置 cur。
func (w *Writer) flush(ctx context.Context) error {
	if w.cur == nil {
		return nil
	}
	if w.cur.rows == 0 {
		// 空 part（StartChannel 后无命中）：丢弃，不占 part_seq。
		w.cur = nil
		return nil
	}
	if err := w.cur.gz.Close(); err != nil {
		return fmt.Errorf("gzip close: %w", err)
	}
	data := w.cur.buf.Bytes()
	// sha256 over gzip 内容（api-spec §5.3）。
	sum := fmt.Sprintf("%x", sha256.Sum256(data))

	key, err := w.up.Upload(ctx, w.taskID, w.caller, w.partSeq, w.day, data)
	if err != nil {
		return fmt.Errorf("upload part %d: %w", w.partSeq, err)
	}
	metrics.PartSizeBytes.Observe(float64(len(data)))

	w.parts = append(w.parts, store.Part{
		TaskID:       w.taskID,
		PartSeq:      w.partSeq,
		S3Key:        key,
		SizeBytes:    int64(len(data)),
		MessageCount: int64(w.cur.rows),
		SHA256:       sum,
		ChannelIDs:   []string{w.curChannel},
	})
	w.partSeq++
	w.cur = nil
	return nil
}

// Finish 关闭最后一个 part，返回全部 PartMeta 和总行数。
func (w *Writer) Finish(ctx context.Context) ([]store.Part, int64, error) {
	if err := w.flush(ctx); err != nil {
		return nil, 0, err
	}
	var total int64
	for _, p := range w.parts {
		total += p.MessageCount
	}
	return w.parts, total, nil
}

// gzipDecode 工具：解压 + 返回行（测试用）。
func gzipDecode(data []byte) ([][]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	raw, err := io.ReadAll(gr)
	if err != nil {
		return nil, err
	}
	var lines [][]byte
	for _, l := range bytes.Split(raw, []byte("\n")) {
		if len(l) > 0 {
			lines = append(lines, l)
		}
	}
	return lines, nil
}
