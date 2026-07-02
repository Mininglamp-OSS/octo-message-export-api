// Package metrics 暴露 Prometheus 指标 + 本地审计日志（jsonl）。
//
// design.md §6 / §4.3：v1 只暴露 /metrics endpoint（运维手工建面板），
// 审计落本地 jsonl（每行同时 fmt 到 stdout，便于 K8s 日志收集）。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 指标集合（design.md §1.1 #13 最小集合）。
// 注册到自建 registry，避免依赖默认全局 registry（便于测试隔离）。
var (
	Registry = prometheus.NewRegistry()
	factory  = promauto.With(Registry)

	// SubmitTotal submit 路径计数。
	SubmitTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "batch_submit_total",
		Help: "submit 路径计数",
	}, []string{"caller", "result"})

	// SubmitDuration submit RTT。
	SubmitDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "batch_submit_duration_seconds",
		Help:    "submit RTT",
		Buckets: prometheus.DefBuckets,
	}, []string{"caller"})

	// ExecuteDuration executor 端到端耗时。
	ExecuteDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "batch_execute_duration_seconds",
		Help:    "executor 端到端耗时",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 900},
	}, []string{"caller", "size_bucket"})

	// CountCheckDuration submit 期 _count 调用耗时。
	CountCheckDuration = factory.NewHistogram(prometheus.HistogramOpts{
		Name:    "batch_count_check_duration_seconds",
		Help:    "submit 期 OS _count 调用耗时",
		Buckets: prometheus.DefBuckets,
	})

	// OSSearchDuration OS 调用耗时（op=count/search/open_pit/close_pit）。
	OSSearchDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "os_search_duration_seconds",
		Help:    "OS 调用耗时",
		Buckets: prometheus.DefBuckets,
	}, []string{"op"})

	// OSSearchTotal OS 调用计数。
	OSSearchTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "os_search_total",
		Help: "OS 调用计数",
	}, []string{"op", "result"})

	// S3UploadDuration S3 写入耗时（op=single/multipart）。
	S3UploadDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3_upload_duration_seconds",
		Help:    "S3 写入耗时",
		Buckets: prometheus.DefBuckets,
	}, []string{"op"})

	// S3UploadBytes 累计写入字节数。
	S3UploadBytes = factory.NewCounter(prometheus.CounterOpts{
		Name: "s3_upload_bytes_total",
		Help: "S3 累计写入字节数",
	})

	// PartSizeBytes 单 part gzip 后字节数（观察指标）。
	PartSizeBytes = factory.NewHistogram(prometheus.HistogramOpts{
		Name:    "part_size_bytes",
		Help:    "单 part gzip 后字节数（观察值）",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 8),
	})

	// ProtectionRejected 因系统保护参数拒绝的次数。
	ProtectionRejected = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "system_protection_rejected_total",
		Help: "因系统保护参数拒绝的次数",
	}, []string{"reason"})

	// QueueDepth executor 队列长度。
	QueueDepth = factory.NewGauge(prometheus.GaugeOpts{
		Name: "executor_queue_depth",
		Help: "executor 队列长度",
	})

	// Inflight 正在执行 task 数。
	Inflight = factory.NewGauge(prometheus.GaugeOpts{
		Name: "executor_inflight",
		Help: "正在执行 task 数",
	})

	// TaskStatusTotal 终态计数。
	TaskStatusTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "task_status_total",
		Help: "task 终态计数",
	}, []string{"status"})

	// PayloadParseSkip payload 解析失败 / 不支持 type 跳过计数。
	PayloadParseSkip = factory.NewCounter(prometheus.CounterOpts{
		Name: "payload_parse_skip_total",
		Help: "payload 解析失败或不支持 type 跳过的消息数",
	})
)

// SizeBucket 把命中规模归一到 30k/100k/300k 三档 label（design.md §6）。
func SizeBucket(count int64) string {
	switch {
	case count <= 30000:
		return "30k"
	case count <= 100000:
		return "100k"
	default:
		return "300k"
	}
}
