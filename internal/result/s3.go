package result

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
)

// S3Config S3 上传所需配置（由 config.S3Config 映射而来）。
type S3Config struct {
	Endpoint     string
	Bucket       string
	Region       string
	AccessKey    string
	SecretKey    string
	UsePathStyle bool
	PresignTTL   time.Duration
	KeyPrefix    string // 环境隔离前缀（test/prod）；多环境共用 bucket 时用。留空不加前缀。
}

// S3Uploader 基于 aws-sdk-go-v2 + s3manager 的 Uploader 实现。
// 大于分片阈值时 s3manager 自动走 multipart。
type S3Uploader struct {
	cfg      S3Config
	client   *s3.Client
	uploader *manager.Uploader
	presign  *s3.PresignClient
}

// NewS3Uploader 构造 S3 client。
func NewS3Uploader(ctx context.Context, cfg S3Config) (*S3Uploader, error) {
	var optFns []func(*awscfg.LoadOptions) error
	optFns = append(optFns, awscfg.WithRegion(cfg.Region))
	if cfg.AccessKey != "" {
		optFns = append(optFns, awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}
	awsConf, err := awscfg.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsConf, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	return &S3Uploader{
		cfg:      cfg,
		client:   client,
		uploader: manager.NewUploader(client),
		presign:  s3.NewPresignClient(client),
	}, nil
}

// objectRoot 是本服务在共用 bucket 内的根命名空间（用项目名，区分同 bucket 其它业务数据）。
const objectRoot = "octo-message-export-api"

// PartKey 生成 part 的 S3 key 主体（不含环境前缀）：
// octo-message-export-api/{yyyy-mm-dd}/{caller}/{task_id}/part-{seq:03d}.ndjson.gz
func PartKey(day, caller, taskID string, seq int) string {
	return fmt.Sprintf("%s/%s/%s/%s/part-%03d.ndjson.gz", objectRoot, day, caller, taskID, seq)
}

// withPrefix 给 key/prefix 拼上环境前缀（KeyPrefix 为空则原样返回）。
// 多环境共用 bucket 时按 {prefix}/octo-message-export-api/... 物理隔离。
func (u *S3Uploader) withPrefix(s string) string {
	p := strings.Trim(u.cfg.KeyPrefix, "/")
	if p == "" {
		return s
	}
	return p + "/" + s
}

// TaskPrefix 返回某 task 的完整对象前缀（含环境前缀），供 GC / cancel 清理用：
// {env}/octo-message-export-api/{yyyy-mm-dd}/{caller}/{task_id}/
func (u *S3Uploader) TaskPrefix(day, caller, taskID string) string {
	return u.withPrefix(fmt.Sprintf("%s/%s/%s/%s/", objectRoot, day, caller, taskID))
}

// Upload 上传 part 字节，返回 S3 key（含环境前缀）。
func (u *S3Uploader) Upload(ctx context.Context, taskID, caller string, partSeq int, day string, data []byte) (string, error) {
	key := u.withPrefix(PartKey(day, caller, taskID, partSeq))
	start := time.Now()
	op := "single"
	if int64(len(data)) > manager.DefaultUploadPartSize {
		op = "multipart"
	}
	_, err := u.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(u.cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/gzip"),
	})
	metrics.S3UploadDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	if err != nil {
		return "", fmt.Errorf("s3 upload: %w", err)
	}
	metrics.S3UploadBytes.Add(float64(len(data)))
	return key, nil
}

// Presign 为某个 key 生成 presigned GET URL，返回 url 与过期秒级 ts。
func (u *S3Uploader) Presign(ctx context.Context, key string) (string, int64, error) {
	req, err := u.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(u.cfg.Bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(u.cfg.PresignTTL))
	if err != nil {
		return "", 0, fmt.Errorf("presign: %w", err)
	}
	expires := time.Now().Add(u.cfg.PresignTTL).Unix()
	return req.URL, expires, nil
}

// EnsureBucket 创建 bucket（若不存在）。本地 MinIO / 首次启动用。
func (u *S3Uploader) EnsureBucket(ctx context.Context) error {
	_, err := u.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(u.cfg.Bucket)})
	if err == nil {
		return nil
	}
	_, err = u.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(u.cfg.Bucket)})
	if err != nil {
		return fmt.Errorf("create bucket %q: %w", u.cfg.Bucket, err)
	}
	return nil
}

// DeletePrefix 删除某 task 的所有 part 对象（GC / cancel 清理用）。
func (u *S3Uploader) DeletePrefix(ctx context.Context, prefix string) error {
	out, err := u.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(u.cfg.Bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return fmt.Errorf("list objects: %w", err)
	}
	for _, obj := range out.Contents {
		_, err := u.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(u.cfg.Bucket),
			Key:    obj.Key,
		})
		if err != nil {
			return fmt.Errorf("delete object %s: %w", aws.ToString(obj.Key), err)
		}
	}
	return nil
}

// Ping 检查 bucket 可达（readyz 用）。
func (u *S3Uploader) Ping(ctx context.Context) error {
	_, err := u.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(u.cfg.Bucket)})
	return err
}
