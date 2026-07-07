// Package config 加载并持有服务运行所需的全部配置。
//
// v1 配置全部来自环境变量（main 启动时用 godotenv 预加载 .env）。
// 不做配置中心 / 热刷新（改配置重启服务）。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 是服务的完整配置快照，启动时构造一次，之后只读。
type Config struct {
	HTTPAddr     string
	DrainGraceMS int

	OS      OSConfig
	MySQL   MySQLConfig
	S3      S3Config
	Auth    AuthConfig
	Exec    ExecConfig
	Protect ProtectConfig
	GC      GCConfig
}

// OSConfig OpenSearch 连接参数。
type OSConfig struct {
	Endpoints          []string
	IndexName          string
	Username           string
	Password           string
	InsecureSkipVerify bool
	MaxConns           int
	PITKeepAlive       time.Duration
}

// MySQLConfig MySQL 连接参数。
type MySQLConfig struct {
	DSN string
}

// S3Config 对象存储（S3 / MinIO）参数。
type S3Config struct {
	Endpoint     string // 留空走 AWS 默认；本地 MinIO 需填
	Bucket       string
	Region       string
	AccessKey    string
	SecretKey    string
	UsePathStyle bool
	PresignTTL   time.Duration
	KeyPrefix    string // 环境隔离前缀（如 test/prod）；多环境共用 bucket，必填（见 validate）。
}

// AuthConfig 鉴权参数。
type AuthConfig struct {
	Enabled bool
	// CallerTokens: token -> caller 名 的反查表。
	// 来源 AUTH_S2S_TOKENS，格式 "name=token,name=token"；
	// 若某项不含 '='，则 caller 名 fallback 到 "default"。
	CallerTokens map[string]string
}

// ExecConfig Executor / Result Writer 参数。
type ExecConfig struct {
	Workers             int
	QueueBuffer         int
	PartDecompressedMax int64
	PartMaxRows         int
}

// ProtectConfig 系统保护参数。
type ProtectConfig struct {
	GlobalInflightMax int
	SubmitWait        time.Duration
	MaxTaskHits       int64
	PerChannelHardCap int
}

// GCConfig GC / 审计参数。
type GCConfig struct {
	TaskRetentionDays int
	AuditLocalPath    string
}

// Load 从环境变量构造 Config，并对必填项做基本校验。
func Load() (*Config, error) {
	c := &Config{
		HTTPAddr:     getEnv("HTTP_ADDR", ":8080"),
		DrainGraceMS: getEnvInt("DRAIN_GRACE_MS", 5000),
		OS: OSConfig{
			Endpoints:          splitCSV(getEnv("OS_ENDPOINTS", "")),
			IndexName:          getEnv("OS_INDEX_NAME", "messages"),
			Username:           getEnv("OS_USERNAME", ""),
			Password:           getEnv("OS_PASSWORD", ""),
			InsecureSkipVerify: getEnvBool("OS_INSECURE_SKIP_VERIFY", false),
			MaxConns:           getEnvInt("OS_MAX_CONNS", 200),
			PITKeepAlive:       getEnvDuration("PIT_KEEP_ALIVE", 5*time.Minute),
		},
		MySQL: MySQLConfig{
			DSN: getEnv("MYSQL_DSN", ""),
		},
		S3: S3Config{
			Endpoint:     getEnv("S3_ENDPOINT", ""),
			Bucket:       getEnv("S3_BUCKET", ""),
			Region:       getEnv("S3_REGION", ""),
			AccessKey:    getEnv("S3_ACCESS_KEY", ""),
			SecretKey:    getEnv("S3_SECRET_KEY", ""),
			UsePathStyle: getEnvBool("S3_USE_PATH_STYLE", false),
			PresignTTL:   time.Duration(getEnvInt("S3_PRESIGN_TTL_SEC", 3600)) * time.Second,
			KeyPrefix:    getEnv("S3_KEY_PREFIX", ""),
		},
		Auth: AuthConfig{
			Enabled:      getEnvBool("AUTH_ENABLED", true),
			CallerTokens: parseCallerTokens(getEnv("AUTH_S2S_TOKENS", "")),
		},
		Exec: ExecConfig{
			Workers:             getEnvInt("EXECUTOR_WORKERS", 16),
			QueueBuffer:         getEnvInt("EXECUTOR_QUEUE_BUFFER", 50),
			PartDecompressedMax: int64(getEnvInt("PART_DECOMPRESSED_MAX_BYTES", 524288000)),
			PartMaxRows:         getEnvInt("PART_MAX_ROWS", 30000),
		},
		Protect: ProtectConfig{
			GlobalInflightMax: getEnvInt("PROTECT_GLOBAL_INFLIGHT_MAX", 50),
			SubmitWait:        time.Duration(getEnvInt("PROTECT_SUBMIT_WAIT_SEC", 10)) * time.Second,
			MaxTaskHits:       int64(getEnvInt("PROTECT_MAX_TASK_HITS", 300000)),
			PerChannelHardCap: getEnvInt("PER_CHANNEL_HARD_CAP", 100000),
		},
		GC: GCConfig{
			TaskRetentionDays: getEnvInt("TASK_RETENTION_DAYS", 7),
			AuditLocalPath:    getEnv("AUDIT_LOCAL_PATH", "/var/log/octo-message-export-api/audit.jsonl"),
		},
	}

	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) validate() error {
	if len(c.OS.Endpoints) == 0 {
		return fmt.Errorf("OS_ENDPOINTS is required")
	}
	if c.MySQL.DSN == "" {
		return fmt.Errorf("MYSQL_DSN is required")
	}
	if c.S3.Bucket == "" {
		return fmt.Errorf("S3_BUCKET is required")
	}
	if c.S3.KeyPrefix == "" {
		return fmt.Errorf("S3_KEY_PREFIX is required (环境隔离前缀，如 test/prod；多环境共用 bucket)")
	}
	if c.Auth.Enabled && len(c.Auth.CallerTokens) == 0 {
		return fmt.Errorf("AUTH_S2S_TOKENS is required when AUTH_ENABLED=true")
	}
	if c.Exec.Workers <= 0 {
		return fmt.Errorf("EXECUTOR_WORKERS must be > 0")
	}
	return nil
}

// parseCallerTokens 解析 "name=token,name=token" 格式为 token->caller 反查表。
// 若某项不含 '='，caller 名 fallback 到 "default"。
func parseCallerTokens(raw string) map[string]string {
	out := make(map[string]string)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if i := strings.Index(item, "="); i >= 0 {
			name := strings.TrimSpace(item[:i])
			token := strings.TrimSpace(item[i+1:])
			if name == "" {
				name = "default"
			}
			if token != "" {
				out[token] = name
			}
		} else {
			out[item] = "default"
		}
	}
	return out
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			return b
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(raw string) []string {
	var out []string
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
