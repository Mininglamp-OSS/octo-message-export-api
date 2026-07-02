// Command octo-message-export-api 是服务入口（dev-guide §A，design.md §2）。
//
// 启动流程：
//  1. 加载 .env.local / .env（若存在）→ config.Load()
//  2. 构造 Store / OSClient / S3 Uploader / Gate / Executor / Submitter / Canceller / Handler
//  3. apply schema migration（IF NOT EXISTS，幂等）
//  4. 启 HTTP server + Executor.Run
//  5. SIGINT/SIGTERM：/readyz 转 503 → 等 DRAIN_GRACE_MS → 关 http → cancel executor → exit
package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mininglamp-OSS/octo-message-export-api/internal/cancel"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/config"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/executor"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/handler"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/metrics"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/osclient"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/result"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/store"
	storeschema "github.com/Mininglamp-OSS/octo-message-export-api/internal/store/migrations"
	"github.com/Mininglamp-OSS/octo-message-export-api/internal/submit"
)

func main() {
	loadDotEnv(".env.local")
	loadDotEnv(".env")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	if err := run(cfg); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func run(cfg *config.Config) error {
	ctx := context.Background()

	// --- 依赖构造 ---
	st, err := store.NewMySQLStore(cfg.MySQL.DSN, 50)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := applyMigrations(cfg.MySQL.DSN); err != nil {
		return err
	}

	osc, err := osclient.New(cfg.OS.Endpoints, cfg.OS.Username, cfg.OS.Password, cfg.OS.MaxConns, cfg.OS.InsecureSkipVerify)
	if err != nil {
		return err
	}

	up, err := result.NewS3Uploader(ctx, result.S3Config{
		Endpoint:     cfg.S3.Endpoint,
		Bucket:       cfg.S3.Bucket,
		Region:       cfg.S3.Region,
		AccessKey:    cfg.S3.AccessKey,
		SecretKey:    cfg.S3.SecretKey,
		UsePathStyle: cfg.S3.UsePathStyle,
		PresignTTL:   cfg.S3.PresignTTL,
		KeyPrefix:    cfg.S3.KeyPrefix,
	})
	if err != nil {
		return err
	}
	// 启动时 best-effort 建 bucket（本地 MinIO / 首次部署）；失败不阻塞启动。
	if err := up.EnsureBucket(ctx); err != nil {
		log.Printf("warn: ensure bucket: %v", err)
	}

	auditor, err := metrics.NewAuditor(cfg.GC.AuditLocalPath)
	if err != nil {
		log.Printf("warn: audit file: %v (degrade to stdout only)", err)
		auditor, _ = metrics.NewAuditor("")
	}
	defer auditor.Close()

	gate := executor.NewGate(cfg.Protect.GlobalInflightMax)

	exec := executor.New(executor.Config{
		Index:             cfg.OS.IndexName,
		PITKeepAlive:      cfg.OS.PITKeepAlive,
		Workers:           cfg.Exec.Workers,
		QueueBuffer:       cfg.Exec.QueueBuffer,
		PartMaxRows:       cfg.Exec.PartMaxRows,
		PartMaxBytes:      cfg.Exec.PartDecompressedMax,
		MaxTaskHits:       cfg.Protect.MaxTaskHits,
		PerChannelHardCap: cfg.Protect.PerChannelHardCap,
	}, osc, st, up, gate, auditor)

	submitter := submit.New(cfg.Protect, cfg.OS.IndexName, osc, st, gate, exec)
	canceller := cancel.New(st, exec, up, auditor)

	var ready atomic.Bool
	ready.Store(true)

	h := handler.New(handler.Deps{
		Auth:      cfg.Auth,
		Submitter: submitter,
		Canceller: canceller,
		Store:     st,
		Presign:   up,
		Audit:     auditor,
		DBPing:    st,
		OSPing:    osc,
		Ready:     ready.Load,
	})

	// --- 启动 executor + http server ---
	execCtx, execCancel := context.WithCancel(ctx)
	defer execCancel()
	go exec.Run(execCtx)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// --- 优雅关闭 ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-sigCh:
		log.Printf("received %s, draining...", sig)
	}

	// 1. /readyz 转 503，让 LB 摘流。
	ready.Store(false)
	// 2. 等 drain 宽限期。
	time.Sleep(time.Duration(cfg.DrainGraceMS) * time.Millisecond)
	// 3. 关 http server（等 in-flight 请求结束）。
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("warn: http shutdown: %v", err)
	}
	// 4. cancel executor ctx → worker 取消 running task → 关 PIT / abort multipart → 标终态。
	execCancel()
	// 给 worker 一点收尾时间。
	time.Sleep(2 * time.Second)
	log.Printf("shutdown complete")
	return nil
}

// schemaSQL 是 batch_task / batch_task_part 的建表语句,通过 storeschema 包的 go:embed
// 嵌入二进制,这样 scratch 容器里也能跑 migration。
var schemaSQL = storeschema.SchemaSQL

// applyMigrations 把 schema.sql apply 一遍（IF NOT EXISTS，幂等）。
//
// DSN 形如 "user:pw@tcp(host:port)/dbname?...";如果 dbname 不存在(本地首次跑、
// docker compose 里 mysql 容器只内置默认库),先用不带 dbname 的 DSN 连一把,
// `CREATE DATABASE IF NOT EXISTS dbname`,再切回正常 DSN apply schema。
func applyMigrations(dsn string) error {
	if err := ensureDatabase(dsn); err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()
	for _, stmt := range strings.Split(schemaSQL, ";") {
		if strings.TrimSpace(stripSQLComments(stmt)) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	log.Printf("migrations applied")
	return nil
}

// ensureDatabase 在 dsn 指向的 MySQL 实例上 CREATE DATABASE IF NOT EXISTS。
// 解析 DSN("user:pw@tcp(host:port)/dbname?params")找出 dbname 后,
// 用空 dbname 重连一把执行 CREATE。
func ensureDatabase(dsn string) error {
	slash := strings.LastIndex(dsn, "/")
	if slash < 0 {
		return errors.New("applyMigrations: DSN missing '/dbname'")
	}
	after := dsn[slash+1:]
	dbname := after
	if q := strings.Index(after, "?"); q >= 0 {
		dbname = after[:q]
	}
	if dbname == "" {
		return errors.New("applyMigrations: DSN dbname empty")
	}
	rootDSN := dsn[:slash+1] // 末尾保留 '/',连默认 schema
	if q := strings.Index(after, "?"); q >= 0 {
		rootDSN += after[q:]
	}
	db, err := sql.Open("mysql", rootDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFn()
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbname+"` CHARACTER SET utf8mb4")
	return err
}

// stripSQLComments 去掉以 "--" 开头的整行注释（粗略，够 schema.sql 用）。
func stripSQLComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// loadDotEnv 读取 .env 风格文件，把未在环境中设置的 key 注入 os.Env（已设置的不覆盖）。
// 极简实现：KEY=VALUE 一行，支持 # 注释；不解析引号转义。无文件时静默返回。
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.Index(line, "=")
		if i < 0 {
			continue
		}
		key := strings.TrimSpace(line[:i])
		val := strings.TrimSpace(line[i+1:])
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
}
