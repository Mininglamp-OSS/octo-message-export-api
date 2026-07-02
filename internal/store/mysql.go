package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ErrNotFound 任务不存在（或非本 caller 创建，由调用方决定是否区分）。
var ErrNotFound = errors.New("task not found")

// Store 是 task / parts 元数据的持久化接口（dev-guide §A4）。
type Store interface {
	Create(ctx context.Context, t *Task) error
	Get(ctx context.Context, taskID string) (*Task, error)
	UpdateStatus(ctx context.Context, taskID string, status Status) error
	// Finalize 写终态：status + actual_count + warnings + error。
	Finalize(ctx context.Context, taskID string, status Status, actualCount int64, warnings []Warning, errCode, errMsg string) error
	AppendPart(ctx context.Context, p *Part) error
	GetParts(ctx context.Context, taskID string) ([]Part, error)
	// CancelCAS 原子地把 status 从 queued/running 切到 cancelled。
	// 返回 (true, status) 表示切换成功；(false, currentStatus) 表示已终态。
	CancelCAS(ctx context.Context, taskID string) (bool, Status, error)
	// ListStuck 列出 running 且 updated_at 早于 cutoff 的僵尸 task（GC 用）。
	ListStuck(ctx context.Context, cutoff time.Time) ([]string, error)
	// ListExpired 列出 expires_at 早于 now 的 task（GC 清理用）。
	ListExpired(ctx context.Context, now time.Time) ([]Task, error)
	Delete(ctx context.Context, taskID string) error
	Ping(ctx context.Context) error
	Close() error
}

// MySQLStore 是 Store 的 MySQL 实现。
type MySQLStore struct {
	db  *sql.DB
	now func() time.Time
}

// NewMySQLStore 打开连接池并 ping 验证。
func NewMySQLStore(dsn string, maxConns int) (*MySQLStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	if maxConns > 0 {
		db.SetMaxOpenConns(maxConns)
		db.SetMaxIdleConns(maxConns / 2)
	}
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return &MySQLStore{db: db, now: time.Now}, nil
}

func (s *MySQLStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s *MySQLStore) Close() error                   { return s.db.Close() }

func (s *MySQLStore) Create(ctx context.Context, t *Task) error {
	scopeJSON, err := json.Marshal(t.Scope)
	if err != nil {
		return fmt.Errorf("marshal scope: %w", err)
	}
	now := s.now()
	expires := now.Add(time.Duration(7) * 24 * time.Hour)
	if t.ExpiresAt > 0 {
		expires = time.Unix(t.ExpiresAt, 0)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO batch_task
		  (task_id, caller_service, status, actual_count, scope_json,
		   time_range_start, time_range_end, request_id, created_at, updated_at, expires_at)
		VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)`,
		t.TaskID, t.CallerService, int8(t.Status), string(scopeJSON),
		t.TimeRange.StartTS, t.TimeRange.EndTS, t.RequestID, now, now, expires)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	t.CreatedAt = now.Unix()
	t.UpdatedAt = now.Unix()
	t.ExpiresAt = expires.Unix()
	return nil
}

func (s *MySQLStore) Get(ctx context.Context, taskID string) (*Task, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT task_id, caller_service, status, actual_count, scope_json,
		       time_range_start, time_range_end, request_id, warnings_json,
		       error_code, error_message, created_at, updated_at, expires_at
		FROM batch_task WHERE task_id = ?`, taskID)
	return scanTask(row)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTask(row rowScanner) (*Task, error) {
	var (
		t            Task
		st           int8
		scopeJSON    string
		warningsJSON sql.NullString
		errMsg       sql.NullString
		createdAt    time.Time
		updatedAt    time.Time
		expiresAt    time.Time
	)
	err := row.Scan(&t.TaskID, &t.CallerService, &st, &t.ActualCount, &scopeJSON,
		&t.TimeRange.StartTS, &t.TimeRange.EndTS, &t.RequestID, &warningsJSON,
		&t.ErrorCode, &errMsg, &createdAt, &updatedAt, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan task: %w", err)
	}
	t.Status = Status(st)
	if err := json.Unmarshal([]byte(scopeJSON), &t.Scope); err != nil {
		return nil, fmt.Errorf("unmarshal scope: %w", err)
	}
	if warningsJSON.Valid && warningsJSON.String != "" {
		_ = json.Unmarshal([]byte(warningsJSON.String), &t.Warnings)
	}
	if errMsg.Valid {
		t.ErrorMessage = errMsg.String
	}
	t.CreatedAt = createdAt.Unix()
	t.UpdatedAt = updatedAt.Unix()
	t.ExpiresAt = expiresAt.Unix()
	return &t, nil
}

func (s *MySQLStore) UpdateStatus(ctx context.Context, taskID string, status Status) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE batch_task SET status = ?, updated_at = ? WHERE task_id = ?`,
		int8(status), s.now(), taskID)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

func (s *MySQLStore) Finalize(ctx context.Context, taskID string, status Status, actualCount int64, warnings []Warning, errCode, errMsg string) error {
	var warningsJSON any
	if len(warnings) > 0 {
		b, err := json.Marshal(warnings)
		if err != nil {
			return fmt.Errorf("marshal warnings: %w", err)
		}
		warningsJSON = string(b)
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE batch_task
		SET status = ?, actual_count = ?, warnings_json = ?, error_code = ?, error_message = ?, updated_at = ?
		WHERE task_id = ?`,
		int8(status), actualCount, warningsJSON, errCode, nullableStr(errMsg), s.now(), taskID)
	if err != nil {
		return fmt.Errorf("finalize task: %w", err)
	}
	return nil
}

func (s *MySQLStore) AppendPart(ctx context.Context, p *Part) error {
	channelsJSON, err := json.Marshal(p.ChannelIDs)
	if err != nil {
		return fmt.Errorf("marshal channel_ids: %w", err)
	}
	// ON DUPLICATE KEY UPDATE：重跑同 part_seq 时覆盖（dev-guide §J.4 幂等约束）。
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO batch_task_part
		  (task_id, part_seq, s3_key, size_bytes, message_count, sha256, channel_ids_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		  s3_key=VALUES(s3_key), size_bytes=VALUES(size_bytes), message_count=VALUES(message_count),
		  sha256=VALUES(sha256), channel_ids_json=VALUES(channel_ids_json), created_at=VALUES(created_at)`,
		p.TaskID, p.PartSeq, p.S3Key, p.SizeBytes, p.MessageCount, p.SHA256, string(channelsJSON), s.now())
	if err != nil {
		return fmt.Errorf("append part: %w", err)
	}
	return nil
}

func (s *MySQLStore) GetParts(ctx context.Context, taskID string) ([]Part, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id, part_seq, s3_key, size_bytes, message_count, sha256, channel_ids_json, created_at
		FROM batch_task_part WHERE task_id = ? ORDER BY part_seq ASC`, taskID)
	if err != nil {
		return nil, fmt.Errorf("query parts: %w", err)
	}
	defer rows.Close()

	var parts []Part
	for rows.Next() {
		var (
			p            Part
			channelsJSON string
			createdAt    time.Time
		)
		if err := rows.Scan(&p.TaskID, &p.PartSeq, &p.S3Key, &p.SizeBytes,
			&p.MessageCount, &p.SHA256, &channelsJSON, &createdAt); err != nil {
			return nil, fmt.Errorf("scan part: %w", err)
		}
		_ = json.Unmarshal([]byte(channelsJSON), &p.ChannelIDs)
		p.CreatedAt = createdAt.Unix()
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

func (s *MySQLStore) CancelCAS(ctx context.Context, taskID string) (bool, Status, error) {
	// 只有非终态（queued/running）才能切 cancelled。
	res, err := s.db.ExecContext(ctx, `
		UPDATE batch_task SET status = ?, updated_at = ?
		WHERE task_id = ? AND status IN (?, ?)`,
		int8(StatusCancelled), s.now(), taskID, int8(StatusQueued), int8(StatusRunning))
	if err != nil {
		return false, 0, fmt.Errorf("cancel cas: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected > 0 {
		return true, StatusCancelled, nil
	}
	// 未切到：要么不存在，要么已终态。回查当前状态。
	t, err := s.Get(ctx, taskID)
	if err != nil {
		return false, 0, err
	}
	return false, t.Status, nil
}

func (s *MySQLStore) ListStuck(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id FROM batch_task WHERE status = ? AND updated_at < ?`,
		int8(StatusRunning), cutoff)
	if err != nil {
		return nil, fmt.Errorf("list stuck: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *MySQLStore) ListExpired(ctx context.Context, now time.Time) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id, caller_service, status, actual_count, scope_json,
		       time_range_start, time_range_end, request_id, warnings_json,
		       error_code, error_message, created_at, updated_at, expires_at
		FROM batch_task WHERE expires_at < ?`, now)
	if err != nil {
		return nil, fmt.Errorf("list expired: %w", err)
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, *t)
	}
	return tasks, rows.Err()
}

func (s *MySQLStore) Delete(ctx context.Context, taskID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `DELETE FROM batch_task_part WHERE task_id = ?`, taskID); err != nil {
		return fmt.Errorf("delete parts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM batch_task WHERE task_id = ?`, taskID); err != nil {
		return fmt.Errorf("delete task: %w", err)
	}
	return tx.Commit()
}

func nullableStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
