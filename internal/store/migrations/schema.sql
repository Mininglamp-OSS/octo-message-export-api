-- octo-message-export-api v1 schema (api-spec.md §4.1 / api-spec §D.1, §D.2)
-- 在 MySQL 库 octo_message_export_api 上执行。

CREATE TABLE IF NOT EXISTS batch_task (
  task_id          VARCHAR(40)  NOT NULL,
  caller_service   VARCHAR(64)  NOT NULL,
  status           TINYINT      NOT NULL,                 -- 0=queued 1=running 2=completed 3=partial 4=failed 5=cancelled
  actual_count     BIGINT       NOT NULL DEFAULT 0,
  scope_json       JSON         NOT NULL,
  time_range_start BIGINT       NOT NULL,
  time_range_end   BIGINT       NOT NULL,
  request_id       VARCHAR(40)  NOT NULL DEFAULT '',
  warnings_json    JSON         NULL,
  error_code       VARCHAR(64)  NOT NULL DEFAULT '',
  error_message    TEXT         NULL,
  created_at       DATETIME     NOT NULL,
  updated_at       DATETIME     NOT NULL,
  expires_at       DATETIME     NOT NULL,
  PRIMARY KEY (task_id),
  KEY idx_caller_status (caller_service, status),
  KEY idx_updated_at (updated_at),
  KEY idx_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS batch_task_part (
  id                BIGINT       NOT NULL AUTO_INCREMENT,
  task_id           VARCHAR(40)  NOT NULL,
  part_seq          INT          NOT NULL,
  s3_key            VARCHAR(256) NOT NULL,
  size_bytes        BIGINT       NOT NULL DEFAULT 0,
  message_count     BIGINT       NOT NULL DEFAULT 0,
  sha256            CHAR(64)     NOT NULL DEFAULT '',
  channel_ids_json  JSON         NOT NULL,
  created_at        DATETIME     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_task_part (task_id, part_seq),
  KEY idx_task_id (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
