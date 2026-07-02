# octo-message-export-api

An async batch messages export API over an OpenSearch-backed message index.
Callers submit a task (channel list + time range), poll for status, and
download results as gzipped NDJSON parts via presigned S3-compatible URLs.
All submissions return **HTTP 202** — there is no inline synchronous path.

## Where it sits in the pipeline

```
[ caller (e.g. smart-summary) ]
   │  POST /v1/messages/batch   (async task submission)
   ▼
[ octo-message-export-api  (THIS repo) ]
   │  PIT + search_after paging over OpenSearch
   ▼
[ OpenSearch  (index populated by an upstream indexer) ]
   │  results stream as gzipped NDJSON parts (500MB / 30000 rows rollover)
   ▼
[ S3-compatible object storage ]
   ▲
   │  GET presigned URL per part
[ caller downloads parts ]
```

This repo is the **read / export side** of a message-search stack. It does
**not** write to the OpenSearch index — that is owned by an upstream indexer
(e.g. [`octo-search-indexer`](https://github.com/Mininglamp-OSS/octo-search-indexer)),
which defines the `channel_id` write convention that callers must follow when
naming channels in a submission (see [`docs/api-spec.md`](docs/api-spec.md)
§6.1). Task and part metadata live in MySQL; result payloads live in
S3-compatible object storage; the service itself is stateless above those two.

## Design discipline

- **All async, HTTP 202.** Submission returns a `task_id` immediately; there
  is no inline path. Callers poll `GET /v1/messages/batch/{task_id}` for
  status and presigned part URLs.
- **Service-to-service trust.** A long-lived bearer token authenticates each
  caller; identity is used only for audit and metrics labels. There is no
  per-caller quota (single-caller model in v1).
- **PIT + search_after paging.** Long-running tasks use OpenSearch Point-in-Time
  with `search_after` cursors (5 min keep-alive by default) so an in-flight
  task sees a stable snapshot even under concurrent writes.
- **NDJSON gzip rollover.** The result writer streams NDJSON lines through
  gzip and rolls a new part when either decompressed bytes ≥ 500 MB or row
  count ≥ 30 000 (dual-count). Each part is uploaded via S3 multipart and
  handed to the caller as a presigned URL.
- **Idempotent cancel.** `DELETE /v1/messages/batch/{task_id}` is
  idempotent — repeat calls return the same terminal state.
- **Single-replica (v1).** In-process inflight counter caps global
  concurrency at 50 tasks. Multi-replica coordination (shared inflight
  counter, task hand-off across restarts) is planned for v0.2 and requires
  an external coordinator (Redis).
- **Audit-first.** Every submit / status / cancel emits a Prometheus metric
  and a JSONL audit record locally (`AUDIT_LOCAL_PATH`).
- **Server protection over caller quota.** A submitted task hitting
  `PROTECT_MAX_TASK_HITS` (300 000 rows by default) is rejected up front
  with `413`; the global inflight cap returns `503` after
  `PROTECT_SUBMIT_WAIT_SEC` of queuing.

## Layout

| Path | Purpose |
| --- | --- |
| `cmd/octo-message-export-api/` | HTTP server entrypoint: env config load, wiring, graceful shutdown |
| `internal/handler/` | HTTP routes (`POST` / `GET` / `DELETE /v1/messages/batch[/{id}]`), `/healthz` + `/readyz` + `/metrics` |
| `internal/auth/` | s2s bearer-token middleware; caller-identity injection into request context |
| `internal/submit/` | Submission validation, per-channel `_count` gate, global inflight admission |
| `internal/executor/` | Worker pool, per-task lifecycle, cancel registry (`gate.go` is the inflight limiter) |
| `internal/osclient/` | OpenSearch client: PIT open/close, `_count`, `search_after` paging |
| `internal/result/` | NDJSON+gzip writer with dual-count rollover, S3 multipart upload, presigned-URL issuance |
| `internal/store/` | MySQL CRUD for `task` / `parts` + state machine; SQL schema in `store/migrations/` |
| `internal/cancel/` | Cancel-signal registry (goroutine-safe map of task_id → cancel func) |
| `internal/metrics/` | Prometheus collectors + local JSONL audit writer |
| `internal/config/` | Env-based config loader with required-field validation |
| `docs/api-spec.md` | Client-facing API contract (v0.1) — request/response schemas, error codes, field semantics |
| `scripts/e2e.sh` | Local end-to-end smoke test (`POST` → poll → download → row-count assert) |

## Configuration (env)

Source of truth: [`internal/config/config.go`](internal/config/config.go).
All configuration is loaded from environment variables at startup; the
service does not support hot reload (change env → restart).

### HTTP

| Variable | Default | Notes |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | listen address |
| `DRAIN_GRACE_MS` | `5000` | on SIGTERM, `/readyz` returns not-ready for this many ms before shutdown so the LB can drain |

### OpenSearch

| Variable | Default | Notes |
| --- | --- | --- |
| `OS_ENDPOINTS` | — | **required**, CSV of node URLs (e.g. `https://os-1:9200,https://os-2:9200`) |
| `OS_INDEX_NAME` | `messages` | target index / alias to read from |
| `OS_USERNAME` / `OS_PASSWORD` | — | optional HTTP basic auth |
| `OS_INSECURE_SKIP_VERIFY` | `false` | set `true` to skip TLS cert verification (dev / self-signed only) |
| `OS_MAX_CONNS` | `200` | HTTP transport `MaxConnsPerHost` |
| `PIT_KEEP_ALIVE` | `5m` | PIT keep-alive for long tasks (Go duration) |

### MySQL

| Variable | Default | Notes |
| --- | --- | --- |
| `MYSQL_DSN` | — | **required**, Go MySQL DSN for task and parts metadata |

### S3-compatible object storage

| Variable | Default | Notes |
| --- | --- | --- |
| `S3_BUCKET` | — | **required**, target bucket for result parts |
| `S3_KEY_PREFIX` | — | **required**, environment prefix (e.g. `test` / `prod`) so multiple deployments can share one bucket |
| `S3_ENDPOINT` | — | leave empty for AWS default; set for MinIO / self-hosted S3 gateways |
| `S3_REGION` | — | AWS-style region (e.g. `us-east-1`) |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | — | static credentials |
| `S3_USE_PATH_STYLE` | `false` | set `true` for path-style addressing (MinIO / most internal gateways) |
| `S3_PRESIGN_TTL_SEC` | `3600` | presigned URL lifetime |

### Auth

| Variable | Default | Notes |
| --- | --- | --- |
| `AUTH_ENABLED` | `true` | master switch |
| `AUTH_S2S_TOKENS` | — | **required when enabled**. Format `name=token,name=token,...`; a bare `token` without `=` is accepted and mapped to caller name `default` |

### Executor / Result Writer

| Variable | Default | Notes |
| --- | --- | --- |
| `EXECUTOR_WORKERS` | `16` | worker pool size |
| `EXECUTOR_QUEUE_BUFFER` | `50` | in-memory task queue depth |
| `PART_DECOMPRESSED_MAX_BYTES` | `524288000` | 500 MB rollover trigger (decompressed) |
| `PART_MAX_ROWS` | `30000` | row-count rollover trigger |

### Server protection

| Variable | Default | Notes |
| --- | --- | --- |
| `PROTECT_GLOBAL_INFLIGHT_MAX` | `50` | global concurrent-task cap |
| `PROTECT_SUBMIT_WAIT_SEC` | `10` | max queuing time on submission before returning `503` |
| `PROTECT_MAX_TASK_HITS` | `300000` | per-task hit ceiling; larger tasks are rejected with `413` at submit |
| `PER_CHANNEL_HARD_CAP` | `100000` | per-channel hit ceiling within a task (server-side defense in depth) |

### GC / Audit

| Variable | Default | Notes |
| --- | --- | --- |
| `TASK_RETENTION_DAYS` | `7` | task and parts retained for this long before GC |
| `AUDIT_LOCAL_PATH` | `/var/log/octo-message-export-api/audit.jsonl` | local JSONL audit file (rotation is the operator's responsibility) |

> The example configuration ships with a caller labeled `smart-summary` —
> this is a label string only, not a required name. Any string works.

## Build

```sh
go build ./...
go test ./...
```

A local `.env.local.example` is provided as a starting point for
docker-compose-based development (OpenSearch + MySQL + MinIO on `127.0.0.1`);
copy it to `.env.local` and fill in credentials.

## Deploy

The container image is published to Docker Hub by
[`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml).
Kubernetes manifests are not bundled in this repository — external adopters
should adapt to their own cluster. Note the v1 single-replica constraint:
running multiple replicas without an external inflight counter will
double-count concurrency and can violate `PROTECT_GLOBAL_INFLIGHT_MAX`.

## License

[Apache License 2.0](./LICENSE).
