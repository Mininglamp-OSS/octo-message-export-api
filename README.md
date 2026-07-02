# octo-message-export-api

Async batch messages query API over an OpenSearch-backed message index.

Callers submit a task (channel list + time range), poll for status, and download
results as gzipped NDJSON parts via presigned S3 URLs. All submissions return
`HTTP 202` — there is no inline synchronous path.

## API

Three endpoints, all async:

- `POST /v1/messages/batch` — submit a task, returns `task_id`
- `GET  /v1/messages/batch/{task_id}` — poll status + parts list
- `DELETE /v1/messages/batch/{task_id}` — idempotent cancel

See [`docs/api-spec.md`](docs/api-spec.md) for full request/response schemas,
error codes, and system protection parameters.

## Auth

Service-to-service bearer token. Configure `AUTH_S2S_TOKENS` (comma-separated)
or `AUTH_CALLER_TOKENS_FILE` (mapping `caller_name=token,...`) at deploy time.
Caller identity is used only for audit and metrics labels; there is no
per-caller quota in this version (single-caller assumption).

> The example configuration ships with a caller labeled `smart-summary` — this
> is a label string only, not a required name. Rename freely to match your
> deployment.

## Requirements

- Go 1.25.3
- OpenSearch 2.x (index writer is out of scope for this repo; the upstream
  indexer that populates the index defines the `channel_id` write convention
  the client must follow — see [`docs/api-spec.md`](docs/api-spec.md) §6.1)
- MySQL 5.7+ (task and parts metadata)
- S3-compatible object storage (results parts)

## Build / Run

```bash
make build              # bin/octo-message-export-api
make test               # go test ./...
cp .env.example .env    # then edit
./start.sh
```

## Deploy

Container image is published to Docker Hub via
[`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml).
For Kubernetes deployment, adapt the manifests to your cluster (single-replica
by design in v1; multi-replica requires shared inflight counter, planned for
v0.2).

## Constraints (v1)

- Single-replica deployment (no HA / crash-recovery)
- No per-caller quota (single-caller model)
- Task retention: 7 days
- Global inflight cap: 50 concurrent tasks
- Per-task hit cap: 300,000 messages

## License

Apache License 2.0. See [LICENSE](LICENSE).
