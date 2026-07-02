#!/usr/bin/env bash
# scripts/e2e.sh — 本地端到端冒烟（design.md §7 / brief）。
#
# 流程：确保 DB/bucket/index 就绪 → 启服务 → POST 提交真实任务 → poll 直到 completed
#       → 下载 parts[0] → gunzip → 校验 NDJSON 首行 5 字段齐全。
# 退出码 0 = pass。
#
# 依赖（本地已具备）：docker（octo-* compose 已起）、curl、jq、python3、gunzip。
# MySQL / MinIO / OpenSearch 客户端不直接依赖：DB 用 docker exec 建、bucket 由服务
# 启动时 EnsureBucket 建、index 用 curl 查。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# --- 配置（与 .env.local.example 对齐）---
MYSQL_CONTAINER="octo-mysql-1"
MYSQL_ROOT_PW="ee52cd83ecfb1f4bbd79535ccea828e3"
DB_NAME="octo_message_export_api"
OS_ENDPOINT="http://127.0.0.1:9201"
OS_INDEX="wukongim-messages-read"
HTTP_ADDR=":18080"
BASE_URL="http://127.0.0.1:18080"
TOKEN="test-token-smart-summary"

SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

fail() { echo "E2E FAIL: $*" >&2; exit 1; }
info() { echo "[e2e] $*"; }

# --- 1. 确保 MySQL DB 存在 ---
info "ensuring MySQL database $DB_NAME"
docker exec "$MYSQL_CONTAINER" mysql -uroot -p"$MYSQL_ROOT_PW" \
  -e "CREATE DATABASE IF NOT EXISTS $DB_NAME CHARACTER SET utf8mb4;" 2>/dev/null \
  || fail "cannot create database (is $MYSQL_CONTAINER running?)"

# --- 2. bucket 由服务启动时 EnsureBucket 自动建（无需 mc/aws）---
info "bucket creation deferred to service startup (EnsureBucket)"

# --- 3. 检查 OS index 有 doc ---
info "checking OpenSearch index $OS_INDEX"
COUNT=$(curl -fsS "$OS_ENDPOINT/$OS_INDEX/_count" | jq -r '.count')
[[ "$COUNT" -ge 1 ]] || fail "index $OS_INDEX has no docs (count=$COUNT)"
info "index has $COUNT docs"

# 取一条 doc 的 channelId + timestamp，用于构造请求。
DOC=$(curl -fsS "$OS_ENDPOINT/$OS_INDEX/_search?size=100" -H 'Content-Type: application/json' \
  -d '{"query":{"match_all":{}},"_source":["channelId","timestamp"]}')
CHANNEL=$(echo "$DOC" | jq -r '.hits.hits[0]._source.channelId')
MIN_TS=$(echo "$DOC" | jq -r '[.hits.hits[]._source.timestamp] | min')
MAX_TS=$(echo "$DOC" | jq -r '[.hits.hits[]._source.timestamp] | max')
[[ -n "$CHANNEL" && "$CHANNEL" != "null" ]] || fail "could not extract channelId"
START_TS=$((MIN_TS - 60))
END_TS=$((MAX_TS + 60))
info "channel=$CHANNEL time_range=[$START_TS,$END_TS]"

# --- 4. 准备 .env.local ---
if [[ ! -f .env.local ]]; then
  info "cp .env.local.example .env.local"
  cp .env.local.example .env.local
fi

# --- 5. 编译 + 启动服务，等 /readyz 200 ---
info "building"
make build >/dev/null
info "starting server on $HTTP_ADDR"
# 不在 bash 里 source .env.local（DSN 含 tcp(...) 括号会破坏 source）：
# 二进制启动时自行 loadDotEnv(".env.local")。
./bin/octo-message-export-api >/tmp/octo-message-export-api-e2e.log 2>&1 &
SERVER_PID=$!

info "waiting for /readyz"
for i in $(seq 1 30); do
  if curl -fsS "$BASE_URL/readyz" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat /tmp/octo-message-export-api-e2e.log >&2
    fail "server exited during startup"
  fi
  sleep 1
  [[ "$i" == 30 ]] && { cat /tmp/octo-message-export-api-e2e.log >&2; fail "/readyz not ready after 30s"; }
done
info "server ready"

# --- 6. POST 提交任务 ---
info "POST /v1/messages/batch"
REQ=$(jq -nc --arg ch "$CHANNEL" --argjson s "$START_TS" --argjson e "$END_TS" \
  '{scope:{channels:[{channel_id:$ch}]},time_range:{start_ts:$s,end_ts:$e}}')
SUBMIT=$(curl -fsS -X POST "$BASE_URL/v1/messages/batch" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "$REQ")
TASK_ID=$(echo "$SUBMIT" | jq -r '.task_id')
[[ -n "$TASK_ID" && "$TASK_ID" != "null" ]] || fail "no task_id in response: $SUBMIT"
info "task_id=$TASK_ID"

# --- 7. poll 直到终态 ---
info "polling for completion"
STATUS=""
for i in $(seq 1 30); do
  GET=$(curl -fsS "$BASE_URL/v1/messages/batch/$TASK_ID" -H "Authorization: Bearer $TOKEN")
  STATUS=$(echo "$GET" | jq -r '.status')
  info "  status=$STATUS"
  case "$STATUS" in
    completed|partial) break ;;
    failed|cancelled) fail "task ended in $STATUS: $GET" ;;
  esac
  sleep 1
  [[ "$i" == 30 ]] && fail "task not complete after 30s (status=$STATUS)"
done

# --- 8. 下载 parts[0]，gunzip，校验首行 5 字段 ---
PART_URL=$(echo "$GET" | jq -r '.parts[0].url')
PART_SHA=$(echo "$GET" | jq -r '.parts[0].sha256')
[[ -n "$PART_URL" && "$PART_URL" != "null" ]] || fail "no parts[0].url: $GET"
info "downloading parts[0]"
TMP_GZ=$(mktemp /tmp/octo-e2e-part.XXXXXX.gz)
trap 'rm -f "$TMP_GZ"; cleanup' EXIT
curl -fsS "$PART_URL" -o "$TMP_GZ" || fail "download part failed"

# sha256 校验（gzip 内容）。
ACTUAL_SHA=$(python3 -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$TMP_GZ")
[[ "$ACTUAL_SHA" == "$PART_SHA" ]] || fail "sha256 mismatch: meta=$PART_SHA actual=$ACTUAL_SHA"
info "sha256 OK"

FIRST_LINE=$(gunzip -c "$TMP_GZ" | head -n1)
[[ -n "$FIRST_LINE" ]] || fail "decompressed part is empty"
info "first line: $FIRST_LINE"

# 校验 5 字段都存在。
echo "$FIRST_LINE" | jq -e \
  'has("message_seq") and has("from_uid") and has("channel_id") and has("timestamp") and has("payload")' \
  >/dev/null || fail "first line missing one of 5 required fields"

# 校验不含 camelCase 泄漏。
echo "$FIRST_LINE" | jq -e \
  '(has("messageSeq") or has("channelId") or has("from") or has("payloadRaw")) | not' \
  >/dev/null || fail "first line leaked camelCase fields"

info "5-field validation PASS"
echo "E2E PASS"
