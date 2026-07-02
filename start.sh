#!/usr/bin/env bash
# 启动脚本：加载 .env（若存在）后启动服务。
set -euo pipefail

cd "$(dirname "$0")"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

make build
exec ./bin/octo-message-export-api
