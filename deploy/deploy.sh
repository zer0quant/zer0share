#!/usr/bin/env bash
# 构建并发布 zer0share 调度容器。
# 用法：deploy/deploy.sh            # 用当前 git HEAD 构建，tag = 日期-短 hash
#       deploy/deploy.sh <tag>      # 回滚到已有镜像 tag（不重新构建）
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/docker-compose.yml)

if [[ $# -ge 1 ]]; then
    TAG="$1"
    docker image inspect "zer0share:${TAG}" >/dev/null
else
    if [[ -n "$(git status --porcelain -- zer0share main.py pyproject.toml uv.lock)" ]]; then
        echo "zer0share/ main.py pyproject.toml uv.lock 有未提交改动，先提交再部署" >&2
        exit 1
    fi
    TAG="$(date +%Y%m%d)-$(git rev-parse --short HEAD)"
    docker build -f deploy/Dockerfile -t "zer0share:${TAG}" -t zer0share:latest .
fi

TAG="$TAG" "${COMPOSE[@]}" up -d
echo "已部署 zer0share:${TAG}"
"${COMPOSE[@]}" ps
