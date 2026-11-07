# zer0share 生产调度镜像：只含代码和依赖，数据 / 配置 / 日志全部从宿主机挂载
FROM ghcr.io/astral-sh/uv:0.11.1 AS uv

FROM python:3.11-slim

COPY --from=uv /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

WORKDIR /app

# 先装依赖（利用缓存层），再装项目本身
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY zer0share ./zer0share
COPY main.py README.md LICENSE ./
RUN uv sync --frozen --no-dev \
    && mkdir -p /app/data /app/db /app/logs /app/reports /app/config \
    && chown -R 1000:1000 /app/data /app/db /app/logs /app/reports

ENV PATH="/app/.venv/bin:$PATH" \
    TZ=Asia/Shanghai \
    HOME=/tmp \
    PYTHONUNBUFFERED=1

USER 1000:1000

CMD ["python", "main.py", "scheduler", "start"]
