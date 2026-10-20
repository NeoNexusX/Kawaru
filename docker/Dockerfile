FROM python:3.12.9-bookworm AS builder

WORKDIR /app

COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir --upgrade pip uv

COPY . .
# --group dev：pytest 等测试工具已迁至 dev 依赖组，集成测试在容器内跑 pytest，故需显式导出该组
RUN uv export --group dev --no-hashes --format requirements-txt > requirements.txt && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt


FROM python:3.12.9-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/home/appuser/.local/bin:$PATH"

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -u 1000 --create-home --shell /bin/bash appuser

COPY --from=builder /install /usr/local
COPY . .

RUN mkdir -p kawaru_logs && chown -R appuser:appuser /app

USER appuser

CMD ["python", "main.py"]
