FROM python:3.12.9-bookworm AS builder

WORKDIR /app

COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir --upgrade pip uv

COPY . .
RUN uv export --no-hashes --format requirements-txt > requirements.txt && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt


FROM python:3.12.9-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/home/appuser/.local/bin:$PATH"

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --create-home --shell /bin/bash appuser

COPY --from=builder /install /usr/local
COPY . .

RUN mkdir -p kwawaru_logs data && chown -R appuser:appuser /app

USER appuser

CMD ["python", "main.py"]
