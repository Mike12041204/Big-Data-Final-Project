FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 1. Install uv
RUN apt-get update && apt-get install -y curl ca-certificates && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app
COPY . /app

# 2. Sync dependencies using your pyproject.toml instead of requirements.txt
WORKDIR /app/project
RUN uv sync

# Creates a non-root user for security
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# 3. Use uv run to execute main.py within the managed environment
CMD ["uv", "run", "main.py"]