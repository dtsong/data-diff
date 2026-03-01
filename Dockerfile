# ---- builder stage ----
FROM python:3.12-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY data_diff/ data_diff/

RUN uv sync --frozen --no-dev

# ---- runtime stage ----
FROM python:3.12-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system appuser && useradd --system --gid appuser appuser

COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:${PATH}"

WORKDIR /app
USER appuser

ENTRYPOINT ["data-diff"]
