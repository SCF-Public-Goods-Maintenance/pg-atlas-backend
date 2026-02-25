# syntax=docker/dockerfile:1

# ── Build stage ──────────────────────────────────────────────────────────────
# Uses the official uv image to resolve and install dependencies into a venv.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

WORKDIR /app

# Copy only the dependency manifest first so Docker layer caching skips the
# install step when only application source code changes.
COPY pyproject.toml uv.lock* ./

# Install production dependencies into /app/.venv.
# --frozen ensures the lock file is used as-is; --no-dev omits dev tools.
RUN uv sync --frozen --no-dev --no-install-project

# Now copy the rest of the source and install the project itself.
# README.md is required by hatchling (declared as readme in pyproject.toml).
COPY README.md ./
COPY pg_atlas/ ./pg_atlas/
RUN uv sync --frozen --no-dev

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.14-slim-bookworm AS runtime

WORKDIR /app

# Copy the pre-built virtual environment from the builder stage.
COPY --from=builder /app/.venv /app/.venv

# Copy application source.
COPY --from=builder /app/pg_atlas ./pg_atlas/

# Activate the venv by prepending it to PATH.
ENV PATH="/app/.venv/bin:$PATH"

# Expose the API port.
EXPOSE 8000

CMD ["uvicorn", "pg_atlas.main:app", "--host", "0.0.0.0", "--port", "8000"]
