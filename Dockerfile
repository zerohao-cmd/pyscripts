# syntax=docker/dockerfile:1.7

FROM oven/bun:1.4.0-alpine AS ui-build
WORKDIR /build/ui
COPY ui/package.json ui/bun.lock ./
RUN bun install --frozen-lockfile
COPY ui/ ./
RUN bun run build

FROM python:3.12-slim-bookworm AS python-build
COPY --from=ghcr.io/astral-sh/uv:0.11.21 /uv /uvx /bin/
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_NO_DEV=1
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY pyscripts_hook/pyproject.toml pyscripts_hook/README.md ./pyscripts_hook/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace
COPY src/ ./src/
COPY pyscripts_hook/sc_hook/ ./pyscripts_hook/sc_hook/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM python:3.12-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 pyscripts \
    && useradd --uid 10001 --create-home --gid pyscripts --shell /usr/sbin/nologin pyscripts \
    && mkdir -p /var/lib/pyscripts/contracts \
    && chown -R pyscripts:pyscripts /var/lib/pyscripts

WORKDIR /app
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYSCRIPTS_UI_DIST_PATH=/app/ui/dist \
    PYSCRIPTS_CONTRACT_ARTIFACT_ROOT=/var/lib/pyscripts/contracts
COPY --from=python-build /app/.venv/ /app/.venv/
COPY --from=ui-build /build/ui/dist/ /app/ui/dist/

USER pyscripts
EXPOSE 8000 50051
CMD ["uvicorn", "pyscripts.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
