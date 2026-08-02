# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0

ARG PYTHON_VERSION=3.14.6

FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /source
COPY . .
RUN python -m venv /opt/samsarix \
    && /opt/samsarix/bin/python -m pip install ".[asymmetric-auth]"

FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="Samsarix Chat Engine" \
      org.opencontainers.image.description="Self-hosted, access-controlled room chat" \
      org.opencontainers.image.source="https://github.com/Deathcharge/samsarix-chat-engine" \
      org.opencontainers.image.licenses="MPL-2.0"

ENV PATH="/opt/samsarix/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 samsarix \
    && useradd --no-log-init --system --uid 10001 --gid 10001 --home-dir /nonexistent samsarix \
    && install --directory --owner=10001 --group=10001 --mode=0750 /data

COPY --from=builder /opt/samsarix /opt/samsarix

USER 10001:10001
WORKDIR /data
EXPOSE 8000
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "from urllib.request import urlopen; urlopen('http://127.0.0.1:8000/readyz', timeout=2).read()"]

ENTRYPOINT ["samsarix-chat"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8000", "--database", "/data/samsarix-chat.db"]
