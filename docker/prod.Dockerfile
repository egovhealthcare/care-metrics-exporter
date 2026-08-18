# syntax=docker/dockerfile:1
#
# Production image for the CARE metrics exporter.
#
# The layout mirrors ohcnetwork/care's docker/prod.Dockerfile: a shared `base`
# stage holding the environment, a `builder` stage that resolves dependencies
# into a virtualenv with pipenv, and a slim `runtime` stage that copies only
# that virtualenv and the application source.

FROM python:3.13-slim-bookworm AS base

ARG APP_HOME=/app

WORKDIR $APP_HOME

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIPENV_VENV_IN_PROJECT=1
ENV PATH=$APP_HOME/.venv/bin:$PATH
ENV HOME=$APP_HOME


# ---
FROM base AS builder

# The exporter has no compiled dependencies, so no build toolchain is needed.
RUN pip install --no-cache-dir pipenv==2025.1.1

RUN python -m venv $APP_HOME/.venv
COPY Pipfile Pipfile.lock $APP_HOME/
RUN pipenv install --deploy --categories "packages"


# ---
FROM base AS runtime

RUN addgroup --system --gid 10001 exporter \
  && adduser --system --uid 10001 --ingroup exporter exporter

COPY --from=builder --chown=exporter:exporter $APP_HOME/.venv $APP_HOME/.venv
COPY --chown=exporter:exporter care_metrics_exporter $APP_HOME/care_metrics_exporter

# Tests are not part of the runtime surface.
RUN rm -rf $APP_HOME/care_metrics_exporter/tests

ARG APP_VERSION="unknown"
ENV APP_VERSION=$APP_VERSION

LABEL org.opencontainers.image.title="care-metrics-exporter" \
      org.opencontainers.image.description="Prometheus exporter for CARE's Celery queue depth" \
      org.opencontainers.image.source="https://github.com/ohcnetwork/care" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="$APP_VERSION"

USER exporter

EXPOSE 8000

# Deliberately probes /healthz, which never touches Redis: a broker outage must
# not mark the container unhealthy and restart the only thing reporting it.
HEALTHCHECK \
  --interval=30s \
  --timeout=5s \
  --start-period=5s \
  --retries=3 \
  CMD ["python", "-c", "import os,urllib.request,sys; port=os.environ.get('EXPORTER_PORT','8000'); sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{port}/healthz', timeout=3).status == 200 else 1)"]

CMD ["python", "-m", "care_metrics_exporter"]
