# One image for all Python services; the command selects the service.

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app
WORKDIR /app
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --home-dir /app app
COPY requirements.lock ./
RUN pip install --no-deps -r requirements.lock

FROM base AS runtime
COPY pyproject.toml ./
COPY config/ config/
COPY schemas/ schemas/
COPY common/ common/
COPY processing/ processing/
COPY database/ database/
COPY ingestion/ ingestion/
COPY generators/ generators/
COPY batch/ batch/
COPY analytics/ analytics/
COPY observability/metrics.py observability/__init__.py observability/
COPY admin/ admin/
COPY scripts/ scripts/
# Named volumes mounted here inherit this ownership, so the non-root services can write settlement files.
RUN mkdir -p /data/settlement/inbox && chown -R app:app /data
USER app
EXPOSE 9100
CMD ["python", "-m", "ingestion.consumer"]

# Dev/test dependencies are installed before the source is copied so code changes do not reinstall them.
FROM base AS test
COPY requirements-dev.lock ./
RUN pip install --no-deps -r requirements-dev.lock
COPY --from=runtime /app /app
COPY tests/ tests/
RUN mkdir -p /app/reports && chown app:app /app/reports
USER app
CMD ["pytest", "-m", "not integration and not e2e and not recovery"]
