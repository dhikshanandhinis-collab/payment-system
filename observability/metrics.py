"""Prometheus metrics with bounded-cardinality labels only.

Labels are restricted to small enumerations (source_system, topic, partition, status, error_code,
outcome, stage). Payment, account and event identifiers are never used as labels.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300)

EVENTS_CONSUMED = Counter("recon_events_consumed_total", "Kafka records consumed", ["topic"])
EVENTS_PERSISTED = Counter("recon_events_persisted_total", "Raw events committed to Bronze", ["source_system"])
EVENTS_VALIDATED = Counter("recon_events_validated_total", "Events that passed schema validation", ["source_system"])
EVENTS_REJECTED = Counter(
    "recon_events_rejected_total", "Events rejected by validation", ["source_system", "error_category", "error_code"]
)
EVENTS_QUARANTINED = Counter("recon_events_quarantined_total", "Events written to quarantine", ["source_system"])
DUPLICATES_DETECTED = Counter("recon_duplicates_detected_total", "Duplicate events detected", ["source_system", "kind"])
REDELIVERED_RECORDS = Counter(
    "recon_kafka_redelivered_records_total", "Kafka records already present in Bronze (offset replay)", ["topic"]
)
DLQ_PUBLISHED = Counter("recon_dlq_published_total", "Messages published to the dead-letter topic", ["outcome"])
RETRIES = Counter("recon_retries_total", "Retry attempts after transient failures", ["component"])
BATCH_FAILURES = Counter("recon_ingestion_batch_failures_total", "Ingestion batches rolled back", ["error_category"])
OFFSET_COMMITS = Counter("recon_offset_commits_total", "Kafka offset commit results", ["outcome"])

CONSUMER_PROCESSING_SECONDS = Histogram(
    "recon_consumer_batch_processing_seconds", "Time to process and commit one consumer batch", buckets=LATENCY_BUCKETS
)
DB_WRITE_SECONDS = Histogram(
    "recon_db_write_seconds", "Database transaction duration", ["component"], buckets=LATENCY_BUCKETS
)
EVENT_AGE_SECONDS = Histogram(
    "recon_event_age_at_ingestion_seconds",
    "Ingestion time minus source event_time",
    ["source_system"],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900, 3600),
)
CONSUMER_LAG = Gauge("recon_consumer_lag_messages", "High watermark minus committed offset", ["topic", "partition"])

CASES_CREATED = Counter("recon_cases_created_total", "Reconciliation cases created", ["trigger_type"])
CASE_TRANSITIONS = Counter(
    "recon_case_transitions_total", "Reconciliation status transitions", ["new_status", "trigger_type"]
)
CASES_RESOLVED = Counter("recon_cases_resolved_total", "Cases reaching MATCHED/NO_FINANCIAL_EFFECT", ["trigger_type"])
AMOUNT_MISMATCHES = Counter("recon_amount_mismatches_total", "Transitions into AMOUNT_MISMATCH", ["trigger_type"])
RECON_ITEM_FAILURES = Counter("recon_reconciliation_item_failures_total", "Queue items that failed evaluation", [])
RECON_QUEUE_DEPTH = Gauge("recon_work_queue_depth", "Payments waiting for (re-)evaluation")
RECON_QUEUE_WAIT_SECONDS = Histogram(
    "recon_work_queue_wait_seconds", "Time between enqueue and evaluation", buckets=LATENCY_BUCKETS
)
OUTBOX_PUBLISHED = Counter("recon_outbox_published_total", "Reconciliation events published to Kafka", ["outcome"])
OUTBOX_BACKLOG = Gauge("recon_outbox_backlog", "Unpublished reconciliation outbox rows")

BATCH_RUN_SECONDS = Histogram(
    "recon_batch_run_seconds", "Batch reconciliation run duration", ["run_type", "outcome"],
    buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600),
)
BATCH_RUNS = Counter("recon_batch_runs_total", "Batch reconciliation runs", ["run_type", "outcome"])

PRODUCED = Counter("recon_simulator_events_produced_total", "Synthetic events produced", ["source_system", "kind"])
PRODUCE_ERRORS = Counter("recon_simulator_produce_errors_total", "Synthetic events that failed delivery", ["source_system"])


class HealthState:
    """Thread-safe liveness/readiness flags plus optional readiness probe."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready = False
        self._detail = "starting"
        self.probe: Callable[[], bool] | None = None

    def set_ready(self, ready: bool, detail: str = "") -> None:
        with self._lock:
            self._ready = ready
            self._detail = detail or ("ready" if ready else "not ready")

    def status(self) -> tuple[bool, str]:
        with self._lock:
            ready, detail = self._ready, self._detail
        if ready and self.probe is not None and not self.probe():
            return False, "dependency probe failed"
        return ready, detail


def start_http_server(port: int, health: HealthState) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server API
            if self.path == "/metrics":
                body, code, ctype = generate_latest(), 200, CONTENT_TYPE_LATEST
            elif self.path == "/healthz":
                body, code, ctype = b'{"status":"alive"}', 200, "application/json"
            elif self.path == "/readyz":
                ready, detail = health.status()
                body = json.dumps({"status": "ready" if ready else "not_ready", "detail": detail}).encode()
                code, ctype = (200 if ready else 503), "application/json"
            else:
                body, code, ctype = b"not found", 404, "text/plain"
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - silence access logs
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)  # noqa: S104 - container-internal endpoint
    thread = threading.Thread(target=server.serve_forever, name="metrics-http", daemon=True)
    thread.start()
    return server
