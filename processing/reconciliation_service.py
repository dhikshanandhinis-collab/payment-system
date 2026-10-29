"""Reconciliation service: work-queue worker, timeout sweeper and outbox publisher.

* Worker   - claims payments from ``reconciliation_work_queue`` with ``FOR UPDATE SKIP LOCKED`` so any
             number of worker processes can run concurrently, evaluates each payment (case row lock),
             and deletes the queue item in the same transaction.
* Sweeper  - re-queues cases whose ``next_evaluation_at`` waiting period has expired, so a pending
             case becomes an exception only when its configured waiting period ends.
* Outbox   - publishes case changes to ``payment.reconciliation.events`` (at-least-once; consumers
             deduplicate on the payload ``event_id``).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from confluent_kafka import Producer

from common.config import (
    BusinessSettings,
    DatabaseSettings,
    KafkaSettings,
    ReconciliationSettings,
    RetrySettings,
    ServiceSettings,
    TopicSettings,
    load,
)
from common.kafka_config import producer_config
from common.logging_setup import configure_logging
from common.signals import install_signal_handlers
from database.connection import Database
from database.repositories import register_rule_version
from ingestion.error_handler import is_transient, run_with_retry
from observability import metrics
from processing.case_manager import ReconcileOutcome, reconcile_payment
from processing.lifecycle import RESOLVED_STATUSES, ReconciliationStatus
from processing.rules import ReconciliationRules, load_rules

log = logging.getLogger("processing.reconciliation_service")


def record_outcome_metrics(outcome: ReconcileOutcome, trigger_type: str) -> None:
    if outcome.created:
        metrics.CASES_CREATED.labels(trigger_type).inc()
    if outcome.changed or outcome.created:
        status = outcome.new_status or "NONE"
        metrics.CASE_TRANSITIONS.labels(status, trigger_type).inc()
        if status in {s.value for s in RESOLVED_STATUSES}:
            metrics.CASES_RESOLVED.labels(trigger_type).inc()
        if status == ReconciliationStatus.AMOUNT_MISMATCH.value:
            metrics.AMOUNT_MISMATCHES.labels(trigger_type).inc()


class ReconciliationWorker:
    def __init__(
        self,
        database: Database,
        rules: ReconciliationRules,
        tz: ZoneInfo,
        settings: ReconciliationSettings,
        producer: Any | None = None,
        reconciliation_topic: str = "payment.reconciliation.events",
    ) -> None:
        self.db = database
        self.rules = rules
        self.tz = tz
        self.settings = settings
        self.producer = producer
        self.topic = reconciliation_topic
        self.stop_event = threading.Event()

    # ------------------------------------------------------------------ queue

    def process_queue_once(self, now: datetime | None = None) -> int:
        """Claim and evaluate up to ``worker_batch_size`` queued payments in one transaction."""
        processed = 0
        started = time.perf_counter()
        with self.db.transaction() as conn:
            rows = conn.execute(
                """
                SELECT payment_id, trigger_type, first_enqueued_at, attempts
                FROM reconciliation_work_queue
                WHERE available_at <= clock_timestamp()
                ORDER BY available_at, payment_id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (self.settings.worker_batch_size,),
            ).fetchall()
            for payment_id, trigger_type, first_enqueued_at, attempts in rows:
                try:
                    with conn.transaction():
                        outcome = reconcile_payment(conn, payment_id, self.rules, self.tz, trigger_type, now=now)
                        conn.execute("DELETE FROM reconciliation_work_queue WHERE payment_id = %s", (payment_id,))
                except Exception as exc:
                    if is_transient(exc):
                        raise
                    self._mark_failed(conn, payment_id, attempts, exc)
                    continue
                processed += 1
                record_outcome_metrics(outcome, trigger_type)
                metrics.RECON_QUEUE_WAIT_SECONDS.observe(
                    max((datetime.now(UTC) - first_enqueued_at).total_seconds(), 0)
                )
                if outcome.changed or outcome.created:
                    log.info(
                        "case evaluated",
                        extra={"payment_id": payment_id, "processing_stage": "reconciliation",
                               "outcome": outcome.new_status, "reason_code": outcome.reason_code,
                               "previous_status": outcome.previous_status, "trigger_type": trigger_type},
                    )
        if rows:
            metrics.DB_WRITE_SECONDS.labels("reconciliation").observe(time.perf_counter() - started)
        return processed

    def _mark_failed(self, conn: psycopg.Connection, payment_id: str, attempts: int, exc: Exception) -> None:
        metrics.RECON_ITEM_FAILURES.inc()
        backoff = min(2 ** min(attempts, 10), 3600)
        log.error(
            "reconciliation failed for payment; will retry",
            extra={"payment_id": payment_id, "error_code": type(exc).__name__, "attempt": attempts + 1},
            exc_info=exc,
        )
        # Items exceeding max attempts stay in the queue (visible to operators) with a long backoff.
        if attempts + 1 >= self.settings.max_item_attempts:
            backoff = 3600
        conn.execute(
            """
            UPDATE reconciliation_work_queue
            SET attempts = attempts + 1, last_error = %s,
                available_at = clock_timestamp() + make_interval(secs => %s)
            WHERE payment_id = %s
            """,
            (f"{type(exc).__name__}: {str(exc)[:500]}", backoff, payment_id),
        )

    # ------------------------------------------------------------------ sweeper

    def sweep_due_cases(self, now: datetime | None = None) -> int:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO reconciliation_work_queue (payment_id, trigger_type)
                SELECT payment_id, 'TIMEOUT_SWEEP' FROM reconciliation_cases
                WHERE next_evaluation_at IS NOT NULL AND next_evaluation_at <= %s
                ORDER BY payment_id
                ON CONFLICT (payment_id) DO NOTHING
                """,
                (now or datetime.now(UTC),),
            )
            queued = cur.rowcount
            depth = conn.execute("SELECT count(*) FROM reconciliation_work_queue").fetchone()
            metrics.RECON_QUEUE_DEPTH.set(depth[0] if depth else 0)
        return max(queued, 0)

    # ------------------------------------------------------------------ outbox

    def publish_outbox_once(self) -> int:
        if self.producer is None:
            return 0
        with self.db.transaction() as conn:
            rows = conn.execute(
                """
                SELECT outbox_id, event_id, payment_id, event_type, payload FROM reconciliation_outbox
                WHERE published_at IS NULL ORDER BY outbox_id LIMIT %s FOR UPDATE SKIP LOCKED
                """,
                (self.settings.outbox_batch_size,),
            ).fetchall()
            if not rows:
                metrics.OUTBOX_BACKLOG.set(0)
                return 0
            delivered: set[int] = set()
            failed: list[str] = []

            def callback(outbox_id: int) -> Callable[[Any, Any], None]:
                def on_delivery(err: Any, _msg: Any) -> None:
                    if err is None:
                        delivered.add(outbox_id)
                    else:
                        failed.append(str(err))

                return on_delivery

            for outbox_id, event_id, payment_id, event_type, payload in rows:
                body = dict(payload)
                body["event_id"] = str(event_id)
                body["event_type"] = event_type
                self.producer.produce(
                    self.topic,
                    key=payment_id.encode(),
                    value=json.dumps(body, sort_keys=True).encode(),
                    on_delivery=callback(outbox_id),
                )
                self.producer.poll(0)
            self.producer.flush(30)
            if delivered:
                conn.execute(
                    "UPDATE reconciliation_outbox SET published_at = clock_timestamp() WHERE outbox_id = ANY(%s)",
                    (sorted(delivered),),
                )
            metrics.OUTBOX_PUBLISHED.labels("delivered").inc(len(delivered))
            if failed:
                metrics.OUTBOX_PUBLISHED.labels("failed").inc(len(failed))
                log.warning("outbox publish failures; rows stay unpublished for retry", extra={"failed": len(failed)})
            backlog = conn.execute("SELECT count(*) FROM reconciliation_outbox WHERE published_at IS NULL").fetchone()
            metrics.OUTBOX_BACKLOG.set(max((backlog[0] if backlog else 0) - len(delivered), 0))
            return len(delivered)

    # ------------------------------------------------------------------ loop

    def run(self, health: metrics.HealthState, retry: RetrySettings) -> None:
        last_sweep = 0.0
        health.set_ready(True, "running")
        while not self.stop_event.is_set():
            try:
                if time.monotonic() - last_sweep >= self.settings.sweep_interval_s:
                    run_with_retry("reconciliation_sweep", retry, self.sweep_due_cases)
                    last_sweep = time.monotonic()
                processed = run_with_retry("reconciliation_queue", retry, self.process_queue_once)
                if self.settings.publish_outbox:
                    run_with_retry("reconciliation_outbox", retry, self.publish_outbox_once)
                health.set_ready(True, "running")
            except Exception as exc:  # noqa: BLE001 - keep the service alive; work stays queued
                health.set_ready(False, type(exc).__name__)
                log.exception("reconciliation loop iteration failed", extra={"error_code": type(exc).__name__})
                processed = 0
                self.stop_event.wait(5)
            if processed == 0:
                self.stop_event.wait(self.settings.poll_interval_s)


def main() -> None:
    service = load(ServiceSettings)
    configure_logging(service.service_name or "reconciliation", service.environment, service.log_level)
    recon_settings, retry = load(ReconciliationSettings), load(RetrySettings)
    business = load(BusinessSettings)
    db_settings = load(DatabaseSettings)
    rules = load_rules(recon_settings.rules_path)

    health = metrics.HealthState()
    if service.metrics_enabled:
        metrics.start_http_server(service.metrics_port, health)
    database = Database(db_settings, name="reconciliation")
    run_with_retry("db_connect", retry, lambda: database.open(wait=True, timeout=30))
    health.probe = database.ping
    with database.transaction() as conn:
        register_rule_version(conn, rules)

    producer = None
    topics = load(TopicSettings)
    if recon_settings.publish_outbox:
        producer = Producer(producer_config(load(KafkaSettings), "reconciliation-outbox"))
    worker = ReconciliationWorker(database, rules, business.tz, recon_settings, producer, topics.reconciliation_events)
    install_signal_handlers(worker.stop_event)
    log.info("reconciliation service started", extra={"rule_version": rules.rule_version})
    try:
        worker.run(health, retry)
    finally:
        if producer is not None:
            producer.flush(10)
        database.close()


if __name__ == "__main__":
    main()
