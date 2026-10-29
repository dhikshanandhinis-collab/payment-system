"""Failure injection and recovery: database outage, consumer restart/crash before offset commit, replay."""

from __future__ import annotations

import threading
import time
import uuid
from datetime import timedelta

import pytest
from psycopg_pool import PoolTimeout

from admin.cli import replay_quarantine
from common.config import AdminDatabaseSettings, load
from database.connection import Database, connect
from tests.conftest import create_test_topics
from tests.factories import app, matched_payment
from tests.harness import Harness, recent_past

RUN = uuid.uuid4().hex[:6].upper()
T0 = recent_past(2).replace(microsecond=0)


def payments(prefix: str, n: int) -> list[dict]:
    out = []
    for i in range(n):
        out += matched_payment(f"PAY-{prefix}{RUN}-{i:04d}", "10.00", T0 + timedelta(seconds=i))
    return out


def raw_count_when_available(harness) -> int:
    """Raw count, or -1 while the pool is still reconnecting after the outage."""
    try:
        return harness.raw_count()
    except PoolTimeout:
        return -1


def silver_counts(harness, prefix):
    with harness.database.transaction() as conn:
        return conn.execute(
            """
            SELECT (SELECT count(*) FROM application_payment_events WHERE payment_id LIKE %(p)s),
                   (SELECT count(*) FROM processor_transactions WHERE payment_id LIKE %(p)s),
                   (SELECT count(*) FROM billing_ledger_entries WHERE payment_id LIKE %(p)s),
                   (SELECT count(*) FROM raw_payment_events WHERE payment_id LIKE %(p)s),
                   (SELECT count(*) FROM (SELECT source_topic, source_partition, source_offset FROM raw_payment_events
                        WHERE payment_id LIKE %(p)s GROUP BY 1, 2, 3 HAVING count(*) > 1) d)
            """,
            {"p": f"PAY-{prefix}{RUN}-%"},
        ).fetchone()


def test_database_outage_does_not_lose_or_duplicate_events(database, rules, db_settings):
    """Scenario 13: the database becomes unavailable mid-stream; offsets are not committed until it recovers."""
    kafka_topics = create_test_topics()
    admin_settings = load(AdminDatabaseSettings)
    admin = connect(db_settings, autocommit=True, user=admin_settings.superuser,
                    password=admin_settings.superuser_password.get_secret_value(), dbname="postgres")
    # Short connection timeouts so bounded retries are exhausted inside the outage window.
    outage_db = Database(db_settings.model_copy(update={"connect_timeout_s": 1}), name="outage", max_size=4)
    outage_db.open()
    harness = Harness(outage_db, kafka_topics, rules, group=f"outage-{RUN}", batch_size=25)
    events = payments("OUT", 40)  # 200 events
    published = harness.publish(events[:100])
    deadline = time.monotonic() + 60
    while harness.raw_count() < 25 and time.monotonic() < deadline:
        harness.ingestion.poll_once()  # some batches are processed normally before the outage

    rewinds: list[int] = []
    original_rewind = harness.ingestion._rewind
    harness.ingestion._rewind = lambda records: (rewinds.append(len(records)), original_rewind(records))
    stop = threading.Event()

    def ingest_loop():
        while not stop.is_set():
            harness.ingestion.poll_once()

    worker = threading.Thread(target=ingest_loop, daemon=True)
    try:
        try:
            admin.execute(f'ALTER DATABASE "{db_settings.name}" WITH ALLOW_CONNECTIONS false')
            admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s", (db_settings.name,)
            )
            published += harness.publish(events[100:])  # Kafka keeps accepting events during the DB outage
            worker.start()
            time.sleep(12)  # outage window: every write fails; consumer retries, rewinds, commits nothing
            assert rewinds, "the consumer should rewind uncommitted records while the database is down"
            assert harness.ingestion.health.status()[0] is False
        finally:
            admin.execute(f'ALTER DATABASE "{db_settings.name}" WITH ALLOW_CONNECTIONS true')
            admin.close()
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and raw_count_when_available(harness) < published:
            time.sleep(0.5)
    finally:
        stop.set()
        worker.join(timeout=30)
    harness.reconcile_all()

    app_n, proc_n, ledger_n, raw_n, dup_positions = silver_counts(harness, "OUT")
    assert (app_n, proc_n, ledger_n, raw_n, dup_positions) == (80, 80, 40, 200, 0)
    with database.transaction() as conn:
        matched = conn.execute(
            "SELECT count(*) FROM reconciliation_cases WHERE payment_id LIKE %s AND reconciliation_status = 'MATCHED'",
            (f"PAY-OUT{RUN}-%",),
        ).fetchone()[0]
    assert matched == 40
    harness.close()
    outage_db.close()


def test_crash_before_offset_commit_and_restart_is_idempotent(database, rules):
    """Scenario 14: consumer dies after the DB commit but before committing offsets; a new consumer
    in the same group re-reads the batch and skips it as already ingested."""
    kafka_topics = create_test_topics()
    group = f"restart-{RUN}"
    harness = Harness(database, kafka_topics, rules, group=group, batch_size=30)
    published = harness.publish(payments("RST", 30))  # 150 events

    first = harness.ingestion
    first._commit = lambda records: None  # simulate crash window: DB committed, offsets never committed
    deadline = time.monotonic() + 60
    while harness.raw_count() < 60 and time.monotonic() < deadline:
        first.poll_once()
    assert harness.raw_count() >= 60
    first.consumer.close()  # "crash": leave the group without committing
    first._closed = True

    second = harness.new_ingestion()
    outcomes: list[str] = []
    original = second._record_metrics
    second._record_metrics = lambda results: (outcomes.extend(r.outcome for r in results), original(results))
    harness.ingest_until(published, service=second)
    for _ in range(3):
        second.poll_once()  # drain any remaining redeliveries
    harness.reconcile_all()
    assert outcomes.count("ALREADY_INGESTED") >= 60  # replayed records were recognised, not re-applied

    app_n, proc_n, ledger_n, raw_n, dup_positions = silver_counts(harness, "RST")
    assert (app_n, proc_n, ledger_n, raw_n, dup_positions) == (60, 60, 30, 150, 0)
    with database.transaction() as conn:
        matched = conn.execute(
            "SELECT count(*), sum(processor_amount) FROM reconciliation_cases WHERE payment_id LIKE %s "
            "AND reconciliation_status = 'MATCHED'", (f"PAY-RST{RUN}-%",),
        ).fetchone()
    assert matched[0] == 30 and str(matched[1]) == "300.00"
    second.close()
    harness.producer.flush(5)


def test_quarantined_event_replay_after_correction(database, rules):
    """Corrected quarantined events are replayed through Kafka, so validation and idempotency still apply."""
    kafka_topics = create_test_topics()
    harness = Harness(database, kafka_topics, rules, group=f"replay-{RUN}")
    p = f"PAY-RPL{RUN}-0001"
    bad = app(p, t=T0)
    bad["currency"] = "C$D"
    before = harness.raw_count()
    harness.publish([bad])
    harness.run(before + 1)
    with database.transaction() as conn:
        qid = conn.execute("SELECT quarantine_id FROM quarantined_events WHERE payment_id = %s", (p,)).fetchone()[0]

    fixed = {**bad, "currency": "CAD"}
    import json

    with database.pool.connection() as conn:
        conn.autocommit = True
        with pytest.raises(ValueError, match="still invalid"):
            replay_quarantine(conn, harness.producer, kafka_topics, rules, qid, json.dumps(bad).encode(),
                              "ops.alice", "Attempt replay without fix", frozenset({"ops.alice"}))
        result = replay_quarantine(conn, harness.producer, kafka_topics, rules, qid, json.dumps(fixed).encode(),
                                   "ops.alice", "Currency corrected by source team", frozenset({"ops.alice"}))
        conn.autocommit = False
    harness.run(before + 2)
    with database.transaction() as conn:
        status = conn.execute("SELECT status, resolved_by FROM quarantined_events WHERE quarantine_id = %s", (qid,)).fetchone()
        req = conn.execute("SELECT currency FROM payment_requests WHERE payment_id = %s", (p,)).fetchone()
    assert result["quarantine_id"] == qid
    assert status == ("REPLAYED", "ops.alice") and req[0] == "CAD"
    harness.close()
