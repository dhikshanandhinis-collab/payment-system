"""PostgreSQL integration: migrations, constraints, least privilege, transactional ingestion and quarantine."""

from __future__ import annotations

import itertools
import json
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from common.config import AdminDatabaseSettings, TopicSettings, load
from database.connection import connect
from database.migrate import MigrationError, Placeholders, apply_migrations
from database.repositories import KafkaRecord
from ingestion.event_writer import (
    ALREADY_INGESTED,
    DUPLICATE_BUSINESS_EVENT,
    DUPLICATE_DELIVERY,
    PROCESSED,
    QUARANTINED,
    EventWriter,
)
from tests.factories import app, ledger, proc

TOPICS = TopicSettings()
_offsets = itertools.count(1_000)
T0 = (datetime.now(UTC) - timedelta(hours=2)).replace(microsecond=0)


def rec(payload: dict | bytes, topic: str | None = None, offset: int | None = None) -> KafkaRecord:
    if isinstance(payload, dict):
        topic = topic or {"mybell_app": TOPICS.app_events, "payment_processor": TOPICS.processor_events,
                          "billing_ledger": TOPICS.ledger_events}[payload["source_system"]]
        value = json.dumps(payload).encode()
    else:
        value = payload
    return KafkaRecord(topic or TOPICS.app_events, 0, offset if offset is not None else next(_offsets), b"k", value,
                       datetime.now(UTC))


def write(database, rules, records):
    writer = EventWriter(TOPICS, rules)
    with database.transaction() as conn:
        return writer.process_batch(conn, records)


def scalar(database, query, params=()):
    with database.transaction() as conn:
        return conn.execute(query, params).fetchone()[0]


def test_migrations_are_idempotent_and_tamper_evident(db_settings):
    admin = load(AdminDatabaseSettings)
    ph = Placeholders(db_settings.schema_name, admin.app_user, admin.reporting_user)
    with connect(db_settings, user=admin.migrator_user, password=admin.migrator_password.get_secret_value()) as conn:
        assert apply_migrations(conn, ph) == []
        conn.execute("UPDATE schema_migrations SET checksum = repeat('0', 64) WHERE version = '0001_bronze'")
        conn.commit()
        with pytest.raises(MigrationError, match="modified"):
            apply_migrations(conn, ph)
        conn.rollback()
        import hashlib

        from database.migrate import MIGRATIONS_DIR

        real = hashlib.sha256((MIGRATIONS_DIR / "0001_bronze.sql").read_bytes()).hexdigest()
        conn.execute("UPDATE schema_migrations SET checksum = %s WHERE version = '0001_bronze'", (real,))
        conn.commit()


def test_valid_event_written_to_bronze_silver_and_queue(database, rules):
    pid = "PAY-IT-DB-0001"
    [result] = write(database, rules, [rec(app(pid, t=T0, amount="42.10"))])
    assert result.outcome == PROCESSED
    with database.transaction() as conn:
        raw = conn.execute(
            "SELECT processing_status, payload_json->>'amount', payload_hash FROM raw_payment_events WHERE payment_id = %s",
            (pid,),
        ).fetchone()
        req = conn.execute("SELECT amount, initiation_count FROM payment_requests WHERE payment_id = %s", (pid,)).fetchone()
        queued = conn.execute("SELECT count(*) FROM reconciliation_work_queue WHERE payment_id = %s", (pid,)).fetchone()
    assert raw[0] == "PROCESSED" and raw[1] == "42.10" and len(raw[2]) == 64
    assert str(req[0]) == "42.10" and req[1] == 1 and queued[0] == 1


def test_redelivery_duplicates_and_conflicts(database, rules):
    pid = "PAY-IT-DB-0002"
    capture = proc(pid, "CAPTURE", "C-IT-0002", "10.00", T0)
    first = rec(capture)
    assert write(database, rules, [first])[0].outcome == PROCESSED
    # Same Kafka position replayed after a crash before offset commit.
    assert write(database, rules, [first])[0].outcome == ALREADY_INGESTED
    # Same event delivered again at a new offset (producer retry / source re-send).
    assert write(database, rules, [rec(capture)])[0].outcome == DUPLICATE_DELIVERY
    # Same processor transaction re-sent under a new event_id.
    resend = {**capture, "event_id": "EVT-RESEND-0002", "event_time": (T0 + timedelta(seconds=30)).isoformat()}
    assert write(database, rules, [rec(resend)])[0].outcome == DUPLICATE_BUSINESS_EVENT
    # Same transaction id with different financial content is quarantined, not accepted or dropped.
    conflict = {**capture, "event_id": "EVT-CONFLICT-0002", "amount": "11.00"}
    result = write(database, rules, [rec(conflict)])[0]
    assert result.outcome == QUARANTINED and result.error_code == "BUSINESS_KEY_CONFLICT"
    assert scalar(database, "SELECT count(*) FROM processor_transactions WHERE payment_id = %s", (pid,)) == 1
    assert scalar(database, "SELECT sum(amount) FROM processor_transactions WHERE payment_id = %s", (pid,)) == 10


def test_invalid_payloads_are_quarantined_with_original_bytes(database, rules):
    garbage = b"\xff\xfenot utf-8 and not json"
    bad_schema = app("PAY-IT-DB-0003", t=T0)
    bad_schema["schema_version"] = "7.0"
    negative = app("PAY-IT-DB-0004", t=T0)
    negative["amount"] = "-1.00"
    results = write(database, rules, [rec(garbage), rec(bad_schema), rec(negative)])
    assert [r.outcome for r in results] == [QUARANTINED] * 3
    assert [r.error_code for r in results] == ["INVALID_ENCODING", "UNSUPPORTED_SCHEMA_VERSION", "INVALID_AMOUNT"]
    with database.transaction() as conn:
        row = conn.execute(
            "SELECT q.payload_bytes, q.error_category, r.payload_json, r.processing_status FROM quarantined_events q "
            "JOIN raw_payment_events r USING (raw_event_id) WHERE q.error_code = 'INVALID_ENCODING'"
        ).fetchone()
        categories = dict(conn.execute("SELECT error_code, error_category FROM quarantined_events").fetchall())
    assert bytes(row[0]) == garbage and row[2] is None and row[3] == "QUARANTINED"
    assert categories["UNSUPPORTED_SCHEMA_VERSION"] == "UNSUPPORTED_SCHEMA"
    assert scalar(database, "SELECT count(*) FROM payment_requests WHERE payment_id IN ('PAY-IT-DB-0003','PAY-IT-DB-0004')") == 0


def test_batch_is_atomic(database, rules):
    pid = "PAY-IT-DB-0005"
    writer = EventWriter(TOPICS, rules)
    with pytest.raises(RuntimeError), database.transaction() as conn:
        writer.process_batch(conn, [rec(app(pid, t=T0))])
        raise RuntimeError("simulated crash before commit")
    assert scalar(database, "SELECT count(*) FROM raw_payment_events WHERE payment_id = %s", (pid,)) == 0


def test_runtime_role_is_least_privilege(database):
    with database.transaction() as conn:
        conn.execute("SELECT 1")
    for statement in (
        "DELETE FROM raw_payment_events",
        "UPDATE raw_payment_events SET payload_bytes = 'x'",
        "DELETE FROM reconciliation_history",
        "DROP TABLE billing_ledger_entries",
        "UPDATE payment_requests SET payment_id = 'x'",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege), database.transaction() as conn:
            conn.execute(statement)


def test_history_is_append_only_even_for_owner(db_settings, database, rules):
    from processing.case_manager import reconcile_payment
    from tests.harness import TZ

    pid = "PAY-IT-DB-0006"
    write(database, rules, [rec(app(pid, t=T0))])
    with database.transaction() as conn:
        reconcile_payment(conn, pid, rules, TZ, "STREAM")
    admin = load(AdminDatabaseSettings)
    with connect(db_settings, user=admin.migrator_user, password=admin.migrator_password.get_secret_value()) as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege, match="append-only"):
            conn.execute("UPDATE reconciliation_history SET new_status = 'MATCHED'")
        conn.rollback()
        with pytest.raises(psycopg.errors.InsufficientPrivilege, match="append-only"):
            conn.execute("DELETE FROM reconciliation_history")


def test_database_constraints(database):
    with pytest.raises(psycopg.errors.CheckViolation), database.transaction() as conn:
        conn.execute(
            "INSERT INTO payment_requests (payment_id, account_ref, amount, currency, payment_method, initiated_at, "
            "first_seen_event_id) VALUES ('P-NEG', 'A', -1, 'CAD', 'CREDIT_CARD', now(), 'E')"
        )
    with pytest.raises(psycopg.errors.CheckViolation), database.transaction() as conn:
        conn.execute(
            "INSERT INTO payment_requests (payment_id, account_ref, amount, currency, payment_method, initiated_at, "
            "first_seen_event_id) VALUES ('P-CUR', 'A', 1, 'cad', 'CREDIT_CARD', now(), 'E')"
        )


def test_payment_request_initiation_is_authoritative_and_retries_counted(database, rules):
    pid = "PAY-IT-DB-0007"
    success_first = app(pid, "PAYMENT_SUCCEEDED", amount="20.00", t=T0 + timedelta(seconds=5))
    init = app(pid, "PAYMENT_INITIATED", amount="20.00", t=T0, method="DEBIT_CARD")
    retry = app(pid, "PAYMENT_INITIATED", amount="20.00", t=T0 + timedelta(seconds=1), method="DEBIT_CARD")
    write(database, rules, [rec(success_first)])
    write(database, rules, [rec(init), rec(retry)])
    with database.transaction() as conn:
        row = conn.execute(
            "SELECT payment_method, initiated_at, initiated_event_id, initiation_count FROM payment_requests "
            "WHERE payment_id = %s", (pid,)
        ).fetchone()
    assert row[0] == "DEBIT_CARD" and row[1] == T0 and row[2] == init["event_id"] and row[3] == 2


def test_ledger_charge_without_payment_is_stored_but_not_queued(database, rules):
    charge = ledger(None, "CHARGE", "LED-CH-IT-0008", "55.00", T0, account_ref="SYN-ACCT-00000099")
    [result] = write(database, rules, [rec(charge)])
    assert result.outcome == PROCESSED and result.payment_id is None
    assert scalar(database, "SELECT count(*) FROM billing_ledger_entries WHERE ledger_entry_id = 'LED-CH-IT-0008'") == 1
