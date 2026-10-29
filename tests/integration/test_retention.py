"""PostgreSQL integration: Bronze payload archival, outbox purge and audit-history partition retention."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from batch.retention import RetentionSettings, read_archive, run_retention
from common.config import AdminDatabaseSettings, TopicSettings, load
from database.connection import connect
from database.repositories import KafkaRecord
from ingestion.event_writer import EventWriter
from tests.factories import app


def migrator(db_settings):
    admin = load(AdminDatabaseSettings)
    return connect(db_settings, autocommit=True, user=admin.migrator_user,
                   password=admin.migrator_password.get_secret_value())


def test_retention_archives_payloads_and_drops_expired_history(database, db_settings, rules, tmp_path):
    topics = TopicSettings()
    payloads = [app(f"RET-{i}") for i in range(3)]
    records = [KafkaRecord(topics.app_events, 0, 50_000 + i, b"k", json.dumps(p).encode(), datetime.now(UTC))
               for i, p in enumerate(payloads)]
    with database.transaction() as conn:
        EventWriter(topics, rules).process_batch(conn, records)

    settings = RetentionSettings(archive_dir=tmp_path, batch_size=2, DB_MIGRATOR_PASSWORD="unused")
    with migrator(db_settings) as conn:
        conn.execute("UPDATE raw_payment_events SET ingestion_time = now() - interval '400 days' "
                     "WHERE source_offset BETWEEN 50000 AND 50001")
        conn.execute("SELECT create_history_partition(DATE '2015-01-01')")

        dry = run_retention(conn, settings, dry_run=True)
        assert dry.bronze_archived == 2
        assert dry.partitions_dropped == ["reconciliation_history_201501"]
        assert conn.execute("SELECT to_regclass('reconciliation_history_201501')").fetchone()[0] is not None

        summary = run_retention(conn, settings)
        assert summary.bronze_archived == 2
        assert summary.partitions_dropped == ["reconciliation_history_201501"]
        assert conn.execute("SELECT to_regclass('reconciliation_history_201501')").fetchone()[0] is None

        rows = conn.execute(
            "SELECT source_offset, length(payload_bytes), payload_json IS NULL, payload_archived_at IS NOT NULL, "
            "archive_uri, payload_hash FROM raw_payment_events WHERE source_offset >= 50000 ORDER BY source_offset"
        ).fetchall()
        assert [r[1:4] for r in rows[:2]] == [(0, True, True), (0, True, True)]
        assert rows[2][3] is False and rows[2][1] > 0

        restored = {r["source_offset"]: r for f in summary.archive_files for r in read_archive(Path(f))}
        assert set(restored) == {50_000, 50_001}
        assert json.loads(restored[50_000]["payload_bytes"]) == payloads[0]
        assert restored[50_000]["payload_hash"] == rows[0][5]

        assert run_retention(conn, settings).bronze_archived == 0  # idempotent


def test_history_stays_append_only_after_partitioning(database):
    with database.transaction() as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("DELETE FROM reconciliation_history")
