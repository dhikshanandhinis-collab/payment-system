from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from batch.retention import (
    ArchiveIntegrityError,
    RetentionSettings,
    archive_line,
    expired_partitions,
    history_drop_before,
    read_archive,
    write_archive,
)
from processing.normalization import sha256_hex


def row(raw_event_id: int, payload: bytes, payload_hash: str | None = None) -> dict:
    return {
        "raw_event_id": raw_event_id, "source_system": "mybell_app", "source_topic": "t", "source_partition": 0,
        "source_offset": raw_event_id, "kafka_key": "k", "ingestion_time": datetime(2026, 1, 1, tzinfo=UTC),
        "payload_bytes": payload, "payload_hash": payload_hash or sha256_hex(payload),
    }


def test_history_drop_before_counts_whole_months():
    assert history_drop_before(datetime(2026, 3, 15, tzinfo=UTC), 84) == date(2019, 3, 1)
    assert history_drop_before(datetime(2026, 1, 1, tzinfo=UTC), 13) == date(2024, 12, 1)


def test_expired_partitions_only_matches_monthly_partitions_before_cutoff():
    names = ["reconciliation_history_201812", "reconciliation_history_201903", "reconciliation_history_default",
             "reconciliation_history_201902", "other_201801"]
    assert expired_partitions(names, date(2019, 3, 1)) == ["reconciliation_history_201812",
                                                           "reconciliation_history_201902"]


def test_archive_round_trip_preserves_exact_bytes(tmp_path):
    rows = [row(1, b'{"a":1}'), row(2, b"\x00\xffnot-json")]
    path = write_archive(tmp_path, date(2026, 1, 1), rows)
    assert path.name.startswith("raw-1-2-") and path.suffixes[-2:] == [".ndjson", ".gz"]
    restored = read_archive(path)
    assert [r["payload_bytes"] for r in restored] == [b'{"a":1}', b"\x00\xffnot-json"]
    assert not list(tmp_path.rglob("*.tmp"))


def test_archive_refuses_payload_that_does_not_match_hash(tmp_path):
    with pytest.raises(ArchiveIntegrityError):
        archive_line(row(7, b"changed", payload_hash=sha256_hex(b"original")))
    with pytest.raises(ArchiveIntegrityError):
        write_archive(tmp_path, date(2026, 1, 1), [row(7, b"x", payload_hash="0" * 64)])


def test_archive_line_is_compact_json():
    record = json.loads(archive_line(row(3, b"abc")))
    assert record["payload_b64"] == "YWJj" and record["raw_event_id"] == 3


def test_retention_settings_enforce_minimums():
    with pytest.raises(ValueError):
        RetentionSettings(bronze_payload_days=1, DB_MIGRATOR_PASSWORD="x")
    with pytest.raises(ValueError):
        RetentionSettings(history_months=6, DB_MIGRATOR_PASSWORD="x")
