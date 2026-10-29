"""Data retention: archive old Bronze payloads, purge published outbox rows, roll audit-history partitions.

* Bronze payloads older than RETENTION_BRONZE_PAYLOAD_DAYS are written to gzip NDJSON files under
  RETENTION_ARCHIVE_DIR (mount object storage there in production). The payload hash is checked against the stored
  hash before anything is blanked. The row itself stays: its Kafka position is the idempotency key, and Silver tables
  reference raw_event_id.
* Outbox rows that were published more than RETENTION_OUTBOX_DAYS ago are deleted.
* reconciliation_history partitions are created RETENTION_HISTORY_MONTHS_AHEAD months ahead. Whole months older than
  RETENTION_HISTORY_MONTHS are dropped (the append-only rule allows no other way to remove history).

The job needs owner rights (UPDATE on payload columns, DELETE, DROP), so it connects as the migrator role. It is
safe to re-run: each batch commits only after its archive file is durable on disk.

Usage:
    python -m batch.retention [--dry-run]
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from pydantic import Field, SecretStr
from pydantic_settings import SettingsConfigDict

from common.config import DatabaseSettings, ServiceSettings, _Base, load
from common.logging_setup import configure_logging
from database.connection import connect
from processing.normalization import sha256_hex

_PARTITION = re.compile(r"^reconciliation_history_(\d{4})(\d{2})$")


class RetentionSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="RETENTION_", extra="ignore", case_sensitive=False, frozen=True)

    bronze_payload_days: int = Field(default=90, ge=7)
    outbox_days: int = Field(default=30, ge=1)
    history_months: int = Field(default=84, ge=13)
    history_months_ahead: int = Field(default=3, ge=1, le=24)
    archive_dir: Path = Path("/data/archive")
    batch_size: int = Field(default=5000, ge=1, le=100_000)
    max_batches: int = Field(default=200, ge=1)
    db_user: str = Field(default="recon_migrator", validation_alias="DB_MIGRATOR_USER")
    db_password: SecretStr = Field(validation_alias="DB_MIGRATOR_PASSWORD")


@dataclass
class RetentionSummary:
    dry_run: bool
    bronze_cutoff: str
    bronze_archived: int = 0
    archive_files: list[str] = field(default_factory=list)
    outbox_deleted: int = 0
    partitions_created: int = 0
    partitions_dropped: list[str] = field(default_factory=list)


class ArchiveIntegrityError(RuntimeError):
    pass


def month_start(value: date) -> date:
    return value.replace(day=1)


def history_drop_before(now: datetime, months: int) -> date:
    """First day of the oldest month that is kept; partitions wholly before it are dropped."""
    total = now.year * 12 + (now.month - 1) - months
    return date(total // 12, total % 12 + 1, 1)


def expired_partitions(names: list[str], keep_from: date) -> list[str]:
    expired = []
    for name in names:
        match = _PARTITION.match(name)
        if match and date(int(match[1]), int(match[2]), 1) < keep_from:
            expired.append(name)
    return sorted(expired)


def archive_line(row: dict[str, Any]) -> bytes:
    payload: bytes = bytes(row["payload_bytes"])
    if sha256_hex(payload) != row["payload_hash"]:
        raise ArchiveIntegrityError(f"raw_event_id {row['raw_event_id']}: payload does not match payload_hash")
    record = {
        "raw_event_id": row["raw_event_id"],
        "source_system": row["source_system"],
        "source_topic": row["source_topic"],
        "source_partition": row["source_partition"],
        "source_offset": row["source_offset"],
        "kafka_key": row["kafka_key"],
        "ingestion_time": row["ingestion_time"].isoformat(),
        "payload_hash": row["payload_hash"],
        "payload_b64": base64.b64encode(payload).decode("ascii"),
    }
    return json.dumps(record, separators=(",", ":")).encode() + b"\n"


def read_archive(path: Path) -> list[dict[str, Any]]:
    """Restore helper: returns archived records with payload bytes decoded and verified."""
    records = []
    with gzip.open(path, "rb") as handle:
        for line in handle:
            record = json.loads(line)
            record["payload_bytes"] = base64.b64decode(record.pop("payload_b64"))
            if sha256_hex(record["payload_bytes"]) != record["payload_hash"]:
                raise ArchiveIntegrityError(f"{path}: raw_event_id {record['raw_event_id']} failed hash check")
            records.append(record)
    return records


def write_archive(directory: Path, cutoff: date, rows: list[dict[str, Any]]) -> Path:
    day_dir = directory / "bronze" / cutoff.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    target = day_dir / f"raw-{rows[0]['raw_event_id']}-{rows[-1]['raw_event_id']}-{uuid.uuid4().hex[:8]}.ndjson.gz"
    tmp = target.with_suffix(".tmp")
    with open(tmp, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb") as handle:
        for row in rows:
            handle.write(archive_line(row))
    with open(tmp, "rb+") as synced:
        os.fsync(synced.fileno())
    tmp.replace(target)
    return target


def archive_bronze(conn: psycopg.Connection, settings: RetentionSettings, cutoff: datetime, dry_run: bool,
                   summary: RetentionSummary) -> None:
    if dry_run:
        row = conn.execute(
            "SELECT count(*) FROM raw_payment_events WHERE payload_archived_at IS NULL AND ingestion_time < %s",
            (cutoff,),
        ).fetchone()
        summary.bronze_archived = int(row[0]) if row else 0
        return
    for _ in range(settings.max_batches):
        with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                """
                SELECT raw_event_id, source_system, source_topic, source_partition, source_offset, kafka_key,
                       ingestion_time, payload_bytes, payload_hash
                FROM raw_payment_events
                WHERE payload_archived_at IS NULL AND ingestion_time < %s
                ORDER BY raw_event_id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (cutoff, settings.batch_size),
            ).fetchall()
            if not rows:
                return
            path = write_archive(settings.archive_dir, cutoff.date(), rows)
            cur.execute(
                """
                UPDATE raw_payment_events
                SET payload_bytes = ''::bytea, payload_json = NULL, payload_archived_at = clock_timestamp(),
                    archive_uri = %s
                WHERE raw_event_id = ANY(%s)
                """,
                (path.as_uri(), [r["raw_event_id"] for r in rows]),
            )
        summary.bronze_archived += len(rows)
        summary.archive_files.append(str(path))


def purge_outbox(conn: psycopg.Connection, cutoff: datetime, dry_run: bool, summary: RetentionSummary) -> None:
    where = "published_at IS NOT NULL AND published_at < %s"
    with conn.transaction():
        if dry_run:
            row = conn.execute(f"SELECT count(*) FROM reconciliation_outbox WHERE {where}", (cutoff,)).fetchone()
            summary.outbox_deleted = int(row[0]) if row else 0
        else:
            summary.outbox_deleted = conn.execute(f"DELETE FROM reconciliation_outbox WHERE {where}", (cutoff,)).rowcount


def roll_history_partitions(conn: psycopg.Connection, settings: RetentionSettings, now: datetime, dry_run: bool,
                            summary: RetentionSummary) -> None:
    with conn.transaction():
        names = [
            r[0]
            for r in conn.execute(
                """
                SELECT c.relname FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = 'reconciliation_history' AND p.relnamespace = current_schema()::regnamespace
                """
            ).fetchall()
        ]
        expired = expired_partitions(names, history_drop_before(now, settings.history_months))
        summary.partitions_dropped = expired
        if dry_run:
            return
        row = conn.execute("SELECT ensure_history_partitions(now(), %s)", (settings.history_months_ahead,)).fetchone()
        summary.partitions_created = int(row[0]) if row else 0
        for name in expired:
            conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(name)))


def run_retention(conn: psycopg.Connection, settings: RetentionSettings, now: datetime | None = None,
                  dry_run: bool = False) -> RetentionSummary:
    now = now or datetime.now(UTC)
    bronze_cutoff = now - timedelta(days=settings.bronze_payload_days)
    summary = RetentionSummary(dry_run=dry_run, bronze_cutoff=bronze_cutoff.isoformat())
    roll_history_partitions(conn, settings, now, dry_run, summary)
    purge_outbox(conn, now - timedelta(days=settings.outbox_days), dry_run, summary)
    archive_bronze(conn, settings, bronze_cutoff, dry_run, summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Archive and purge data past its retention period")
    parser.add_argument("--dry-run", action="store_true", help="report what would change without changing it")
    args = parser.parse_args(argv)

    service = load(ServiceSettings)
    configure_logging(service.service_name or "retention", service.environment, service.log_level)
    settings = load(RetentionSettings)
    db = load(DatabaseSettings)
    with connect(db, autocommit=True, user=settings.db_user, password=settings.db_password.get_secret_value()) as conn:
        summary = run_retention(conn, settings, dry_run=args.dry_run)
    print(json.dumps(asdict(summary)))  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
