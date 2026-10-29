"""Idempotency keys for duplicate-delivery and duplicate-business-event detection.

Two keys are registered per valid event:
  * EVENT_ID      - the source event identifier (detects re-delivery of the same event).
  * BUSINESS_KEY  - processor_transaction_id / ledger_entry_id (detects a source re-sending the same
                    financial transaction under a new event_id, e.g. webhook redelivery).
If a key already exists with a different content hash the event is a conflicting duplicate and is
quarantined rather than silently accepted or dropped.
"""

from __future__ import annotations

from enum import StrEnum

from psycopg import Cursor


class DedupOutcome(StrEnum):
    NEW = "NEW"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"


class KeyType(StrEnum):
    EVENT_ID = "EVENT_ID"
    BUSINESS_KEY = "BUSINESS_KEY"


def register_key(
    cur: Cursor, source_system: str, key_type: KeyType, key_value: str, content_hash: str, raw_event_id: int
) -> DedupOutcome:
    cur.execute(
        """
        INSERT INTO deduplication_keys (source_system, key_type, key_value, content_hash, first_raw_event_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source_system, key_type, key_value) DO NOTHING
        RETURNING key_value
        """,
        (source_system, key_type.value, key_value, content_hash, raw_event_id),
    )
    if cur.fetchone() is not None:
        return DedupOutcome.NEW
    cur.execute(
        "SELECT content_hash FROM deduplication_keys WHERE source_system = %s AND key_type = %s AND key_value = %s",
        (source_system, key_type.value, key_value),
    )
    row = cur.fetchone()
    if row is not None and row[0] == content_hash:
        return DedupOutcome.DUPLICATE
    return DedupOutcome.CONFLICT
