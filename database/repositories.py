"""SQL repositories. All statements are parameterized; identifiers come only from internal constants."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg import Cursor, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from processing.normalization import SilverRow
from processing.reconciliation_engine import (
    AppEvent,
    LedgerRecord,
    PaymentRequest,
    PaymentSnapshot,
    ProcessorRecord,
)
from processing.rules import ReconciliationRules
from processing.validation import EventMetadata

SILVER_TABLES = frozenset(
    {"application_payment_events", "processor_transactions", "payment_refunds", "payment_reversals", "billing_ledger_entries"}
)


class RuleVersionConflict(RuntimeError):
    pass


# ---------------------------------------------------------------- Bronze


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp: datetime | None
    headers: tuple[tuple[str, bytes | None], ...] = ()


def insert_raw_event(
    cur: Cursor,
    record: KafkaRecord,
    source_system: str,
    meta: EventMetadata,
    payload_text: str | None,
    payload_hash: str,
    processing_status: str,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> int | None:
    """Insert a raw event; returns None when this Kafka position was already ingested (redelivery).

    The JSONB copy is cast from the original text by PostgreSQL so numeric values keep full precision.
    """
    cur.execute(
        """
        INSERT INTO raw_payment_events (
            source_system, source_topic, source_partition, source_offset, kafka_key, kafka_timestamp,
            source_event_id, event_type, payment_id, event_time, schema_version,
            payload_bytes, payload_json, payload_hash, processing_status, error_code, validation_error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT raw_payment_events_source_position_uk DO NOTHING
        RETURNING raw_event_id
        """,
        (
            source_system,
            record.topic,
            record.partition,
            record.offset,
            record.key.decode("utf-8", "replace")[:200] if record.key else None,
            record.timestamp,
            meta.source_event_id,
            meta.event_type,
            meta.payment_id,
            meta.event_time,
            meta.schema_version,
            record.value or b"",
            payload_text,
            payload_hash,
            processing_status,
            error_code,
            error_detail,
        ),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def update_raw_status(
    cur: Cursor, raw_event_id: int, status: str, error_code: str | None = None, error_detail: str | None = None
) -> None:
    cur.execute(
        "UPDATE raw_payment_events SET processing_status = %s, error_code = %s, validation_error = %s "
        "WHERE raw_event_id = %s",
        (status, error_code, error_detail, raw_event_id),
    )


def insert_quarantine(
    cur: Cursor,
    raw_event_id: int | None,
    record: KafkaRecord,
    source_system: str,
    meta: EventMetadata,
    error_category: str,
    error_code: str,
    error_detail: str,
) -> int:
    cur.execute(
        """
        INSERT INTO quarantined_events (raw_event_id, source_topic, source_partition, source_offset, source_system,
            source_event_id, payment_id, error_category, error_code, error_detail, payload_bytes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT quarantined_events_source_position_uk DO NOTHING
        RETURNING quarantine_id
        """,
        (
            raw_event_id,
            record.topic,
            record.partition,
            record.offset,
            source_system,
            meta.source_event_id,
            meta.payment_id,
            error_category,
            error_code,
            error_detail[:4000],
            record.value or b"",
        ),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute(
            "SELECT quarantine_id FROM quarantined_events WHERE source_topic = %s AND source_partition = %s "
            "AND source_offset = %s",
            (record.topic, record.partition, record.offset),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------- Silver


def insert_silver_row(cur: Cursor, row: SilverRow, raw_event_id: int) -> bool:
    if row.table not in SILVER_TABLES:
        raise ValueError(f"unknown silver table {row.table}")
    values = {**row.values, "raw_event_id": raw_event_id}
    columns = list(values)
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING RETURNING 1").format(
        sql.Identifier(row.table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    cur.execute(query, [values[c] for c in columns])
    return cur.fetchone() is not None


def upsert_payment_request(cur: Cursor, request: dict[str, Any]) -> None:
    """First app event creates the row; the first PAYMENT_INITIATED event is authoritative."""
    cur.execute(
        """
        INSERT INTO payment_requests (payment_id, account_ref, amount, currency, payment_method, initiated_at,
            initiated_event_id, first_seen_event_id, initiation_count)
        VALUES (%(payment_id)s, %(account_ref)s, %(amount)s, %(currency)s, %(payment_method)s, %(event_time)s,
            CASE WHEN %(is_initiation)s THEN %(source_event_id)s END, %(source_event_id)s,
            CASE WHEN %(is_initiation)s THEN 1 ELSE 0 END)
        ON CONFLICT (payment_id) DO UPDATE SET
            initiation_count = payment_requests.initiation_count + EXCLUDED.initiation_count,
            account_ref = CASE WHEN payment_requests.initiated_event_id IS NULL AND EXCLUDED.initiated_event_id IS NOT NULL
                THEN EXCLUDED.account_ref ELSE payment_requests.account_ref END,
            amount = CASE WHEN payment_requests.initiated_event_id IS NULL AND EXCLUDED.initiated_event_id IS NOT NULL
                THEN EXCLUDED.amount ELSE payment_requests.amount END,
            currency = CASE WHEN payment_requests.initiated_event_id IS NULL AND EXCLUDED.initiated_event_id IS NOT NULL
                THEN EXCLUDED.currency ELSE payment_requests.currency END,
            payment_method = CASE WHEN payment_requests.initiated_event_id IS NULL AND EXCLUDED.initiated_event_id IS NOT NULL
                THEN EXCLUDED.payment_method ELSE payment_requests.payment_method END,
            initiated_at = CASE WHEN payment_requests.initiated_event_id IS NULL AND EXCLUDED.initiated_event_id IS NOT NULL
                THEN EXCLUDED.initiated_at ELSE LEAST(payment_requests.initiated_at, EXCLUDED.initiated_at) END,
            initiated_event_id = COALESCE(payment_requests.initiated_event_id, EXCLUDED.initiated_event_id),
            updated_at = now()
        """,
        request,
    )


def enqueue_payments(cur: Cursor, payment_ids: Iterable[str], trigger_type: str) -> int:
    """Queue payments for (re-)evaluation. Sorted to give concurrent writers a consistent lock order."""
    ids = sorted(set(payment_ids))
    if not ids:
        return 0
    cur.execute(
        """
        INSERT INTO reconciliation_work_queue (payment_id, trigger_type)
        SELECT unnest(%s::text[]), %s
        ORDER BY 1
        ON CONFLICT (payment_id) DO UPDATE SET last_enqueued_at = clock_timestamp(),
            available_at = LEAST(reconciliation_work_queue.available_at, clock_timestamp())
        """,
        (ids, trigger_type),
    )
    return len(ids)


# ---------------------------------------------------------------- Snapshot


def load_snapshot(cur: Cursor, payment_id: str) -> PaymentSnapshot:
    cur.execute(
        "SELECT payment_id, account_ref, amount, currency, payment_method, initiated_at, initiated_event_id "
        "FROM payment_requests WHERE payment_id = %s",
        (payment_id,),
    )
    r = cur.fetchone()
    request = PaymentRequest(*r) if r else None

    cur.execute(
        "SELECT source_event_id, event_type, source_status, amount, currency, event_time "
        "FROM application_payment_events WHERE payment_id = %s ORDER BY event_time, source_event_id",
        (payment_id,),
    )
    app_events = tuple(AppEvent(*row) for row in cur.fetchall())

    cur.execute(
        """
        SELECT processor_transaction_id, payment_id, transaction_type, original_processor_transaction_id,
               amount, currency, event_time FROM processor_transactions WHERE payment_id = %(p)s
        UNION ALL
        SELECT refund_id, payment_id, 'REFUND', original_processor_transaction_id, amount, currency, event_time
        FROM payment_refunds WHERE payment_id = %(p)s
        UNION ALL
        SELECT reversal_id, payment_id, 'REVERSAL', original_processor_transaction_id, amount, currency, event_time
        FROM payment_reversals WHERE payment_id = %(p)s
        ORDER BY 7, 1
        """,
        {"p": payment_id},
    )
    processor = tuple(ProcessorRecord(*row) for row in cur.fetchall())

    cur.execute(
        """
        SELECT ledger_entry_id, payment_id, entry_type, direction, amount, currency, posting_status,
               original_ledger_entry_id, processor_reference, event_time
        FROM billing_ledger_entries WHERE payment_id = %s ORDER BY event_time, ledger_entry_id
        """,
        (payment_id,),
    )
    ledger = tuple(LedgerRecord(*row) for row in cur.fetchall())

    own = {p.transaction_id for p in processor}
    refs = sorted(
        {p.original_transaction_id for p in processor if p.original_transaction_id and p.original_transaction_id not in own}
    )
    foreign: tuple[ProcessorRecord, ...] = ()
    if refs:
        cur.execute(
            """
            SELECT processor_transaction_id, payment_id, transaction_type, original_processor_transaction_id,
                   amount, currency, event_time
            FROM processor_transactions WHERE processor_transaction_id = ANY(%s) AND payment_id <> %s
            """,
            (refs, payment_id),
        )
        foreign = tuple(ProcessorRecord(*row) for row in cur.fetchall())

    return PaymentSnapshot(payment_id, request, app_events, processor, ledger, foreign)


# ---------------------------------------------------------------- Rules


def register_rule_version(conn: psycopg.Connection, rules: ReconciliationRules) -> None:
    row = conn.execute(
        "SELECT config_hash FROM reconciliation_rule_versions WHERE rule_version = %s", (rules.rule_version,)
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO reconciliation_rule_versions (rule_version, config_hash, config_json, description) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (rule_version) DO NOTHING",
            (rules.rule_version, rules.config_hash, Jsonb(rules.raw), rules.description),
        )
        row = conn.execute(
            "SELECT config_hash FROM reconciliation_rule_versions WHERE rule_version = %s", (rules.rule_version,)
        ).fetchone()
    if row is None or row[0] != rules.config_hash:
        raise RuleVersionConflict(
            f"rule_version {rules.rule_version} is registered with different content; bump rule_version"
        )


# ---------------------------------------------------------------- Cases


def lock_case(cur: Cursor, payment_id: str) -> dict[str, Any] | None:
    with cur.connection.cursor(row_factory=dict_row) as dcur:
        dcur.execute("SELECT * FROM reconciliation_cases WHERE payment_id = %s FOR UPDATE", (payment_id,))
        return dcur.fetchone()


def insert_case_placeholder(
    cur: Cursor, payment_id: str, business_date: Any, reference_time: datetime, rule_version: str
) -> None:
    cur.execute(
        """
        INSERT INTO reconciliation_cases (payment_id, business_date, reconciliation_status, computed_status, reason_code,
            processor_outcome, reference_time, rule_version, evaluation_count)
        VALUES (%s, %s, 'PENDING_MATCH', 'PENDING_MATCH', 'NEW', 'NONE', %s, %s, 0)
        ON CONFLICT (payment_id) DO NOTHING
        """,
        (payment_id, business_date, reference_time, rule_version),
    )


def update_case(cur: Cursor, case_id: Any, values: dict[str, Any]) -> None:
    columns = list(values)
    query = sql.SQL(
        "UPDATE reconciliation_cases SET {}, version = version + 1, updated_at = clock_timestamp() "
        "WHERE reconciliation_case_id = {}"
    ).format(
        sql.SQL(", ").join(sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder()) for c in columns),
        sql.Placeholder(),
    )
    cur.execute(query, [*(_adapt(values[c]) for c in columns), case_id])


def _adapt(value: Any) -> Any:
    if isinstance(value, dict | list):
        return Jsonb(value)
    return value


def insert_history(cur: Cursor, values: dict[str, Any]) -> None:
    columns = list(values)
    query = sql.SQL("INSERT INTO reconciliation_history ({}) VALUES ({})").format(
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    cur.execute(query, [_adapt(values[c]) for c in columns])


def insert_outbox(cur: Cursor, case_id: Any, payment_id: str, event_type: str, payload: dict[str, Any]) -> None:
    cur.execute(
        "INSERT INTO reconciliation_outbox (reconciliation_case_id, payment_id, event_type, payload) "
        "VALUES (%s, %s, %s, %s)",
        (case_id, payment_id, event_type, Jsonb(payload)),
    )


def insert_admin_action(
    cur: Cursor, action_type: str, actor: str, reason: str, target: str, details: dict[str, Any]
) -> str:
    cur.execute(
        "INSERT INTO admin_actions (action_type, actor, reason, target, details) VALUES (%s, %s, %s, %s, %s) "
        "RETURNING action_id",
        (action_type, actor, reason, target, Jsonb(json.loads(json.dumps(details, default=str)))),
    )
    row = cur.fetchone()
    assert row is not None
    return str(row[0])


def fetch_dicts(cur: Cursor, query: str, params: Sequence[Any] | dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with cur.connection.cursor(row_factory=dict_row) as dcur:
        dcur.execute(query, params)
        return list(dcur.fetchall())
