"""Administrative CLI for controlled, audited corrections.

There is deliberately no command that sets a reconciliation status or amount directly. Operators can
only (a) re-run the engine, (b) place/release a review hold, (c) replay or discard quarantined events
through Kafka (so all normal validation and idempotency protections apply), and (d) re-queue open
cases. Each action requires an allow-listed actor and a written reason and is recorded in the
append-only admin_actions and reconciliation_history tables.

Local authorization uses ADMIN_ALLOWED_ACTORS; production must use real identity (SSO/IAM) and a
separate privileged database role (see docs/security.md).

Examples:
    python -m admin.cli reprocess-case --payment-id PAY-local-000000000123 --actor ops.alice --reason "Re-check after fix"
    python -m admin.cli hold --payment-id PAY-... --actor ops.alice --reason "Customer dispute under investigation"
    python -m admin.cli replay-quarantine --quarantine-id 42 --payload-file fixed.json \
        --actor ops.alice --reason "Corrected currency"
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from confluent_kafka import Producer

from common.config import (
    AdminSettings,
    BusinessSettings,
    DatabaseSettings,
    KafkaSettings,
    ReconciliationSettings,
    TopicSettings,
    load,
)
from common.kafka_config import producer_config
from database import repositories as repo
from database.connection import connect
from processing.case_manager import ReconcileOutcome, reconcile_payment
from processing.rules import ReconciliationRules, load_rules
from processing.validation import ValidationFailure, decode_payload, validate_event

MIN_REASON = 10


class AuthorizationError(PermissionError):
    pass


def authorize(actor: str, reason: str, allowed: frozenset[str]) -> None:
    if not allowed:
        raise AuthorizationError("no administrators configured (ADMIN_ALLOWED_ACTORS is empty)")
    if actor not in allowed:
        raise AuthorizationError(f"actor {actor!r} is not authorized for administrative actions")
    if len(reason.strip()) < MIN_REASON:
        raise AuthorizationError(f"a reason of at least {MIN_REASON} characters is required")


def reprocess_case(
    conn: psycopg.Connection, rules: ReconciliationRules, tz: ZoneInfo, payment_id: str, actor: str, reason: str,
    allowed: frozenset[str],
) -> ReconcileOutcome:
    authorize(actor, reason, allowed)
    with conn.transaction(), conn.cursor() as cur:
        details = {"rule_version": rules.rule_version}
        action_id = repo.insert_admin_action(cur, "REPROCESS_CASE", actor, reason, payment_id, details)
        return reconcile_payment(
            conn, payment_id, rules, tz, "ADMIN_REPROCESS", run_id=action_id, actor=actor, note=reason, force_history=True
        )


def set_review_hold(
    conn: psycopg.Connection, rules: ReconciliationRules, tz: ZoneInfo, payment_id: str, hold: bool, actor: str,
    reason: str, allowed: frozenset[str],
) -> ReconcileOutcome:
    authorize(actor, reason, allowed)
    action = "PLACE_REVIEW_HOLD" if hold else "RELEASE_REVIEW_HOLD"
    trigger = "ADMIN_REVIEW_HOLD" if hold else "ADMIN_REVIEW_RELEASE"
    with conn.transaction(), conn.cursor() as cur:
        if repo.lock_case(cur, payment_id) is None:
            raise LookupError(f"no reconciliation case for payment {payment_id}")
        action_id = repo.insert_admin_action(cur, action, actor, reason, payment_id, {})
        return reconcile_payment(
            conn, payment_id, rules, tz, trigger, run_id=action_id, actor=actor, note=reason, force_history=True,
            review_hold_override=hold,
        )


def replay_quarantine(
    conn: psycopg.Connection,
    producer: Any,
    topics: TopicSettings,
    rules: ReconciliationRules,
    quarantine_id: int,
    corrected_payload: bytes | None,
    actor: str,
    reason: str,
    allowed: frozenset[str],
) -> dict[str, Any]:
    """Re-publish a quarantined event (optionally corrected) to its original source topic."""
    authorize(actor, reason, allowed)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT source_topic, source_system, payload_bytes, status FROM quarantined_events "
            "WHERE quarantine_id = %s FOR UPDATE",
            (quarantine_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise LookupError(f"quarantine record {quarantine_id} not found")
        topic, source_system, original, status = row
        if status != "OPEN":
            raise ValueError(f"quarantine record {quarantine_id} is already {status}")
        if topic not in topics.source_topics:
            raise ValueError(f"cannot replay to non-source topic {topic}")
        value = corrected_payload if corrected_payload is not None else bytes(original)
        # Fail fast: refuse to replay a payload that would be quarantined again.
        verdict = validate_event(decode_payload(value), source_system, datetime.now(UTC), rules.timestamp_rules)
        if isinstance(verdict, ValidationFailure):
            raise ValueError(f"replay payload is still invalid: {verdict.error_code}: {verdict.detail}")

        errors: list[str] = []
        key = getattr(verdict, "payment_id", None) or getattr(verdict, "account_ref", None)
        producer.produce(
            topic,
            key=key.encode() if key else None,
            value=value,
            headers=[("x-replay-of-quarantine-id", str(quarantine_id).encode()), ("x-replayed-by", actor.encode())],
            on_delivery=lambda err, _m: errors.append(str(err)) if err else None,
        )
        if producer.flush(30) > 0 or errors:
            raise RuntimeError(f"replay publish was not acknowledged: {errors or 'timeout'}")
        action_id = repo.insert_admin_action(
            cur, "REPLAY_QUARANTINE", actor, reason, str(quarantine_id),
            {"topic": topic, "corrected": corrected_payload is not None, "event_id": verdict.event_id},
        )
        cur.execute(
            "UPDATE quarantined_events SET status = 'REPLAYED', resolved_at = clock_timestamp(), resolved_by = %s, "
            "resolution_note = %s WHERE quarantine_id = %s",
            (actor, f"{reason} (admin_action {action_id})", quarantine_id),
        )
    return {"quarantine_id": quarantine_id, "topic": topic, "event_id": verdict.event_id, "action_id": action_id}


def discard_quarantine(
    conn: psycopg.Connection, quarantine_id: int, actor: str, reason: str, allowed: frozenset[str]
) -> str:
    authorize(actor, reason, allowed)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE quarantined_events SET status = 'DISCARDED', resolved_at = clock_timestamp(), resolved_by = %s, "
            "resolution_note = %s WHERE quarantine_id = %s AND status = 'OPEN' RETURNING quarantine_id",
            (actor, reason, quarantine_id),
        )
        if cur.fetchone() is None:
            raise LookupError(f"open quarantine record {quarantine_id} not found")
        return repo.insert_admin_action(cur, "DISCARD_QUARANTINE", actor, reason, str(quarantine_id), {})


def reevaluate_open_cases(conn: psycopg.Connection, actor: str, reason: str, allowed: frozenset[str]) -> int:
    """Queue every non-resolved case for re-evaluation (e.g. after deploying a new rule version)."""
    authorize(actor, reason, allowed)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT payment_id FROM reconciliation_cases "
            "WHERE reconciliation_status NOT IN ('MATCHED', 'NO_FINANCIAL_EFFECT') ORDER BY payment_id"
        )
        ids = [r[0] for r in cur.fetchall()]
        repo.insert_admin_action(cur, "REEVALUATE_OPEN_CASES", actor, reason, "open-cases", {"count": len(ids)})
        return repo.enqueue_payments(cur, ids, "ADMIN_REPROCESS")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audited administrative operations")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("reprocess-case", "hold", "release"):
        p = sub.add_parser(name)
        p.add_argument("--payment-id", required=True)
    p = sub.add_parser("replay-quarantine")
    p.add_argument("--quarantine-id", type=int, required=True)
    p.add_argument("--payload-file", type=Path)
    p = sub.add_parser("discard-quarantine")
    p.add_argument("--quarantine-id", type=int, required=True)
    sub.add_parser("reevaluate-open")
    for p in sub.choices.values():
        p.add_argument("--actor", required=True)
        p.add_argument("--reason", required=True)
    args = parser.parse_args(argv)

    allowed = load(AdminSettings).actors
    rules = load_rules(load(ReconciliationSettings).rules_path)
    tz = load(BusinessSettings).tz
    with connect(load(DatabaseSettings), autocommit=True) as conn:
        result: Any
        if args.command == "reprocess-case":
            result = reprocess_case(conn, rules, tz, args.payment_id, args.actor, args.reason, allowed).__dict__
        elif args.command in ("hold", "release"):
            result = set_review_hold(
                conn, rules, tz, args.payment_id, args.command == "hold", args.actor, args.reason, allowed
            ).__dict__
        elif args.command == "replay-quarantine":
            producer = Producer(producer_config(load(KafkaSettings), "admin-replay", linger_ms=0))
            payload = args.payload_file.read_bytes() if args.payload_file else None
            result = replay_quarantine(
                conn, producer, load(TopicSettings), rules, args.quarantine_id, payload, args.actor, args.reason, allowed
            )
        elif args.command == "discard-quarantine":
            result = {"action_id": discard_quarantine(conn, args.quarantine_id, args.actor, args.reason, allowed)}
        else:
            result = {"queued": reevaluate_open_cases(conn, args.actor, args.reason, allowed)}
    print(json.dumps(result, indent=2, default=str))  # noqa: T201 - CLI output
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
