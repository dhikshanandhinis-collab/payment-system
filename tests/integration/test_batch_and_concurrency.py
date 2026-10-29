"""Batch reconciliation: rerun idempotency, failure + safe rerun, exclusivity, concurrency and admin audit."""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta

import pytest

from admin.cli import AuthorizationError, reprocess_case, set_review_hold
from batch import control_totals
from batch.daily_reconciliation import RunInProgressError, run_business_date
from common.config import TopicSettings
from database.connection import connect
from ingestion.event_writer import EventWriter
from processing.case_manager import business_date, reconcile_payment
from tests.factories import app, ledger, matched_payment, proc
from tests.harness import TZ
from tests.integration.test_database_ingestion import rec

T0 = (datetime.now(UTC) - timedelta(hours=3)).replace(microsecond=0)
DAY = business_date(T0, TZ)
ALLOWED = frozenset({"ops.alice"})


def ingest(database, rules, payloads):
    writer = EventWriter(TopicSettings(), rules)
    with database.transaction() as conn:
        writer.process_batch(conn, [rec(p) for p in payloads])


@pytest.fixture(scope="module")
def dataset(database, rules):
    payloads = []
    for i in range(1, 21):
        payloads += matched_payment(f"PAY-BATCH-{i:04d}", f"{i}.00", T0 + timedelta(seconds=i))
    payloads += matched_payment("PAY-BATCH-0100", "10.00", T0)[:-1]  # missing billing posting
    payloads += [app("PAY-BATCH-0101", t=T0), proc("PAY-BATCH-0101", "DECLINE", "D-BATCH-0101", t=T0)]
    ingest(database, rules, payloads)
    return payloads


def totals(database):
    with database.transaction() as conn:
        return sorted(conn.execute(
            "SELECT source_system, metric_name, currency, record_count, amount FROM source_control_totals "
            "WHERE business_date = %s", (DAY,)
        ).fetchall())


def counts(database):
    with database.transaction() as conn:
        return conn.execute(
            "SELECT (SELECT count(*) FROM reconciliation_cases), (SELECT count(*) FROM reconciliation_history)"
        ).fetchone()


def test_batch_run_reconciles_and_records_controls(db_settings, database, rules, dataset):
    with connect(db_settings) as conn:
        summary = run_business_date(conn, rules, TZ, DAY, "DAILY", "test")
    assert summary.status == "SUCCEEDED" and summary.payments_evaluated == 22
    assert summary.status_counts == {"MATCHED": 20, "MISSING_BILLING_POSTING": 1, "NO_FINANCIAL_EFFECT": 1}
    t = {(s, m): (n, a) for s, m, _, n, a in totals(database)}
    assert t[("payment_processor", "CAPTURES")] == (21, sum(range(1, 21)) + 10)
    assert t[("billing_ledger", "PAYMENT_CREDITS_EFFECTIVE")] == (20, sum(range(1, 21)))
    assert t[("mybell_app", "PAYMENT_REQUESTS")][0] == 22


def test_rerun_same_date_is_idempotent(db_settings, database, rules, dataset):
    before_totals, before_counts = totals(database), counts(database)
    with connect(db_settings) as conn:
        summary = run_business_date(conn, rules, TZ, DAY, "DAILY", "test-rerun")
    assert summary.status == "SUCCEEDED" and summary.status_changes == 0
    assert totals(database) == before_totals
    assert counts(database) == before_counts  # no new cases, no new history rows


def test_failed_run_then_safe_rerun(db_settings, database, rules, dataset, monkeypatch):
    before = totals(database)

    def explode(*_args, **_kwargs):
        raise RuntimeError("simulated failure while computing control totals")

    monkeypatch.setattr(control_totals, "compute_control_totals", explode)
    with connect(db_settings) as conn, pytest.raises(RuntimeError):
        run_business_date(conn, rules, TZ, DAY, "DAILY", "test-fail")
    assert totals(database) == before  # failed run did not partially replace totals
    with database.transaction() as conn:
        failed = conn.execute(
            "SELECT status, error_message FROM reconciliation_run_history WHERE triggered_by = 'test-fail'"
        ).fetchone()
    assert failed[0] == "FAILED" and "simulated failure" in failed[1]
    monkeypatch.undo()
    with connect(db_settings) as conn:
        assert run_business_date(conn, rules, TZ, DAY, "DAILY", "test-after-fail").status == "SUCCEEDED"
    assert totals(database) == before


def test_concurrent_run_for_same_date_is_rejected(db_settings, rules, dataset):
    with connect(db_settings, autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(4242, %s)", (int(DAY.strftime("%Y%m%d")),))
        with connect(db_settings) as conn, pytest.raises(RunInProgressError):
            run_business_date(conn, rules, TZ, DAY, "DAILY", "test-concurrent")


def test_quality_results_stored(database, dataset):
    with database.transaction() as conn:
        checks = dict(conn.execute(
            "SELECT check_name, passed FROM data_quality_results d JOIN reconciliation_run_history r USING (run_id) "
            "WHERE r.triggered_by = 'test'"
        ).fetchall())
    assert set(checks) == {"quarantine_rate", "duplicate_delivery_rate", "late_event_rate", "open_exception_rate",
                           "processor_completeness", "ledger_completeness"}


def test_concurrent_evaluations_of_same_payment_are_serialized(database, rules):
    """Scenario 17: many workers + late events for one payment never create conflicting outcomes."""
    p = "PAY-CONC-0001"
    events = matched_payment(p, "50.00", T0)
    ingest(database, rules, events[:3])
    errors: list[Exception] = []

    def evaluate_repeatedly():
        try:
            for _ in range(5):
                with database.transaction() as conn:
                    reconcile_payment(conn, p, rules, TZ, "STREAM")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=evaluate_repeatedly) for _ in range(6)]
    for t in threads:
        t.start()
    ingest(database, rules, events[3:])  # counterpart arrives while evaluations are running
    for t in threads:
        t.join()
    with database.transaction() as conn:
        reconcile_payment(conn, p, rules, TZ, "STREAM")
        n_cases = conn.execute("SELECT count(*) FROM reconciliation_cases WHERE payment_id = %s", (p,)).fetchone()[0]
        status, billing = conn.execute(
            "SELECT reconciliation_status, billing_amount FROM reconciliation_cases WHERE payment_id = %s", (p,)
        ).fetchone()
        history = conn.execute(
            "SELECT previous_status, new_status FROM reconciliation_history WHERE payment_id = %s ORDER BY history_id",
            (p,),
        ).fetchall()
    assert not errors and n_cases == 1 and status == "MATCHED" and str(billing) == "50.00"
    assert history[0][0] is None
    for prev, nxt in zip(history, history[1:], strict=False):
        assert nxt[0] == prev[1]  # unbroken audit chain: each entry starts where the previous ended


def test_admin_reprocess_and_review_hold_are_audited(database, rules, dataset):
    p = "PAY-BATCH-0100"
    with database.transaction() as conn:
        reconcile_payment(conn, p, rules, TZ, "STREAM")
    with database.pool.connection() as conn:
        conn.autocommit = True
        with pytest.raises(AuthorizationError):
            reprocess_case(conn, rules, TZ, p, "mallory", "Trying to tamper with outcome", ALLOWED)
        held = set_review_hold(conn, rules, TZ, p, True, "ops.alice", "Customer dispute opened", ALLOWED)
        assert held.new_status == "UNDER_REVIEW"
        ingest(database, rules, [ledger(p, "PAYMENT_CREDIT", "L-BATCH-0100", "10.00", T0 + timedelta(seconds=30))])
        with conn.transaction():
            reconcile_payment(conn, p, rules, TZ, "STREAM")
        still = conn.execute(
            "SELECT reconciliation_status, computed_status FROM reconciliation_cases WHERE payment_id = %s", (p,)
        ).fetchone()
        assert still == ("UNDER_REVIEW", "MATCHED")  # engine result recorded, hold respected
        released = set_review_hold(conn, rules, TZ, p, False, "ops.alice", "Dispute closed, release", ALLOWED)
        assert released.new_status == "MATCHED"
        reprocess_case(conn, rules, TZ, p, "ops.alice", "Verify after release", ALLOWED)
        audit = conn.execute(
            "SELECT trigger_type, actor FROM reconciliation_history WHERE payment_id = %s AND actor <> 'system' "
            "ORDER BY history_id", (p,)
        ).fetchall()
        actions = conn.execute("SELECT action_type FROM admin_actions WHERE target = %s ORDER BY created_at", (p,)).fetchall()
        conn.autocommit = False
    assert audit == [("ADMIN_REVIEW_HOLD", "ops.alice"), ("ADMIN_REVIEW_RELEASE", "ops.alice"),
                     ("ADMIN_REPROCESS", "ops.alice")]
    assert [a[0] for a in actions] == ["PLACE_REVIEW_HOLD", "RELEASE_REVIEW_HOLD", "REPROCESS_CASE"]
