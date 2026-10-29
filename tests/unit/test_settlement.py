"""Settlement files: generator, adapter (file-level integrity, publishing) and the engine's settlement dimension."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import jsonschema
import pytest

from common.config import REPO_ROOT, SimulationSettings, TopicSettings
from generators.settlement_files import CHARGEBACK_FEE, fee_for, render_file, settlement_lines, write_settlement_file
from ingestion.settlement_adapter import FileRejected, SettlementAdapter, parse_settlement_file
from processing.models import SettlementEvent
from processing.reconciliation_engine import evaluate
from tests.factories import NOW, D, build_snapshot, ledger, matched_payment, proc, settlement

TZ = ZoneInfo("America/Toronto")
TOPICS = TopicSettings()
SCHEMA = jsonschema.Draft202012Validator(json.loads((REPO_ROOT / "schemas" / "settlement_record.json").read_text()))
P = "PAY-T-000001"
START = datetime(2026, 10, 28, 14, 0, tzinfo=UTC)


def sim(**kw) -> SimulationSettings:
    return SimulationSettings(epoch=START - timedelta(days=1), payments_per_second=2.0, **kw)


def run(payloads, rules, now=NOW):
    return evaluate(build_snapshot(P, payloads), now, rules)


# ---------------------------------------------------------------- generator + adapter


def test_generated_file_round_trips_through_adapter_into_valid_events(tmp_path):
    s = sim(seed=21, chargeback_rate=0.2, refund_rate=0.2)
    path = write_settlement_file(s, TOPICS, TZ, START, START + timedelta(minutes=5), tmp_path)
    assert path is not None and path.suffix == ".csv"
    events = parse_settlement_file(path.read_text(encoding="utf-8"), TZ)
    assert len(events) > 400
    types = set()
    for event in events:
        SCHEMA.validate(event)
        parsed = SettlementEvent.model_validate(event)
        types.add(parsed.transaction_type.value)
        assert parsed.event_time.astimezone(TZ).time() == datetime.min.time()
    assert {"CAPTURE", "REFUND", "CHARGEBACK"} <= types
    again = write_settlement_file(s, TOPICS, TZ, START, START + timedelta(minutes=5), tmp_path / "again")
    assert again is not None and again.read_bytes() == path.read_bytes()  # deterministic


def test_settlement_reflects_processor_truth_and_fees():
    s = sim(seed=4, decline_rate=0.0, settlement_missing_rate=0.0, settlement_amount_mismatch_rate=0.0,
            refund_rate=0.0, reversal_rate=0.0, chargeback_rate=0.0)
    lines = settlement_lines(s, TOPICS, 1000)
    assert [line["transaction_type"] for line in lines] == ["CAPTURE"]
    line = lines[0]
    assert line["net_amount"] == line["gross_amount"] - line["fee_amount"]
    assert fee_for("CREDIT_CARD", "CAPTURE", D("100.00")) == D("2.55")
    assert fee_for("PRE_AUTHORIZED_DEBIT", "CAPTURE", D("100.00")) == D("0.25")
    assert fee_for("CREDIT_CARD", "REFUND", D("100.00")) == D("0.00")
    assert fee_for("DEBIT_CARD", "CHARGEBACK", D("100.00")) == CHARGEBACK_FEE


def _file(rows: list[list[str]], trailer: list[str] | None = None) -> str:
    rows_text = render_file([], "STB-X", TZ).splitlines()[0] + "\n"
    for row in rows:
        rows_text += ",".join(row) + "\n"
    return rows_text + ",".join(trailer or ["TRAILER", str(len(rows)), "0.00"]) + "\n"


LINE = ["2026-10-28", "STB-X", "STL-C-1", "C-1", P, "CAPTURE", "100.00", "2.55", "97.45", "CAD"]


@pytest.mark.parametrize(("text", "code"), [
    ("wrong,header\nTRAILER,0,0.00\n", "INVALID_HEADER"),
    (_file([LINE]).rsplit("TRAILER", 1)[0], "MISSING_TRAILER"),
    (_file([LINE[:-1]], ["TRAILER", "1", "97.45"]), "WRONG_COLUMN_COUNT"),
    (_file([LINE], ["TRAILER", "2", "97.45"]), "CONTROL_COUNT_MISMATCH"),
    (_file([LINE], ["TRAILER", "1", "97.40"]), "CONTROL_TOTAL_MISMATCH"),
    (_file([LINE[:8] + ["abc", "CAD"]], ["TRAILER", "1", "0"]), "UNPARSABLE_CONTROL_VALUE"),
])
def test_file_level_integrity_is_enforced(text, code):
    with pytest.raises(FileRejected) as exc:
        parse_settlement_file(text, TZ)
    assert exc.value.code == code


def test_line_level_problems_are_left_to_ingestion_validation():
    bad = LINE[:6] + ["100.00", "2.55", "97.40", "CAD"]  # net != gross - fee, but control total matches
    [event] = parse_settlement_file(_file([bad], ["TRAILER", "1", "97.40"]), TZ)
    with pytest.raises(ValueError, match="net_amount"):
        SettlementEvent.model_validate(event)


class FakeProducer:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[tuple[str, bytes | None, bytes]] = []
        self._callbacks: list = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.messages.append((topic, key, value))
        self._callbacks.append(on_delivery)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        for cb in self._callbacks:
            cb("BROKER_DOWN" if self.fail else None, None)
        self._callbacks.clear()
        return 0


def _inbox(tmp_path: Path, text: str) -> Path:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "settlement_a.csv").write_text(text, encoding="utf-8")
    (inbox / "settlement_b.csv.tmp").write_text("partial", encoding="utf-8")  # incomplete upload is ignored
    return inbox


def test_adapter_publishes_then_moves_file(tmp_path):
    inbox = _inbox(tmp_path, _file([LINE], ["TRAILER", "1", "97.45"]))
    producer = FakeProducer()
    adapter = SettlementAdapter(inbox, TOPICS, producer, TZ)
    assert adapter.run_once() == 1
    [(topic, key, value)] = producer.messages
    assert topic == TOPICS.settlement_events and key == P.encode()
    assert json.loads(value)["event_id"] == "STL-EVT-STL-C-1"
    assert not list(inbox.glob("*.csv")) and len(list((tmp_path / "processed").glob("*.csv"))) == 1


def test_adapter_keeps_file_when_publishing_fails_and_rejects_corrupt_files(tmp_path):
    inbox = _inbox(tmp_path, _file([LINE], ["TRAILER", "1", "97.45"]))
    assert SettlementAdapter(inbox, TOPICS, FakeProducer(fail=True), TZ).process_file(inbox / "settlement_a.csv") == "retry"
    assert (inbox / "settlement_a.csv").exists()

    (inbox / "settlement_a.csv").write_text(_file([LINE], ["TRAILER", "9", "97.45"]), encoding="utf-8")
    producer = FakeProducer()
    assert SettlementAdapter(inbox, TOPICS, producer, TZ).process_file(inbox / "settlement_a.csv") == "rejected"
    assert producer.messages == []
    [error] = (tmp_path / "rejected").glob("*.error.txt")
    assert error.read_text(encoding="utf-8").startswith("CONTROL_COUNT_MISMATCH")


# ---------------------------------------------------------------- engine settlement dimension


def test_settled_payment_reports_fees_and_net(rules):
    r = run(matched_payment(P) + [settlement(P, "C-T-000001", gross="100.00", fee="2.55")], rules)
    assert r.status.value == "MATCHED"
    assert (r.settlement.status, r.settlement.fees, r.settlement.settled_net) == ("SETTLED", D("2.55"), D("97.45"))
    assert r.amounts()["settlement_status"] == "SETTLED"


def test_settlement_pending_then_unsettled(rules):
    t0 = NOW - timedelta(minutes=10)
    events = matched_payment(P, t0=t0)
    pending = run(events, rules)
    assert pending.status.value == "MATCHED" and pending.settlement.status == "PENDING"
    capture_time = t0 + timedelta(seconds=2)
    assert pending.next_evaluation_at == capture_time + rules.settlement_grace
    late = run(events, rules, now=capture_time + rules.settlement_grace)
    assert (late.settlement.status, late.settlement.reason, late.settlement.difference) == (
        "UNSETTLED", "SETTLEMENT_MISSING", D("-100.00"))


@pytest.mark.parametrize(("extra", "reason", "difference"), [
    ([settlement(P, "C-T-000001", gross="99.00", fee="2.53")], "SETTLEMENT_AMOUNT_MISMATCH", D("-1.00")),
    ([settlement(P, "C-T-000001"), settlement(P, "C-T-000001", settlement_id="STL-DUP")], "DUPLICATE_SETTLEMENT",
     D("100.00")),
    ([settlement(P, "C-T-000001"), settlement(P, "C-OTHER", fee="0.00")], "SETTLED_TRANSACTION_UNKNOWN", D("100.00")),
    ([settlement(P, "C-T-000001"), settlement(P, "A-T-000001", fee="0.00")], "UNEXPECTED_SETTLEMENT", D("100.00")),
    ([settlement(P, "C-T-000001", transaction_type="CHARGEBACK_REVERSAL", fee="0.00")], "SETTLEMENT_TYPE_MISMATCH",
     D("0.00")),
])
def test_settlement_mismatches(rules, extra, reason, difference):
    r = run(matched_payment(P) + extra, rules)
    assert r.status.value == "MATCHED"  # cash reconciliation is reported alongside, not instead of, matching
    assert (r.settlement.status, r.settlement.reason, r.settlement.difference) == ("SETTLEMENT_MISMATCH", reason,
                                                                                   difference)


def test_refund_and_chargeback_settlement_lines(rules):
    events = matched_payment(P, "100.00") + [
        proc(P, "REFUND", "R-1", "40.00", NOW - timedelta(hours=1, minutes=30), original="C-T-000001"),
        ledger(P, "REFUND_DEBIT", "LR-1", "40.00", NOW - timedelta(hours=1, minutes=29), processor_reference="R-1"),
        settlement(P, "C-T-000001", fee="2.55"),
        settlement(P, "R-1", transaction_type="REFUND", gross="40.00", fee="0.00"),
    ]
    r = run(events, rules)
    assert r.reason_code == "PARTIALLY_REFUNDED"
    assert (r.settlement.status, r.settlement.settled_net) == ("SETTLED", D("57.45"))


def test_settlement_line_alone_opens_a_case(rules):
    r = run([settlement(P, "C-T-000001")], rules)
    assert r is not None and r.reason_code in ("AWAITING_APPLICATION_RECORD", "APPLICATION_RECORD_MISSING")
    assert r.settlement.status == "SETTLEMENT_MISMATCH" and r.settlement.reason == "SETTLED_TRANSACTION_UNKNOWN"
