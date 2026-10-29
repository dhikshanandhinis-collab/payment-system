"""Avro + schema registry: wire format, contract parity with JSON, ingestion decoding and producer encoding."""

from __future__ import annotations

import json
import struct
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from common.config import ConfigurationError, SchemaRegistrySettings, SimulationSettings, TopicSettings, load
from common.errors import TransientError
from common.schema_registry import AvroDecoder, UnknownSchemaError, schemas_by_topic, serializers_for, subject_for
from database.repositories import KafkaRecord
from generators.runner import SourceProducer
from generators.settlement_files import write_settlement_file
from generators.simulation import PROCESSOR, ScheduledEvent, build_all_events
from ingestion.event_writer import EventWriter, _Quarantine
from ingestion.settlement_adapter import parse_settlement_file
from processing.models import MODEL_BY_SOURCE
from scripts.register_schemas import IncompatibleSchemaError, register_all

TOPICS = TopicSettings()
START = datetime(2026, 10, 28, 14, 0, tzinfo=UTC)
TZ = ZoneInfo("America/Toronto")


class FakeRegistry:
    """In-memory stand-in for the registry REST API (ids, subjects, BACKWARD compatibility by field defaults)."""

    def __init__(self) -> None:
        self.schemas: dict[int, str] = {}
        self.subjects: dict[str, list[int]] = {}
        self.compatibility: dict[str, str] = {}
        self.unavailable = False

    def register(self, subject: str, schema: str) -> int:
        for schema_id, text in self.schemas.items():
            if text == schema:
                self.subjects.setdefault(subject, []).append(schema_id)
                return schema_id
        schema_id = len(self.schemas) + 1
        self.schemas[schema_id] = schema
        self.subjects.setdefault(subject, []).append(schema_id)
        return schema_id

    def latest_id(self, subject: str) -> int:
        return self.subjects[subject][-1]

    def get_schema(self, schema_id: int) -> str:
        if self.unavailable:
            raise TransientError("schema registry unreachable")
        if schema_id not in self.schemas:
            raise UnknownSchemaError(404, f"schema {schema_id} not found")
        return self.schemas[schema_id]

    def set_compatibility(self, subject: str, level: str) -> None:
        self.compatibility[subject] = level

    def is_compatible(self, subject: str, schema: str) -> bool:
        if subject not in self.subjects:
            return True
        old = {f["name"] for f in json.loads(self.schemas[self.latest_id(subject)])["fields"]}
        added = [f for f in json.loads(schema)["fields"] if f["name"] not in old]
        return all("default" in f for f in added)  # BACKWARD: new readers need defaults for new fields


def valid_events(**kw) -> list[ScheduledEvent]:
    sim = SimulationSettings(seed=17, invalid_payload_rate=0.0, refund_rate=0.3, reversal_rate=0.2,
                             chargeback_rate=0.3, chargeback_reversal_share=0.5, pending_posting_rate=0.3, **kw)
    return build_all_events(sim, TOPICS, 0, 60, START)


def test_every_contract_round_trips_through_avro_unchanged(tmp_path):
    registry = FakeRegistry()
    serializers = serializers_for(registry, TOPICS, auto_register=True)
    decoder = AvroDecoder(registry)
    sim = SimulationSettings(seed=17, refund_rate=0.3, chargeback_rate=0.3, epoch=START)
    path = write_settlement_file(sim, TOPICS, TZ, START, START + timedelta(minutes=2), tmp_path)
    assert path is not None
    settlement_payloads = parse_settlement_file(path.read_text(encoding="utf-8"), TZ)
    payloads = [(e.topic, json.loads(e.value)) for e in valid_events()] + [
        (TOPICS.settlement_events, p) for p in settlement_payloads]
    assert {topic for topic, _ in payloads} == set(TOPICS.source_topics)
    for topic, payload in payloads:
        framed = serializers[topic](payload)
        assert framed[0] == 0 and struct.unpack(">I", framed[1:5])[0] == registry.latest_id(subject_for(topic))
        decoded = decoder.decode(framed)
        assert decoded == payload
        MODEL_BY_SOURCE[TOPICS.source_topics[topic]].model_validate(decoded)


def _record(value: bytes, topic: str | None = None) -> KafkaRecord:
    return KafkaRecord(topic or TOPICS.processor_events, 0, 1, b"k", value, datetime.now(UTC))


def test_ingestion_decodes_avro_and_json_identically(rules):
    registry = FakeRegistry()
    writer = EventWriter(TOPICS, rules, avro_decoder=AvroDecoder(registry))
    event = next(e for e in valid_events() if e.source_system == PROCESSOR)
    avro = serializers_for(registry, TOPICS, auto_register=True)[event.topic](json.loads(event.value))
    from_avro, from_json = writer._decode(_record(avro)), writer._decode(_record(event.value))
    assert from_avro.payload == from_json.payload and from_avro.error is None
    assert json.loads(from_avro.text or "") == json.loads(from_json.text or "")
    assert from_avro.payload_hash != from_json.payload_hash  # Bronze keeps the exact bytes received


@pytest.mark.parametrize(("value", "decoder", "code"), [
    (b"\x00\x00\x00\x00\x63" + b"\x02x", True, "UNKNOWN_SCHEMA_ID"),
    (b"\x00\x00\x00\x00\x01" + b"\xff\xff\xff", True, "INVALID_AVRO"),
    (b"\x00\x00\x00\x00\x01" + b"\x02x", False, "UNSUPPORTED_ENCODING"),
])
def test_bad_avro_is_quarantined_with_a_specific_code(rules, value, decoder, code):
    registry = FakeRegistry()
    serializers_for(registry, TOPICS, auto_register=True)[TOPICS.processor_events](
        json.loads(next(e for e in valid_events() if e.source_system == PROCESSOR).value))
    writer = EventWriter(TOPICS, rules, avro_decoder=AvroDecoder(registry) if decoder else None)
    record = _record(value)
    decoded = writer._decode(record)
    with pytest.raises(_Quarantine) as exc:
        writer._validate(record, decoded)
    assert exc.value.failure.error_code == code


def test_registry_outage_is_transient_not_quarantined(rules):
    registry = FakeRegistry()
    event = next(e for e in valid_events() if e.source_system == PROCESSOR)
    framed = serializers_for(registry, TOPICS, auto_register=True)[event.topic](json.loads(event.value))
    registry.unavailable = True
    writer = EventWriter(TOPICS, rules, avro_decoder=AvroDecoder(registry))
    with pytest.raises(TransientError):
        writer._decode(_record(framed))


def test_producer_encodes_valid_events_and_keeps_injected_garbage_raw():
    registry = FakeRegistry()
    sent = []

    class Capture:
        def produce(self, topic, key=None, value=None, on_delivery=None):
            sent.append(value)

        def poll(self, _t):
            return 0

    sim = SimulationSettings(seed=3, invalid_payload_rate=0.5)
    producer = SourceProducer(PROCESSOR, sim, TOPICS, Capture(), clock=lambda: START,
                              serializers=dict(serializers_for(registry, TOPICS, auto_register=True)))
    invalid = ScheduledEvent(START, 1, TOPICS.processor_events, b"k", b"{not-json", PROCESSOR, "invalid_not_json")
    valid = next(e for e in valid_events() if e.source_system == PROCESSOR)
    assert producer._encode(invalid) == b"{not-json"
    assert producer._encode(valid)[0] == 0


def test_register_all_enforces_backward_compatibility():
    registry = FakeRegistry()
    schemas = schemas_by_topic(TOPICS)
    ids = register_all(registry, schemas, "BACKWARD")
    assert set(ids) == {subject_for(t) for t in TOPICS.source_topics}
    assert set(registry.compatibility.values()) == {"BACKWARD"}
    assert register_all(registry, schemas, "BACKWARD") == ids  # idempotent

    evolved = json.loads(schemas[TOPICS.processor_events])
    evolved["fields"].append({"name": "merchant_id", "type": ["null", "string"], "default": None})
    assert register_all(registry, {TOPICS.processor_events: json.dumps(evolved)}, "BACKWARD")  # optional: fine

    breaking = json.loads(schemas[TOPICS.ledger_events])
    breaking["fields"].append({"name": "merchant_id", "type": "string"})  # no default -> old data unreadable
    with pytest.raises(IncompatibleSchemaError, match=subject_for(TOPICS.ledger_events)):
        register_all(registry, {TOPICS.ledger_events: json.dumps(breaking)}, "BACKWARD")


def test_avro_requires_a_registry_url(monkeypatch):
    monkeypatch.setenv("EVENT_SERIALIZATION", "avro")
    with pytest.raises(ConfigurationError, match="SCHEMA_REGISTRY_URL"):
        load(SchemaRegistrySettings)
    assert load(SchemaRegistrySettings, url="http://registry:8081").serialization == "avro"
