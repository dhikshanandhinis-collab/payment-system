"""Against a real schema registry (CI starts one): registration, compatibility policy and Avro ingestion."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from common.config import SchemaRegistrySettings, TopicSettings, load
from common.schema_registry import AvroDecoder, SchemaRegistryClient, schemas_by_topic, serializers_for
from database.repositories import KafkaRecord
from ingestion.event_writer import PROCESSED, EventWriter
from scripts.register_schemas import IncompatibleSchemaError, register_all
from tests.factories import ledger, proc

pytestmark = pytest.mark.skipif(not os.environ.get("SCHEMA_REGISTRY_URL"), reason="SCHEMA_REGISTRY_URL not set")

T0 = (datetime.now(UTC) - timedelta(hours=1)).replace(microsecond=0)


@pytest.fixture(scope="module")
def topics() -> TopicSettings:
    suffix = uuid.uuid4().hex[:8]  # unique subjects per run
    return TopicSettings(app_events=f"sr-{suffix}.app", processor_events=f"sr-{suffix}.processor",
                         ledger_events=f"sr-{suffix}.ledger", settlement_events=f"sr-{suffix}.settlement",
                         reconciliation_events=f"sr-{suffix}.recon", dlq=f"sr-{suffix}.dlq")


@pytest.fixture(scope="module")
def client() -> SchemaRegistryClient:
    return SchemaRegistryClient(load(SchemaRegistrySettings))


def test_registration_is_idempotent_and_backward_compatibility_is_enforced(client, topics):
    schemas = schemas_by_topic(topics)
    ids = register_all(client, schemas, "BACKWARD")
    assert register_all(client, schemas, "BACKWARD") == ids
    breaking = json.loads(schemas[topics.ledger_events])
    breaking["fields"].append({"name": "merchant_id", "type": "string"})
    with pytest.raises(IncompatibleSchemaError):
        register_all(client, {topics.ledger_events: json.dumps(breaking)}, "BACKWARD")


def test_avro_events_are_ingested_like_json(database, rules, client, topics):
    serializers = serializers_for(client, topics, auto_register=True)
    payloads = [
        (topics.processor_events, proc("PAY-IT-AVRO-1", "CAPTURE", "C-IT-AVRO-1", "12.34", T0)),
        (topics.ledger_events, ledger("PAY-IT-AVRO-1", "PAYMENT_CREDIT", "L-IT-AVRO-1", "12.34", T0)),
    ]
    records = [KafkaRecord(topic, 0, 9_000 + i, b"k", serializers[topic](payload), datetime.now(UTC))
               for i, (topic, payload) in enumerate(payloads)]
    writer = EventWriter(topics, rules, avro_decoder=AvroDecoder(client))
    with database.transaction() as conn:
        results = writer.process_batch(conn, records)
    assert [r.outcome for r in results] == [PROCESSED, PROCESSED]
    with database.transaction() as conn:
        stored = conn.execute(
            "SELECT payload_json->>'amount', get_byte(payload_bytes, 0) FROM raw_payment_events "
            "WHERE payment_id = 'PAY-IT-AVRO-1' ORDER BY source_offset").fetchall()
        silver = conn.execute("SELECT amount FROM processor_transactions WHERE payment_id = 'PAY-IT-AVRO-1'").fetchone()
    assert stored == [("12.34", 0), ("12.34", 0)]  # queryable JSON copy, original Avro bytes kept
    assert str(silver[0]) == "12.34"
