"""Avro events in the Confluent wire format, backed by a schema registry.

Wire format (Confluent Schema Registry, Karapace and Apicurio's ccompat API all use it):
    byte 0      magic byte 0x00
    bytes 1-4   schema id, big-endian unsigned int
    bytes 5-    Avro binary body (schemaless)

Subjects follow the TopicNameStrategy (``<topic>-value``). The Avro records mirror the JSON contracts in
``schemas/`` field for field (amounts and timestamps stay strings), so both formats decode into the same
payload and go through the same Pydantic validation; ingestion accepts JSON and Avro side by side, which
allows producers to migrate one at a time.

Only the registry's REST API is used (stdlib HTTP client), which keeps the dependency footprint to fastavro.
"""

from __future__ import annotations

import base64
import io
import json
import ssl
import struct
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import fastavro

from common.config import REPO_ROOT, SchemaRegistrySettings, TopicSettings
from common.errors import TransientError

MAGIC_BYTE = 0
AVRO_SCHEMA_DIR = REPO_ROOT / "schemas" / "avro"
SCHEMA_FILE_BY_SOURCE = {
    "mybell_app": "application_payment.avsc",
    "payment_processor": "processor_transaction.avsc",
    "billing_ledger": "billing_ledger.avsc",
    "processor_settlement": "settlement_record.avsc",
}
_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class SchemaRegistryError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"schema registry HTTP {status}: {message}")
        self.status = status


class UnknownSchemaError(SchemaRegistryError):
    pass


def subject_for(topic: str) -> str:
    return f"{topic}-value"


def load_avro_schema(source_system: str) -> str:
    return (AVRO_SCHEMA_DIR / SCHEMA_FILE_BY_SOURCE[source_system]).read_text(encoding="utf-8")


def schemas_by_topic(topics: TopicSettings) -> dict[str, str]:
    return {topic: load_avro_schema(source) for topic, source in topics.source_topics.items()}


class SchemaRegistryClient:
    def __init__(self, settings: SchemaRegistrySettings) -> None:
        if not settings.url:
            raise ValueError("SCHEMA_REGISTRY_URL is not configured")
        self.base = settings.url.rstrip("/")
        self.timeout = settings.timeout_s
        self.context = ssl.create_default_context(cafile=settings.ca_location) if settings.ca_location else None
        self.headers = {"Accept": _CONTENT_TYPE, "Content-Type": _CONTENT_TYPE}
        if settings.username and settings.password:
            token = f"{settings.username}:{settings.password.get_secret_value()}".encode()
            self.headers["Authorization"] = "Basic " + base64.b64encode(token).decode()
        self._schemas_by_id: dict[int, str] = {}

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data=data, headers=self.headers, method=method)  # noqa: S310
        try:
            with urllib.request.urlopen(request, timeout=self.timeout, context=self.context) as response:  # noqa: S310
                return json.loads(response.read() or b"null")
        except urllib.error.HTTPError as exc:
            message = exc.read().decode("utf-8", "replace")[:500]
            if exc.code >= 500 or exc.code == 429:
                raise TransientError(f"schema registry HTTP {exc.code}") from exc
            if exc.code == 404:
                raise UnknownSchemaError(exc.code, message) from exc
            raise SchemaRegistryError(exc.code, message) from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            raise TransientError(f"schema registry unreachable: {exc}") from exc

    def register(self, subject: str, schema: str) -> int:
        return int(self._request("POST", f"/subjects/{subject}/versions", {"schema": schema, "schemaType": "AVRO"})["id"])

    def latest_id(self, subject: str) -> int:
        return int(self._request("GET", f"/subjects/{subject}/versions/latest")["id"])

    def get_schema(self, schema_id: int) -> str:
        if schema_id not in self._schemas_by_id:
            self._schemas_by_id[schema_id] = self._request("GET", f"/schemas/ids/{schema_id}")["schema"]
        return self._schemas_by_id[schema_id]

    def set_compatibility(self, subject: str, level: str) -> None:
        self._request("PUT", f"/config/{subject}", {"compatibility": level})

    def is_compatible(self, subject: str, schema: str) -> bool:
        try:
            result = self._request("POST", f"/compatibility/subjects/{subject}/versions/latest",
                                   {"schema": schema, "schemaType": "AVRO"})
        except UnknownSchemaError:
            return True  # first version of the subject
        return bool(result.get("is_compatible"))


def _parse(schema: str) -> Any:
    return fastavro.parse_schema(json.loads(schema))


class AvroSerializer:
    """Encodes payload dicts for one topic; the schema id is resolved once (register or latest)."""

    def __init__(self, client: SchemaRegistryClient, topic: str, schema: str, auto_register: bool) -> None:
        self.client = client
        self.subject = subject_for(topic)
        self.schema = schema
        self.parsed = _parse(schema)
        self.auto_register = auto_register
        self.schema_id: int | None = None

    def __call__(self, payload: dict[str, Any]) -> bytes:
        if self.schema_id is None:
            register = self.client.register if self.auto_register else None
            self.schema_id = register(self.subject, self.schema) if register else self.client.latest_id(self.subject)
        out = io.BytesIO()
        out.write(struct.pack(">bI", MAGIC_BYTE, self.schema_id))
        fastavro.schemaless_writer(out, self.parsed, payload)
        return out.getvalue()


class AvroDecoder:
    """Decodes Confluent-framed Avro bytes into the same payload dicts the JSON contracts describe."""

    def __init__(self, client: SchemaRegistryClient) -> None:
        self.client = client
        self._parsed: dict[int, Any] = {}

    @staticmethod
    def is_avro(value: bytes | None) -> bool:
        return value is not None and len(value) > 5 and value[0] == MAGIC_BYTE

    def decode(self, value: bytes) -> dict[str, Any]:
        _, schema_id = struct.unpack(">bI", value[:5])
        if schema_id not in self._parsed:
            self._parsed[schema_id] = _parse(self.client.get_schema(schema_id))
        record = fastavro.schemaless_reader(io.BytesIO(value[5:]), self._parsed[schema_id], None)
        if not isinstance(record, dict):
            raise ValueError("Avro body is not a record")
        # Absent optional fields are omitted, exactly like in the JSON payloads.
        return {k: v for k, v in record.items() if v is not None}


def serializers_for(client: SchemaRegistryClient, topics: TopicSettings, auto_register: bool) -> dict[str, AvroSerializer]:
    return {topic: AvroSerializer(client, topic, schema, auto_register) for topic, schema in schemas_by_topic(topics).items()}


def schema_files() -> list[Path]:
    return sorted(AVRO_SCHEMA_DIR.glob("*.avsc"))
