"""Register the Avro event schemas and enforce the compatibility policy.

Run in the deploy pipeline before producers are upgraded (and as the ``schema-init`` job locally). Exits
non-zero, registering nothing for that subject, if a schema is incompatible with the latest registered
version under SCHEMA_REGISTRY_COMPATIBILITY (default BACKWARD: new consumers can read old data).

    python -m scripts.register_schemas
"""

from __future__ import annotations

import logging
import sys

from common.config import RetrySettings, SchemaRegistrySettings, ServiceSettings, TopicSettings, load
from common.logging_setup import configure_logging
from common.schema_registry import SchemaRegistryClient, schemas_by_topic, subject_for
from ingestion.error_handler import run_with_retry

log = logging.getLogger("scripts.register_schemas")


class IncompatibleSchemaError(RuntimeError):
    pass


def register_all(client: SchemaRegistryClient, schemas: dict[str, str], compatibility: str) -> dict[str, int]:
    """schemas: topic -> Avro schema. Returns subject -> schema id."""
    incompatible = []
    ids: dict[str, int] = {}
    for topic, schema in schemas.items():
        subject = subject_for(topic)
        client.set_compatibility(subject, compatibility)
        if not client.is_compatible(subject, schema):
            incompatible.append(subject)
            continue
        ids[subject] = client.register(subject, schema)
    if incompatible:
        raise IncompatibleSchemaError(f"incompatible with latest registered version ({compatibility}): {incompatible}")
    return ids


def main() -> int:
    service = load(ServiceSettings)
    configure_logging("register-schemas", service.environment, service.log_level)
    settings, topics, retry = load(SchemaRegistrySettings), load(TopicSettings), load(RetrySettings)
    if not settings.url:
        log.error("SCHEMA_REGISTRY_URL is not set")
        return 2
    client = SchemaRegistryClient(settings)
    try:
        ids = run_with_retry("schema_registry", retry,
                             lambda: register_all(client, schemas_by_topic(topics), settings.compatibility))
    except IncompatibleSchemaError as exc:
        log.error("schema compatibility check failed", extra={"error_code": "INCOMPATIBLE_SCHEMA", "detail": str(exc)})
        return 1
    log.info("schemas registered", extra={"subjects": ids, "compatibility": settings.compatibility})
    return 0


if __name__ == "__main__":
    sys.exit(main())
