from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from common.config import REPO_ROOT, AdminDatabaseSettings, DatabaseSettings, TopicSettings, load
from processing.rules import ReconciliationRules, load_rules

INTEGRATION_DIRS = {"integration": "integration", "end_to_end": "e2e", "recovery": "recovery"}
_CREATED_TOPICS: list[str] = []


def integration_enabled() -> bool:
    return os.environ.get("RECON_INTEGRATION") == "1"


def pytest_collection_modifyitems(config, items):  # noqa: ARG001
    skip = pytest.mark.skip(reason="set RECON_INTEGRATION=1 with Kafka/PostgreSQL running (docker compose --profile test)")
    for item in items:
        parts = Path(str(item.fspath)).parts
        for folder, marker in INTEGRATION_DIRS.items():
            if folder in parts:
                item.add_marker(getattr(pytest.mark, marker))
                item.add_marker(pytest.mark.integration)
                if not integration_enabled():
                    item.add_marker(skip)


@pytest.fixture(scope="session")
def rules() -> ReconciliationRules:
    return load_rules(REPO_ROOT / "config" / "reconciliation_rules.yaml")


@pytest.fixture(scope="module")
def db_settings(rules) -> Iterator[DatabaseSettings]:
    """A fresh, fully migrated database per test module (dropped afterwards)."""
    from database.connection import connect
    from database.migrate import initialize_database

    admin = load(AdminDatabaseSettings)
    name = f"recon_test_{uuid.uuid4().hex[:10]}"
    settings = load(DatabaseSettings, name=name)
    initialize_database(admin, settings, rules, "America/Toronto", database_name=name)
    yield settings
    with connect(settings, autocommit=True, user=admin.superuser,
                 password=admin.superuser_password.get_secret_value(), dbname="postgres") as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')  # name is generated above


@pytest.fixture(scope="module")
def database(db_settings):
    from database.connection import Database

    db = Database(db_settings, name="test", max_size=8)
    db.open()
    yield db
    db.close()


def create_test_topics() -> TopicSettings:
    """Create a uniquely named topic set so a test never reads another test's messages."""
    from common.config import KafkaSettings, RetrySettings
    from scripts.create_topics import create_topics

    suffix = uuid.uuid4().hex[:8]
    topics = TopicSettings(
        app_events=f"it-{suffix}.mybell.payment.events",
        processor_events=f"it-{suffix}.processor.transaction.events",
        ledger_events=f"it-{suffix}.billing.ledger.events",
        reconciliation_events=f"it-{suffix}.payment.reconciliation.events",
        dlq=f"it-{suffix}.payment.reconciliation.dlq",
        partitions=3,
    )
    create_topics(load(KafkaSettings), topics, RetrySettings(max_attempts=5))
    _CREATED_TOPICS.extend([*topics.source_topics, topics.reconciliation_events, topics.dlq])
    return topics


def pytest_sessionfinish(session, exitstatus):  # noqa: ARG001
    """Drop the per-test topics; leftovers add metadata and coordinator load to the local broker."""
    if not _CREATED_TOPICS:
        return
    from confluent_kafka.admin import AdminClient

    from common.config import KafkaSettings

    admin = AdminClient(load(KafkaSettings).client_config())
    for future in admin.delete_topics(list(_CREATED_TOPICS), operation_timeout=30).values():
        try:
            future.result(timeout=60)
        except Exception:  # noqa: BLE001 - best-effort cleanup must not fail the run
            pass
    _CREATED_TOPICS.clear()


@pytest.fixture(scope="module")
def kafka_topics() -> TopicSettings:
    return create_test_topics()
