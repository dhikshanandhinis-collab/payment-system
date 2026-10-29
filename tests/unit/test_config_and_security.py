"""Configuration validation, rule loading, logging redaction and admin authorization."""

from __future__ import annotations

import json
import logging

import pytest

from admin.cli import AuthorizationError, authorize
from common.config import (
    BusinessSettings,
    ConfigurationError,
    DatabaseSettings,
    KafkaSettings,
    SimulationSettings,
    TopicSettings,
    apply_secret_files,
    load,
)
from common.logging_setup import JsonFormatter, redact_value
from processing.rules import rules_from_dict


def test_missing_mandatory_secret_fails_clearly(monkeypatch):
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    with pytest.raises(ConfigurationError, match="password"):
        load(DatabaseSettings)


def test_invalid_values_fail(monkeypatch):
    monkeypatch.setenv("BUSINESS_TIMEZONE", "Mars/Olympus")
    with pytest.raises(ConfigurationError, match="BUSINESS_TIMEZONE"):
        load(BusinessSettings)
    with pytest.raises(ConfigurationError):
        load(SimulationSettings, decline_rate=1.5)
    with pytest.raises(ConfigurationError):
        load(TopicSettings, app_events="same", processor_events="same")
    with pytest.raises(ConfigurationError):
        load(TopicSettings, replication_factor=1, min_insync_replicas=2)
    with pytest.raises(ConfigurationError):
        load(DatabaseSettings, password="x", pool_min_size=10, pool_max_size=2)


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "secret-value")
    monkeypatch.setenv("DB_POOL_MAX_SIZE", "9")
    settings = load(DatabaseSettings)
    assert settings.pool_max_size == 9
    assert "secret-value" not in repr(settings)


def test_rules_loaded_and_versioned(rules):
    assert rules.rule_version == "2026.11.2"
    assert rules.calendar.business_day_methods == {"PRE_AUTHORIZED_DEBIT", "ONLINE_BANKING"}
    assert rules.processor_grace("PRE_AUTHORIZED_DEBIT") > rules.processor_grace("CREDIT_CARD")
    assert rules.processor_grace("UNKNOWN_METHOD") == rules.processor_grace_default
    changed = dict(rules.raw)
    changed["late_event_threshold_seconds"] = 61
    assert rules_from_dict(changed).config_hash != rules.config_hash
    assert rules_from_dict(dict(rules.raw)).config_hash == rules.config_hash


def test_rules_reject_unknown_keys(rules):
    bad = dict(rules.raw)
    bad["surprise"] = 1
    with pytest.raises(ValueError):
        rules_from_dict(bad)


def test_log_redaction():
    assert redact_value("db_password", "abc") == "***REDACTED***"
    assert redact_value("account_ref", "SYN-ACCT-12345678") == "***5678"


def test_kafka_mutual_tls_and_sasl_config():
    kafka = load(
        KafkaSettings,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="recon-ingestion",
        sasl_password="s3cret",
        ssl_ca_location="/etc/kafka/ca.pem",
        ssl_certificate_location="/etc/kafka/client.pem",
        ssl_key_location="/etc/kafka/client.key",
    )
    conf = kafka.client_config()
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.password"] == "s3cret"
    assert conf["ssl.certificate.location"] == "/etc/kafka/client.pem"
    assert conf["ssl.endpoint.identification.algorithm"] == "https"
    assert "s3cret" not in repr(kafka)
    assert "ssl.ca.location" not in load(KafkaSettings).client_config()


def test_kafka_config_rejects_incomplete_security():
    with pytest.raises(ConfigurationError, match="SASL_MECHANISM"):
        load(KafkaSettings, security_protocol="SASL_SSL")
    with pytest.raises(ConfigurationError, match="SASL_USERNAME"):
        load(KafkaSettings, security_protocol="SASL_SSL", sasl_mechanism="PLAIN")
    with pytest.raises(ConfigurationError, match="set together"):
        load(KafkaSettings, security_protocol="SSL", ssl_certificate_location="/c.pem")


def test_require_secure_transport(monkeypatch):
    monkeypatch.setenv("REQUIRE_SECURE_TRANSPORT", "true")
    with pytest.raises(ConfigurationError, match="REQUIRE_SECURE_TRANSPORT"):
        load(KafkaSettings)
    with pytest.raises(ConfigurationError, match="REQUIRE_SECURE_TRANSPORT"):
        load(KafkaSettings, security_protocol="SSL", ssl_endpoint_identification_algorithm="none")
    with pytest.raises(ConfigurationError, match="verify-full"):
        load(DatabaseSettings, password="x", sslmode="require")
    assert load(KafkaSettings, security_protocol="SSL").security_protocol == "SSL"
    db = load(DatabaseSettings, password="x", sslmode="verify-full", sslrootcert="/etc/pg/ca.pem")
    assert db.conninfo()["sslrootcert"] == "/etc/pg/ca.pem"
    assert "sslcert" not in db.conninfo()


def test_secrets_from_files(tmp_path, monkeypatch):
    secret = tmp_path / "db_password"
    secret.write_text("from-file\n", encoding="utf-8")
    monkeypatch.setenv("DB_PASSWORD", "")  # registers the variable so monkeypatch restores it afterwards
    monkeypatch.setenv("DB_PASSWORD_FILE", str(secret))
    assert load(DatabaseSettings).password.get_secret_value() == "from-file"

    monkeypatch.setenv("DB_PASSWORD", "explicit")
    assert load(DatabaseSettings).password.get_secret_value() == "explicit"

    env = {"KAFKA_SASL_PASSWORD_FILE": str(tmp_path / "missing")}
    with pytest.raises(ConfigurationError, match="KAFKA_SASL_PASSWORD_FILE"):
        apply_secret_files(env)
    env = {"COVERAGE_FILE": str(secret)}
    apply_secret_files(env)
    assert env == {"COVERAGE_FILE": str(secret)}
    assert "4111" not in redact_value("note", "card 4111 1111 1111 1111 used")
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "pan 4111111111111111", None, None)
    record.payment_id = "PAY-1"
    record.sasl_password = "hunter2"
    out = json.loads(JsonFormatter("svc", "test").format(record))
    assert out["payment_id"] == "PAY-1" and out["sasl_password"] == "***REDACTED***"
    assert "4111111111111111" not in out["message"] and out["service_name"] == "svc"


def test_admin_authorization():
    allowed = frozenset({"ops.alice"})
    authorize("ops.alice", "Investigating ledger delay", allowed)
    with pytest.raises(AuthorizationError):
        authorize("mallory", "Investigating ledger delay", allowed)
    with pytest.raises(AuthorizationError):
        authorize("ops.alice", "short", allowed)
    with pytest.raises(AuthorizationError):
        authorize("ops.alice", "Investigating ledger delay", frozenset())
