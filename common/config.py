"""Environment-based configuration with validation at startup.

Every service loads only the setting groups it needs, so a missing database password does not
prevent a producer from starting, but does stop the ingestion service with a clear error.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parent.parent

# Secrets that may instead be supplied as a file path in <NAME>_FILE (Kubernetes/Docker/Vault-agent style).
SECRET_ENV_VARS = (
    "DB_PASSWORD",
    "DB_SUPERUSER_PASSWORD",
    "DB_MIGRATOR_PASSWORD",
    "DB_REPORTING_PASSWORD",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SSL_KEY_PASSWORD",
    "SCHEMA_REGISTRY_PASSWORD",
)


class ConfigurationError(RuntimeError):
    """Raised when mandatory configuration is missing or invalid."""


class _Base(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False, frozen=True)


class ServiceSettings(_Base):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False, frozen=True)

    environment: Literal["local", "test", "staging", "production"] = "local"
    service_name: str = "recon"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    metrics_port: int = Field(default=9100, ge=1, le=65535)
    metrics_enabled: bool = True


class KafkaSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore", case_sensitive=False, frozen=True)

    bootstrap_servers: str = "localhost:9094"
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"] | None = None
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None
    ssl_ca_location: str | None = None
    # Client certificate for mutual TLS.
    ssl_certificate_location: str | None = None
    ssl_key_location: str | None = None
    ssl_key_password: SecretStr | None = None
    ssl_endpoint_identification_algorithm: Literal["https", "none"] = "https"
    client_id_prefix: str = "bell-recon"
    require_secure_transport: bool = Field(default=False, validation_alias="REQUIRE_SECURE_TRANSPORT")

    @model_validator(mode="after")
    def _check(self) -> KafkaSettings:
        uses_sasl = self.security_protocol.startswith("SASL")
        if uses_sasl and self.sasl_mechanism is None:
            raise ValueError(f"KAFKA_SASL_MECHANISM is required with {self.security_protocol}")
        if uses_sasl and self.sasl_mechanism != "OAUTHBEARER" and not (self.sasl_username and self.sasl_password):
            raise ValueError("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for SASL PLAIN/SCRAM")
        if bool(self.ssl_certificate_location) != bool(self.ssl_key_location):
            raise ValueError("KAFKA_SSL_CERTIFICATE_LOCATION and KAFKA_SSL_KEY_LOCATION must be set together")
        if self.require_secure_transport and self.security_protocol not in ("SSL", "SASL_SSL"):
            raise ValueError("REQUIRE_SECURE_TRANSPORT forbids KAFKA_SECURITY_PROTOCOL " + self.security_protocol)
        if self.require_secure_transport and self.ssl_endpoint_identification_algorithm == "none":
            raise ValueError("REQUIRE_SECURE_TRANSPORT forbids disabling broker hostname verification")
        return self

    def client_config(self) -> dict[str, str | int | float | bool]:
        conf: dict[str, str | int | float | bool] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            conf["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            conf["sasl.username"] = self.sasl_username
        if self.sasl_password:
            conf["sasl.password"] = self.sasl_password.get_secret_value()
        if self.security_protocol in ("SSL", "SASL_SSL"):
            conf["ssl.endpoint.identification.algorithm"] = self.ssl_endpoint_identification_algorithm
            if self.ssl_ca_location:
                conf["ssl.ca.location"] = self.ssl_ca_location
            if self.ssl_certificate_location and self.ssl_key_location:
                conf["ssl.certificate.location"] = self.ssl_certificate_location
                conf["ssl.key.location"] = self.ssl_key_location
            if self.ssl_key_password:
                conf["ssl.key.password"] = self.ssl_key_password.get_secret_value()
        return conf


class TopicSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="TOPIC_", extra="ignore", case_sensitive=False, frozen=True)

    app_events: str = "mybell.payment.events"
    processor_events: str = "processor.transaction.events"
    ledger_events: str = "billing.ledger.events"
    settlement_events: str = "processor.settlement.events"
    reconciliation_events: str = "payment.reconciliation.events"
    dlq: str = "payment.reconciliation.dlq"
    partitions: int = Field(default=6, ge=1, le=256)
    replication_factor: int = Field(default=1, ge=1, le=5)
    min_insync_replicas: int = Field(default=1, ge=1, le=5)
    retention_ms: int = Field(default=7 * 24 * 3600 * 1000, ge=60_000)
    dlq_retention_ms: int = Field(default=30 * 24 * 3600 * 1000, ge=60_000)

    @property
    def source_topics(self) -> dict[str, str]:
        """Map topic name -> source_system expected on that topic."""
        return {
            self.app_events: "mybell_app",
            self.processor_events: "payment_processor",
            self.ledger_events: "billing_ledger",
            self.settlement_events: "processor_settlement",
        }

    @model_validator(mode="after")
    def _check(self) -> TopicSettings:
        names = [self.app_events, self.processor_events, self.ledger_events, self.settlement_events,
                 self.reconciliation_events, self.dlq]
        if len(set(names)) != len(names):
            raise ValueError("topic names must be distinct")
        if self.min_insync_replicas > self.replication_factor:
            raise ValueError("TOPIC_MIN_INSYNC_REPLICAS cannot exceed TOPIC_REPLICATION_FACTOR")
        return self


class DatabaseSettings(_Base):
    """Runtime (least-privilege) database connection used by services."""

    model_config = SettingsConfigDict(env_prefix="DB_", extra="ignore", case_sensitive=False, frozen=True)

    host: str = "localhost"
    port: int = Field(default=5432, ge=1, le=65535)
    name: str = "recon"
    schema_name: str = Field(default="recon", pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    user: str = "recon_app"
    password: SecretStr
    sslmode: Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"] = "prefer"
    sslrootcert: str | None = None
    sslcert: str | None = None
    sslkey: str | None = None
    connect_timeout_s: int = Field(default=5, ge=1, le=120)
    pool_min_size: int = Field(default=1, ge=1, le=100)
    pool_max_size: int = Field(default=5, ge=1, le=200)
    statement_timeout_ms: int = Field(default=60_000, ge=1000)
    require_secure_transport: bool = Field(default=False, validation_alias="REQUIRE_SECURE_TRANSPORT")

    @model_validator(mode="after")
    def _check(self) -> DatabaseSettings:
        if self.pool_min_size > self.pool_max_size:
            raise ValueError("DB_POOL_MIN_SIZE cannot exceed DB_POOL_MAX_SIZE")
        if bool(self.sslcert) != bool(self.sslkey):
            raise ValueError("DB_SSLCERT and DB_SSLKEY must be set together")
        if self.require_secure_transport and self.sslmode not in ("verify-ca", "verify-full"):
            raise ValueError("REQUIRE_SECURE_TRANSPORT requires DB_SSLMODE verify-ca or verify-full")
        return self

    def conninfo(self, user: str | None = None, password: str | None = None, dbname: str | None = None) -> dict:
        info = {
            "host": self.host,
            "port": self.port,
            "dbname": dbname or self.name,
            "user": user or self.user,
            "password": password if password is not None else self.password.get_secret_value(),
            "sslmode": self.sslmode,
            "connect_timeout": self.connect_timeout_s,
            "application_name": "bell-recon",
            "options": f"-c search_path={self.schema_name} -c statement_timeout={self.statement_timeout_ms}",
        }
        for key in ("sslrootcert", "sslcert", "sslkey"):
            if getattr(self, key):
                info[key] = getattr(self, key)
        return info


class AdminDatabaseSettings(_Base):
    """Privileged credentials used only by initialization/migration tooling."""

    model_config = SettingsConfigDict(env_prefix="DB_", extra="ignore", case_sensitive=False, frozen=True)

    superuser: str = "postgres"
    superuser_password: SecretStr
    migrator_user: str = "recon_migrator"
    migrator_password: SecretStr
    app_user: str = "recon_app"
    app_password: SecretStr = Field(validation_alias="DB_PASSWORD")
    reporting_user: str = "recon_reporting"
    reporting_password: SecretStr


class SchemaRegistrySettings(_Base):
    """Avro + schema registry. JSON stays the default; ingestion always accepts both formats."""

    model_config = SettingsConfigDict(env_prefix="SCHEMA_REGISTRY_", extra="ignore", case_sensitive=False, frozen=True)

    url: str | None = None
    username: str | None = None
    password: SecretStr | None = None
    ca_location: str | None = None
    timeout_s: float = Field(default=10.0, gt=0, le=120)
    # Producers register their schema on first use. Set false in production: schemas are registered and
    # compatibility-checked by scripts.register_schemas in the deploy pipeline, producers use the latest.
    auto_register: bool = True
    compatibility: Literal["BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL",
                           "FULL_TRANSITIVE"] = "BACKWARD"
    serialization: Literal["json", "avro"] = Field(default="json", validation_alias="EVENT_SERIALIZATION")

    @model_validator(mode="after")
    def _check(self) -> SchemaRegistrySettings:
        if self.serialization == "avro" and not self.url:
            raise ValueError("EVENT_SERIALIZATION=avro requires SCHEMA_REGISTRY_URL")
        return self


class RetrySettings(_Base):
    model_config = SettingsConfigDict(env_prefix="RETRY_", extra="ignore", case_sensitive=False, frozen=True)

    max_attempts: int = Field(default=6, ge=1, le=50)
    initial_backoff_s: float = Field(default=0.5, gt=0, le=60)
    max_backoff_s: float = Field(default=10.0, gt=0, le=600)
    jitter_s: float = Field(default=0.5, ge=0, le=60)


class IngestionSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="INGESTION_", extra="ignore", case_sensitive=False, frozen=True)

    consumer_group: str = "recon-ingestion"
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    batch_size: int = Field(default=500, ge=1, le=10_000)
    poll_timeout_s: float = Field(default=1.0, gt=0, le=30)
    max_poll_interval_ms: int = Field(default=300_000, ge=10_000)
    session_timeout_ms: int = Field(default=45_000, ge=6_000)
    lag_report_interval_s: float = Field(default=15.0, gt=0)
    publish_dlq: bool = True
    bulk_writes: bool = True


class ReconciliationSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="RECON_", extra="ignore", case_sensitive=False, frozen=True)

    rules_path: Path = REPO_ROOT / "config" / "reconciliation_rules.yaml"
    worker_batch_size: int = Field(default=100, ge=1, le=5000)
    poll_interval_s: float = Field(default=0.5, gt=0, le=60)
    sweep_interval_s: float = Field(default=10.0, gt=0, le=3600)
    outbox_batch_size: int = Field(default=500, ge=1, le=10_000)
    publish_outbox: bool = True
    max_item_attempts: int = Field(default=10, ge=1, le=100)


class BusinessSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore", case_sensitive=False, frozen=True)

    business_timezone: str = "America/Toronto"

    @field_validator("business_timezone")
    @classmethod
    def _tz(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown BUSINESS_TIMEZONE {value!r}") from exc
        return value

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.business_timezone)


class AdminSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="ADMIN_", extra="ignore", case_sensitive=False, frozen=True)

    allowed_actors: str = ""

    @property
    def actors(self) -> frozenset[str]:
        return frozenset(a.strip() for a in self.allowed_actors.split(",") if a.strip())


class SimulationSettings(_Base):
    """Synthetic source simulation. All probabilities are per payment intent unless noted."""

    model_config = SettingsConfigDict(env_prefix="SIM_", extra="ignore", case_sensitive=False, frozen=True)

    seed: int = 20261028
    run_id: str = Field(default="local", pattern=r"^[A-Za-z0-9]{1,16}$")
    epoch: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    payments_per_second: float = Field(default=2.0, gt=0, le=10_000)
    currency: str = Field(default="CAD", pattern=r"^[A-Z]{3}$")
    min_amount: str = "15.00"
    max_amount: str = "450.00"

    decline_rate: float = Field(default=0.06, ge=0, le=1)
    ambiguous_app_result_rate: float = Field(default=0.02, ge=0, le=1)
    app_retry_rate: float = Field(default=0.01, ge=0, le=1)
    missing_app_rate: float = Field(default=0.003, ge=0, le=1)
    missing_processor_rate: float = Field(default=0.005, ge=0, le=1)
    missing_ledger_rate: float = Field(default=0.008, ge=0, le=1)
    processor_amount_mismatch_rate: float = Field(default=0.004, ge=0, le=1)
    ledger_amount_mismatch_rate: float = Field(default=0.004, ge=0, le=1)
    ledger_adjustment_fix_rate: float = Field(default=0.5, ge=0, le=1)
    duplicate_delivery_rate: float = Field(default=0.02, ge=0, le=1)
    duplicate_business_event_rate: float = Field(default=0.005, ge=0, le=1)
    delayed_event_rate: float = Field(default=0.05, ge=0, le=1)
    out_of_order_rate: float = Field(default=0.03, ge=0, le=1)
    invalid_payload_rate: float = Field(default=0.003, ge=0, le=1)
    pending_posting_rate: float = Field(default=0.1, ge=0, le=1)
    refund_rate: float = Field(default=0.03, ge=0, le=1)
    partial_refund_share: float = Field(default=0.6, ge=0, le=1)
    reversal_rate: float = Field(default=0.01, ge=0, le=1)
    chargeback_rate: float = Field(default=0.004, ge=0, le=1)
    chargeback_reversal_share: float = Field(default=0.3, ge=0, le=1)
    settlement_missing_rate: float = Field(default=0.003, ge=0, le=1)
    settlement_amount_mismatch_rate: float = Field(default=0.002, ge=0, le=1)
    billing_charge_rate: float = Field(default=0.2, ge=0, le=1)

    delay_min_s: float = Field(default=5.0, ge=0)
    delay_max_s: float = Field(default=90.0, ge=0)
    refund_after_min_s: float = Field(default=20.0, ge=0)
    refund_after_max_s: float = Field(default=120.0, ge=0)

    interruption_probability_per_minute: float = Field(default=0.0, ge=0, le=1)
    interruption_duration_s: float = Field(default=20.0, ge=0)

    @model_validator(mode="after")
    def _check(self) -> SimulationSettings:
        if self.epoch.tzinfo is None:
            raise ValueError("SIM_EPOCH must include a timezone")
        if self.delay_min_s > self.delay_max_s:
            raise ValueError("SIM_DELAY_MIN_S cannot exceed SIM_DELAY_MAX_S")
        if self.refund_after_min_s > self.refund_after_max_s:
            raise ValueError("SIM_REFUND_AFTER_MIN_S cannot exceed SIM_REFUND_AFTER_MAX_S")
        from common.money import ISO_4217_CODES, to_money

        if self.currency not in ISO_4217_CODES:
            raise ValueError(f"SIM_CURRENCY {self.currency} is not a supported ISO 4217 code")
        if to_money(self.min_amount) <= 0 or to_money(self.min_amount) > to_money(self.max_amount):
            raise ValueError("SIM_MIN_AMOUNT must be positive and <= SIM_MAX_AMOUNT")
        return self


def apply_secret_files(environ: MutableMapping[str, str] = os.environ) -> None:
    """Populate SECRET_ENV_VARS from <NAME>_FILE paths; an explicitly set <NAME> wins."""
    for name in SECRET_ENV_VARS:
        path = environ.get(f"{name}_FILE")
        if not path or environ.get(name):
            continue
        try:
            environ[name] = Path(path).read_text(encoding="utf-8").rstrip("\r\n")
        except OSError as exc:
            raise ConfigurationError(f"cannot read {name}_FILE: {exc.strerror}") from exc


def load[T: BaseSettings](cls: type[T], **overrides: object) -> T:
    """Instantiate a settings class, converting validation failures into a readable startup error."""
    apply_secret_files()
    try:
        return cls(**overrides)  # type: ignore[arg-type]
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in err['loc']) or cls.__name__}: {err['msg']}" for err in exc.errors()
        )
        raise ConfigurationError(f"invalid configuration for {cls.__name__}: {problems}") from exc


@lru_cache(maxsize=1)
def service_settings() -> ServiceSettings:
    return load(ServiceSettings)
