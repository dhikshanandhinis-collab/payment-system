"""Checksum-tracked SQL migration runner and database initialization.

Migrations are applied by the dedicated migrator role. Each migration runs in its own transaction
and is recorded with a SHA-256 checksum; editing an applied migration is detected and refused.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass

import psycopg
from psycopg import sql

from common.config import REPO_ROOT, AdminDatabaseSettings, DatabaseSettings
from database.connection import connect
from processing.rules import ReconciliationRules

log = logging.getLogger(__name__)

MIGRATIONS_DIR = REPO_ROOT / "database" / "migrations"
GOLD_SQL = REPO_ROOT / "analytics" / "gold_tables.sql"
_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Placeholders:
    schema: str
    app_role: str
    reporting_role: str

    def __post_init__(self) -> None:
        for value in (self.schema, self.app_role, self.reporting_role):
            if not _IDENT.match(value):
                raise MigrationError(f"invalid SQL identifier {value!r}")

    def render(self, text: str) -> str:
        return (
            text.replace("{{schema}}", self.schema)
            .replace("{{app_role}}", self.app_role)
            .replace("{{reporting_role}}", self.reporting_role)
        )


def apply_migrations(conn: psycopg.Connection, placeholders: Placeholders) -> list[str]:
    """Apply pending migrations and (re)create the Gold views. Returns the versions applied."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     TEXT PRIMARY KEY,
            checksum    CHAR(64) NOT NULL,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    conn.commit()
    applied: list[str] = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        content = path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(content.encode()).hexdigest()
        row = conn.execute("SELECT checksum FROM schema_migrations WHERE version = %s", (path.stem,)).fetchone()
        if row is not None:
            if row[0] != checksum:
                raise MigrationError(f"migration {path.name} was modified after being applied")
            continue
        with conn.transaction():
            conn.execute(placeholders.render(content))
            conn.execute("INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s)", (path.stem, checksum))
        applied.append(path.stem)
        log.info("migration applied", extra={"migration": path.name})
    with conn.transaction():
        conn.execute(placeholders.render(GOLD_SQL.read_text(encoding="utf-8")))
    return applied


def _ensure_role(cur: psycopg.Cursor, role: str, password: str) -> None:
    exists = cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
    verb = sql.SQL("ALTER ROLE") if exists else sql.SQL("CREATE ROLE")
    cur.execute(
        sql.SQL("{} {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD {}").format(
            verb, sql.Identifier(role), sql.Literal(password)
        )
    )


def initialize_database(
    admin: AdminDatabaseSettings,
    db: DatabaseSettings,
    rules: ReconciliationRules,
    business_timezone: str,
    database_name: str | None = None,
) -> list[str]:
    """Idempotently create roles, database, schema, run migrations and register settings."""
    dbname = database_name or db.name
    if not _IDENT.match(dbname):
        raise MigrationError(f"invalid database name {dbname!r}")
    placeholders = Placeholders(db.schema_name, admin.app_user, admin.reporting_user)

    with connect(
        db, autocommit=True, user=admin.superuser, password=admin.superuser_password.get_secret_value(), dbname="postgres"
    ) as conn:
        with conn.cursor() as cur:
            _ensure_role(cur, admin.migrator_user, admin.migrator_password.get_secret_value())
            _ensure_role(cur, admin.app_user, admin.app_password.get_secret_value())
            _ensure_role(cur, admin.reporting_user, admin.reporting_password.get_secret_value())
            if not cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone():
                cur.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {}").format(
                        sql.Identifier(dbname), sql.Identifier(admin.migrator_user)
                    )
                )

    with connect(
        db, autocommit=True, user=admin.superuser, password=admin.superuser_password.get_secret_value(), dbname=dbname
    ) as conn:
        ident = sql.Identifier
        conn.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(ident(dbname)))
        conn.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}").format(
                ident(dbname), ident(admin.app_user), ident(admin.reporting_user)
            )
        )
        conn.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}").format(
                ident(db.schema_name), ident(admin.migrator_user)
            )
        )
        for role in (admin.migrator_user, admin.app_user, admin.reporting_user):
            conn.execute(
                sql.SQL("ALTER ROLE {} IN DATABASE {} SET search_path = {}").format(
                    ident(role), ident(dbname), ident(db.schema_name)
                )
            )

    with connect(
        db, user=admin.migrator_user, password=admin.migrator_password.get_secret_value(), dbname=dbname
    ) as conn:
        applied = apply_migrations(conn, placeholders)
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO reporting_settings (id, business_timezone, late_event_threshold_seconds)
                VALUES (1, %s, %s)
                ON CONFLICT (id) DO UPDATE SET business_timezone = EXCLUDED.business_timezone,
                    late_event_threshold_seconds = EXCLUDED.late_event_threshold_seconds, updated_at = now()
                """,
                (business_timezone, int(rules.late_event_threshold.total_seconds())),
            )
        from database.repositories import register_rule_version

        with conn.transaction():
            register_rule_version(conn, rules)
    return applied
