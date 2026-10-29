"""Create roles, database, schema, run migrations, create Gold views and register the rule version.

Usage: python -m scripts.initialize_database
Requires DB_SUPERUSER_PASSWORD, DB_MIGRATOR_PASSWORD, DB_PASSWORD and DB_REPORTING_PASSWORD.
Safe to run repeatedly (idempotent).
"""

from __future__ import annotations

import sys

from common.config import (
    AdminDatabaseSettings,
    BusinessSettings,
    DatabaseSettings,
    ReconciliationSettings,
    RetrySettings,
    ServiceSettings,
    load,
)
from common.logging_setup import configure_logging
from database.migrate import initialize_database
from ingestion.error_handler import run_with_retry
from processing.rules import load_rules


def main() -> int:
    service = load(ServiceSettings)
    configure_logging("initialize-database", service.environment, service.log_level)
    admin, db = load(AdminDatabaseSettings), load(DatabaseSettings)
    rules = load_rules(load(ReconciliationSettings).rules_path)
    business = load(BusinessSettings)
    applied = run_with_retry(
        "db_initialize", load(RetrySettings),
        lambda: initialize_database(admin, db, rules, business.business_timezone),
    )
    print(f"database '{db.name}' ready; migrations applied this run: {applied or 'none'}; "  # noqa: T201
          f"rule_version={rules.rule_version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
