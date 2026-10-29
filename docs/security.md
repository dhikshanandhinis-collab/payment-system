# Security

This repository is a **local, synthetic-data** engineering project. The controls below are
production-oriented, but the local Docker environment is **not** production-secure; the differences
are listed explicitly in section 4.

## 1. Data

* Only synthetic identifiers are generated (`PAY-<run>-<n>`, `SYN-ACCT-<n>`, `PTX-...`, `LED-...`).
* No card numbers (PAN), CVV, bank account numbers, names, addresses, real Bell credentials or real
  Bell customer data exist anywhere in the code, tests or fixtures.
* The platform never moves money, initiates refunds or modifies customer accounts.
* `payment_id`/`account_ref` are identifiers, not access controls; access is enforced by database roles.

## 2. Implemented controls

| Area | Control | Where |
|---|---|---|
| Secrets | No secrets in code; all credentials from environment; `.env` git-ignored; `.env.example` holds placeholders only; `SecretStr` prevents secrets appearing in reprs/logs; compose fails fast (`${VAR:?}`) when a secret is missing | `common/config.py`, `.gitignore`, `docker-compose.yml` |
| Per-service secrets | Each container receives only the variables it needs. The superuser and migrator passwords are given only to `db-init` (and the test runner) | `docker-compose.yml` |
| Least privilege | Three roles: `recon_migrator` (DDL owner), `recon_app` (runtime DML), `recon_reporting` (read-only). Runtime cannot run DDL, cannot delete Bronze/Silver/audit rows, and has column-level UPDATE only where needed (e.g. `raw_payment_events.processing_status`, never `payload_bytes`) | `database/migrations/0004_grants.sql` |
| Audit immutability | `reconciliation_history` and `admin_actions` reject UPDATE/DELETE/TRUNCATE via trigger, even for the owner | `0003_reconciliation.sql` |
| Restricted admin operations | No command can set a status or amount directly. Allowed: re-run engine, review hold/release, replay/discard quarantine (via Kafka, so validation applies), re-queue open cases. Each needs an allow-listed actor + written reason and is audited | `admin/cli.py` |
| Input validation | Strict Pydantic contracts (unknown fields rejected), decimal-string amounts, ISO currencies, timestamp bounds, status/type consistency | `processing/models.py`, `processing/validation.py` |
| SQL injection | All values are bound parameters; dynamic identifiers come only from internal constants via `psycopg.sql.Identifier`; migration placeholders are validated identifiers | `database/repositories.py`, `database/migrate.py` |
| Log hygiene | JSON logs; keys matching password/secret/token/card/pan/cvv/sasl are redacted; 13-19 digit sequences masked; `account_ref` masked to last 4; validation errors never echo input values | `common/logging_setup.py`, `processing/validation.py` |
| Metrics | Bounded label sets only; no payment/account/event ids as labels | `observability/metrics.py` |
| Containers | Non-root user (uid 10001), read-only root FS, `no-new-privileges`, all capabilities dropped, `init` for signal handling, slim base image | `Dockerfile`, `docker-compose.yml` |
| Network exposure | Host ports bound to `127.0.0.1` only | `docker-compose.yml` |
| PostgreSQL auth | SCRAM-SHA-256 for host connections; `PUBLIC` connect and `public` schema create revoked | `docker-compose.yml`, `database/migrate.py` |
| Dependencies | Version ranges in `requirements*.txt`, exact resolved versions in `requirements*.lock` (generated in the runtime image's Python) | `scripts/lock_dependencies.*` |

## 3. Retention and deletion

| Data | Retention (defaults) | Mechanism |
|---|---|---|
| Kafka source topics | 7 days (`TOPIC_RETENTION_MS`) | Topic config; replay window |
| DLQ topic | 30 days (`TOPIC_DLQ_RETENTION_MS`) | Topic config |
| Bronze payload bytes | 90 days (`RETENTION_BRONZE_PAYLOAD_DAYS`) | `batch.retention` writes gzip NDJSON to `RETENTION_ARCHIVE_DIR`, checking every payload against its stored SHA-256 first, then blanks `payload_bytes`/`payload_json` and records `archive_uri`. The row (Kafka position, hash, ids) is kept for idempotency and Silver foreign keys. `read_archive()` restores and re-verifies |
| `reconciliation_history` | 84 months (`RETENTION_HISTORY_MONTHS`) | Monthly RANGE partitions (migration 0007); expired months are dropped as whole partitions. Row-level DELETE stays blocked by triggers on the parent and every partition |
| Published outbox rows | 30 days (`RETENTION_OUTBOX_DAYS`) | Deleted by `batch.retention` |
| Silver, cases, quarantine, admin actions, dedup keys | Retained | Financial evidence; deletion is an approved DBA procedure |

The retention job connects as the migrator (owner) role, because the runtime role deliberately cannot
update payloads, delete or drop tables. Each batch commits only after its archive file has been
fsynced and renamed, so a crash at worst leaves an orphan archive file and never a blanked payload
without an archive. In production, `RETENTION_ARCHIVE_DIR` should be immutable (WORM) object storage.

## 4. Local simplifications vs. production requirements

| Local | Production requirement (implemented, enabled by configuration) |
|---|---|
| Kafka PLAINTEXT, no authentication, single broker | SASL_SSL/mTLS supported; `REQUIRE_SECURE_TRANSPORT=true` refuses plaintext. Still needed: topic ACLs per service principal, RF=3, `min.insync.replicas=2` |
| PostgreSQL `sslmode=disable` inside the Docker network | `DB_SSLMODE=verify-full` with `DB_SSLROOTCERT` (and client certs) supported and enforced by `REQUIRE_SECURE_TRANSPORT` |
| Passwords in a local `.env` file | `<NAME>_FILE` secrets; the Helm chart mounts them from an existing Secret (External Secrets / Vault) |
| Admin authorization = allow-list env var | SSO/IAM identities, MFA, separate privileged DB role for admin tooling, four-eyes approval for corrections |
| Grafana admin password from `.env` | SSO, RBAC |
| Images built locally, no scanning in CI | Image signing, SBOM, vulnerability scanning (e.g. Trivy/Grype) in CI, pinned digests |
| No encryption at rest configured | Encrypted volumes/backups; field-level encryption if any real personal data were introduced |
| Plain HTTP metrics/health endpoints on the container network | Authenticated scrape endpoints or network policy isolation |

## 5. Dependency and image review

* Runtime dependencies: confluent-kafka (librdkafka), psycopg 3 + pool, Pydantic 2, pydantic-settings,
  PyYAML (`safe_load` only), tenacity, prometheus-client, tzdata.
* Images: `python:3.12-slim`, `apache/kafka:3.9.1` (KRaft), `postgres:17-alpine`; optional
  `prom/prometheus`, `grafana/grafana-oss`.
* Review cadence: regenerate lock files, rebuild, and run the full test suite; scan images before use.
  No automated vulnerability scan is part of this repository.
