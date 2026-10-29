# Operational Runbook

All commands run from the repository root. On Windows use PowerShell; commands are identical
except where noted. `docker compose` refers to Docker Compose v2.

## 1. Daily operations

| Task | Command |
|---|---|
| Service status / health | `docker compose ps` (health column uses `/readyz`) |
| Follow logs (JSON) | `docker compose logs -f ingestion reconciliation` |
| Metrics of a service | `curl http://localhost:9101/metrics` (ingestion), `9102` reconciliation, `9103-9105` producers, `9106` batch-scheduler |
| Readiness | `curl http://localhost:9101/readyz` |
| Consumer lag | `docker compose run --rm tools python -m scripts.report --kafka-lag` |
| Reports | `docker compose run --rm tools python -m scripts.report --all` (or `--list`) |
| Open exceptions | `docker compose run --rm tools python -m scripts.report oldest_unresolved_exceptions` |
| Daily reconciliation (previous business date) | `docker compose run --rm tools python -m batch.daily_reconciliation` |
| Specific date | `... python -m batch.daily_reconciliation --business-date 2026-10-27` |
| Backfill | `... python -m batch.backfill --start-date 2026-10-01 --end-date 2026-10-07` |
| psql (runtime role) | `docker compose exec postgres psql -U recon_app -d recon` (password from `.env`) |
| Inspect a topic | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic payment.reconciliation.dlq --from-beginning --property print.headers=true --max-messages 5` |
| Consumer group | `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group recon-ingestion` |

## 2. Alerts and first response

| Symptom (metric / signal) | Likely cause | Action |
|---|---|---|
| `recon_consumer_lag_messages` rising, `/readyz` 503 on ingestion | PostgreSQL unavailable or slow | `docker compose ps postgres`; check `docker compose logs postgres`; ingestion recovers automatically once the DB is back (offsets were not committed) |
| `recon_ingestion_batch_failures_total{error_category="RETRY_EXHAUSTED"}` increasing | Same as above | Same as above |
| `recon_offset_commits_total{outcome="failed"}` increasing, lag rising while `/readyz` is 200; logs show `SESSTMOUT` / `GroupCoordinator: Timed out` | Kafka broker fenced or coordinator moving (CPU starvation on a single local broker) | `docker compose logs kafka \| Select-String BrokerLifecycleManager`; free CPU or `docker compose restart kafka`; records are redelivered and skipped idempotently. See failure_recovery.md 2.1 |
| `recon_events_quarantined_total` spike for one source | Source contract change / bad deploy at source | `scripts.report quarantine_reasons`; contact source owner; after fix use `admin.cli replay-quarantine` |
| `recon_dlq_published_total{outcome="failed"}` | Kafka DLQ topic unavailable | Quarantine rows are still durable in PostgreSQL; fix Kafka |
| `recon_work_queue_depth` growing | Reconciliation service down or slow | `docker compose logs reconciliation`; scale with `docker compose up -d --scale reconciliation=2` (SKIP LOCKED makes this safe) |
| `recon_reconciliation_item_failures_total` > 0 | Bug or bad data for specific payments | `SELECT payment_id, attempts, last_error FROM reconciliation_work_queue WHERE attempts > 0;` |
| `recon_outbox_backlog` > 0 for long | Kafka unavailable for reconciliation service | Fix Kafka; rows are retried automatically |
| `recon_batch_runs_total{outcome="FAILED"}` | Batch failure | `scripts.report run_summary`; rerun the date (safe) |
| DQ check failed in run summary | Threshold exceeded | `SELECT * FROM data_quality_results WHERE NOT passed ORDER BY run_id DESC;` then investigate the source |

## 3. Common procedures

### 3.1 PostgreSQL outage drill
```bash
docker compose stop postgres          # ingestion: retries, then rewinds; readiness 503; no offsets committed
docker compose logs --tail 20 ingestion
docker compose start postgres         # ingestion resumes from the uncommitted offsets automatically
docker compose run --rm tools python -m scripts.report duplicate_delivery_rates
```

### 3.2 Kafka outage drill
```bash
docker compose stop kafka             # producers buffer and log delivery errors; consumer reconnects
docker compose start kafka
```

### 3.3 Restart a service safely
`docker compose restart ingestion` - SIGTERM is handled: the current batch finishes and commits,
the consumer leaves the group cleanly (`stop_grace_period: 30s`).

### 3.4 Quarantine handling
```sql
SELECT quarantine_id, source_system, error_code, error_detail, convert_from(payload_bytes, 'UTF8')
FROM quarantined_events WHERE status = 'OPEN' ORDER BY quarantined_at LIMIT 20;
```
* Correctable -> `python -m admin.cli replay-quarantine --quarantine-id N --payload-file fixed.json --actor A --reason R`
* Not correctable -> `python -m admin.cli discard-quarantine --quarantine-id N --actor A --reason R`

Files for `--payload-file` can be placed in `./load_test_results/` (mounted at `/app/load_test_results` in `tools`).

### 3.5 Disputed case
```bash
docker compose run --rm tools python -m admin.cli hold --payment-id PAY-... --actor ops.alice --reason "Dispute #123 opened"
docker compose run --rm tools python -m admin.cli release --payment-id PAY-... --actor ops.alice --reason "Dispute #123 closed"
```

### 3.6 Deploying a new rule version
1. Edit `config/reconciliation_rules.yaml` **and** bump `rule_version`.
2. Rebuild and restart `reconciliation` (it registers the new version; refuses changed content under an old version).
3. `python -m admin.cli reevaluate-open --actor ops.alice --reason "Rule version X deployed"`.

### 3.7 Schema version change at a source
A new `schema_version` is quarantined as `UNSUPPORTED_SCHEMA_VERSION` until a model for it is added
in `processing/models.py`. After deploying support, replay the quarantined events (3.4).

## 4. Start, stop, reset

| Task | Command |
|---|---|
| Build | `docker compose build` |
| Start infrastructure only | `docker compose up -d kafka postgres` |
| Initialize DB + topics (idempotent) | `docker compose up kafka-init db-init` |
| Start everything | `docker compose up -d` |
| Optional Prometheus/Grafana | `docker compose --profile observability up -d` (Grafana http://localhost:3000, admin / `GRAFANA_ADMIN_PASSWORD`) |
| Stop (keep data) | `docker compose down` |
| **Reset (DESTROYS all local Kafka and PostgreSQL data)** | `docker compose --profile observability --profile test --profile tools down -v` |

> **Warning:** `down -v` deletes the `kafka-data` and `postgres-data` volumes - every event, case and
> audit record in the local environment is lost. Only use it for synthetic local data.
