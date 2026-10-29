# Performance Testing

> The 3 million events/day target is a **synthetic engineering benchmark**, not a statement about
> Bell's payment volumes. 3,000,000 / 86,400 s ~= **34.7 events/s** on average. Bursts are tested
> separately. All numbers below were measured on the hardware listed; nothing is extrapolated
> unless explicitly labelled as such.

## 1. Tooling

[scripts/run_load_test.py](../scripts/run_load_test.py) builds a deterministic synthetic workload
(same generator as the simulators, independent sources, with duplicates, delays, invalid payloads
and missing/mismatched records injected), publishes it to the real topics at a configurable steady
rate with an optional initial burst, waits until every event is in Bronze and the reconciliation
queue for the run is empty, then queries PostgreSQL for the measurements and finally times a full
business-date batch run. Results are written to `load_test_results/<run_id>.json`.

Configurable: `--payments` (total events ~ 5 x payments), `--rate`, `--burst-rate`,
`--burst-seconds`, `--duplicate-rate`, `--delayed-rate`, `--delay-max-s`, `--invalid-rate`,
`--failure-rate`, `--wait-timeout`.

Measured: producer throughput (acked events/s), ingestion throughput (Bronze rows/s between first and
last ingestion), Kafka->Bronze latency p50/p95/p99, last-event->case-evaluated latency p50/p95, max
consumer lag (sampled every 2 s), reconciliation throughput (cases/s), drain time, daily batch
duration, and a `docker stats` snapshot when the Docker CLI is available.

```bash
docker compose up -d
docker compose stop mybell-producer processor-producer billing-producer
docker compose run --rm tools python -m scripts.run_load_test --payments 20000 --rate 300 --burst-rate 1500 --burst-seconds 10
```

## 2. Test environment

| Item | Value |
|---|---|
| Host | Laptop, Intel Core i5-1245U (10 cores / 12 threads), 15.7 GB RAM, Windows, Docker Desktop (WSL 2 backend, 12 vCPU / 10.7 GB assigned) |
| Containers | Single-broker Kafka 3.9.1 (KRaft), PostgreSQL 17, 1 ingestion, 1 reconciliation worker, batch scheduler; simulators stopped during the runs |
| Configuration | Repository defaults (`INGESTION_BATCH_SIZE` 500, 6 partitions per topic, `KAFKA_BROKER_SESSION_TIMEOUT_MS` 30000) |
| Background load | **Not isolated.** An unrelated Docker stack (Airflow, Superset, Flink, Kafka UI...) ran on the same machine and regularly used 1-3 vCPUs (`docker stats` showed e.g. Superset 118-295 %, Airflow scheduler 60-114 %). The database already held ~35,500 Bronze rows from the demo/backlog. |
| Date | 2026-10-28 |

Because of the shared, CPU-contended host the numbers below are a **lower bound for this code on this
laptop**, not a capacity statement for production hardware.

## 3. Measured results

Raw JSON for each run is in `load_test_results/<run_id>.json` (git-ignored; regenerate with the commands
above). Kafka->Bronze latency = PostgreSQL `ingestion_time` minus the Kafka record timestamp.
"Last event -> case" = case `last_evaluated_at` minus the ingestion time of the payment's last event.

| Metric | Run A: steady at target (`LT2E339461`) | Run B: burst + 3x target (`LT870E7FA8`) |
|---|---|---|
| Command | `--payments 2000 --rate 35` | `--payments 4000 --rate 100 --burst-rate 300 --burst-seconds 20` |
| Events published (all 3 sources, incl. injected duplicates/invalid) | 10,688 | 21,562 |
| Producer throughput (acked) | 35.0 ev/s, 0 failed | 122.8 ev/s, 0 failed |
| Ingestion throughput (first->last Bronze row) | **35.0 ev/s** (kept pace) | **38.9 ev/s** (did not keep pace) |
| Kafka->Bronze latency p50 / p95 / p99 | **1.1 s / 2.5 s / 3.1 s** | 111 s / 344 s / 369 s (queueing behind the backlog) |
| Max consumer lag (sampled every 2 s) | 191 messages | 11,274 messages |
| Time until fully drained (from publish start) | 309.6 s (publish took 305.4 s) | 560.9 s (publish took 175.6 s) |
| Reconciliation: cases / throughput | 2,000 cases, 6.7 cases/s | 4,000 cases, 7.4 cases/s |
| Last event -> case evaluated p50 / p95 | 0.67 s / 1.94 s | 6.8 s / 14.0 s |
| Quarantined / duplicates detected | 29 / 196 | 68 / 443 |
| Final statuses | 1,836 MATCHED, 114 NO_FINANCIAL_EFFECT, 15 AMOUNT_MISMATCH, 6 MISSING_PROCESSOR_RECORD, 2 MISSING_BILLING_POSTING, 27 PENDING_MATCH | 3,689 MATCHED, 225 NO_FINANCIAL_EFFECT, 25 AMOUNT_MISMATCH, 21 MISSING_BILLING_POSTING, 13 MISSING_PROCESSOR_RECORD, 10 MISSING_APPLICATION_RECORD, 16 PENDING_MATCH, 1 REFUND_PENDING |
| Daily batch for the business date (all payments of the day, not only the run) | 88.5 s for 12,650 payments | 163.0 s for 10,650 payments |

Notes:

* The non-MATCHED statuses are the faults the generator injects on purpose (`--failure-rate 0.01`,
  invalid/duplicate/delayed events); `PENDING_MATCH` cases are still inside their waiting period when
  the run ends. The batch log line "data quality threshold exceeded" is expected for this injected
  fault mix.
* `docker stats` could not be captured from inside the tools container. A host-side snapshot taken while
  the ingestion service drained a ~30k-message backlog (no load-test producer running) showed:
  PostgreSQL 94-152 % CPU, ingestion 31-105 %, reconciliation 52-55 %, Kafka 12-24 % (665-700 MiB),
  ingestion 115-128 MiB, PostgreSQL ~100-320 MiB. During that drain the ingestion metrics reported
  65 committed batches with an average DB write time of 7.8 s per 500-record batch (~15 ms per event),
  i.e. ~58 ev/s at best.

### 3.1 Interpretation

* **3 million events/day average (34.7 ev/s): met in a 5-minute steady run** with ~1-3 s end-to-end
  ingestion latency and bounded lag. This is a short run, not a 24-hour soak test, and it was measured on a
  contended laptop.
* **Headroom is small on this machine.** The highest sustained ingestion rate observed was ~39 ev/s during
  run B and ~58 ev/s during the backlog drain. Bursts above that are absorbed by Kafka as lag and drained
  afterwards without loss or duplication (every run ended with `raw_rows == published` and
  reconciliation complete), but latency grows to minutes.
* **Bottleneck: the ingestion transaction, bound by PostgreSQL CPU and per-record round trips.** Each
  event is written inside its own savepoint (raw insert, dedup keys, Silver insert, work-queue upsert),
  which isolates poison records but costs several round trips per event. PostgreSQL was the busiest
  container in every snapshot. Kafka was never the limiting factor.
* **Reconciliation keeps up** with ingestion (p95 last-event->case under 2 s at the target rate); its
  throughput is bounded by ingestion, not the other way round.
* **Not extrapolated:** no claim is made about multi-broker Kafka, multiple ingestion instances or
  dedicated database hardware - those were not measured.

## 4. Tuning options (correctness preserved)

| Lever | Where | Effect / caveat |
|---|---|---|
| Partitions | `TOPIC_PARTITIONS` | More partitions allow more ingestion consumers in the group; keys stay `payment_id`, so per-payment ordering within a topic is unchanged |
| Ingestion batch size | `INGESTION_BATCH_SIZE` | Larger batches amortize the per-transaction commit and offset commit; a failed batch is retried as a whole (records are idempotent) |
| More ingestion instances | `docker compose up -d --scale ingestion=N` | Consumer-group rebalancing distributes partitions; sorted queue upserts avoid lock-order deadlocks |
| More reconciliation workers | `--scale reconciliation=N`, `RECON_WORKER_BATCH_SIZE` | `FOR UPDATE SKIP LOCKED` + case row locks keep outcomes consistent |
| Connection pool | `DB_POOL_MIN_SIZE`, `DB_POOL_MAX_SIZE` | Size to workers x concurrency; PostgreSQL `max_connections=200` locally |
| Indexes | migrations 0001-0003 | Payment-id indexes on every Silver table back the per-payment snapshot load; partial indexes on open queue/outbox rows |
| Transaction size | batch size / worker batch size | Bigger transactions = fewer commits but longer lock hold times on queue rows |
| Producer batching | `linger.ms`, `compression.type=lz4` | Higher linger increases throughput at the cost of a few ms latency |
| Gold layer | views -> materialized views/partitioned tables | Needed for long histories; refresh from the batch job |

Things deliberately **not** done for speed: disabling `acks=all`/idempotence, auto-committing
offsets, skipping deduplication, or comparing amounts approximately.
