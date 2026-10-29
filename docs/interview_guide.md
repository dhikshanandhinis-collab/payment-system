# Interview Guide

A structured walkthrough for explaining this project. Keep the framing honest: *"an independent,
production-oriented reconciliation platform, inspired by public descriptions of telecom bill payment,
running on synthetic data. It is not Bell software and I make no claims about Bell's internal systems."*

## 1. Elevator pitch (30 seconds)
Three independent systems - a customer payment app, an external payment processor and a billing
ledger - each publish events to Kafka. A Python ingestion service stores every message durably
(Bronze), validates and normalizes it (Silver), and a reconciliation engine continuously decides for
each payment whether the three sources agree, using exact decimal arithmetic and explicit references
only. Late, duplicate, out-of-order and malformed events are expected and handled. A scheduled batch
job produces control totals and data-quality checks per business date, and SQL views provide the
Gold reporting layer. Everything is idempotent, auditable, and tested end to end through real Kafka
and PostgreSQL.

## 2. Architecture talking points
* **Why Kafka?** Durable, replayable log; independent producers; partitioning by `payment_id` keeps
  a payment's events ordered within a topic. We still never assume cross-topic order.
* **Why PostgreSQL only?** 3 M events/day is ~35 events/s. A relational store gives transactions,
  constraints and row locks - exactly what financial correctness needs. Spark/Flink would add cost
  without improving correctness at this scale.
* **Why separate ingestion and reconciliation?** Ingestion must be fast and simple (store first). The
  engine can be retried, scaled (`SKIP LOCKED`) and re-run (batch, admin) independently.
* **Stateless engine over a full snapshot.** Each evaluation recomputes from all known records, so
  arrival order doesn't matter, late events just trigger re-evaluation, and the same code serves
  streaming, timeouts, batch and backfill.

## 3. Delivery guarantees (be precise)
* Consumer: at-least-once; offsets committed **after** the DB commit (async commit request, so a missing
  group coordinator cannot freeze the poll loop - a real incident on the local broker).
* DB effect: idempotent - unique `(topic, partition, offset)` in Bronze; `EVENT_ID` and
  `BUSINESS_KEY` dedup keys with content hashes (same content = duplicate, different = conflict ->
  quarantine).
* Not distributed exactly-once. Explain the crash window between DB commit and offset commit and
  how redelivery is absorbed (`ALREADY_INGESTED`). Demonstrated by
  `test_crash_before_offset_commit_and_restart_is_idempotent`.
* Output: transactional outbox -> at-least-once to `payment.reconciliation.events`.

## 4. Financial correctness
* Amounts are decimal strings in JSON, `Decimal` in Python, `NUMERIC(14,2)` in SQL. JSON numbers are
  rejected so no producer can introduce floats.
* Positive magnitudes + explicit direction; signed effects derived in one place.
* Authorization != capture. Reversal cancels a capture, subtracted once. Refunds link to the
  capture; multiple partial refunds; same refund id counted once; refunds > capture is an exception.
* Ledger postings only count when effective (POSTED or confirmed); PENDING tracked separately.
* `DUPLICATE_FINANCIAL_EFFECT`: two different capture ids for one payment is a real-world risk
  (double charge) - distinct from duplicate *delivery* of the same capture.

## 5. Time and waiting periods
* Waiting periods per payment method in a versioned rule file; a case becomes an exception only
  when the waiting period expires (sweeper uses `next_evaluation_at`).
* Event time (not ingestion time) anchors deadlines so backfills behave correctly.
* Business date = UTC timestamp converted to the configured business timezone; DST days are 23/25 h
  (unit-tested).

## 6. Data quality
* Malformed data -> quarantine (raw bytes kept, error category/code, DLQ notification).
* Business exceptions -> reconciliation statuses. A decline is not bad data; a missing posting is not
  a schema error.
* Batch DQ checks with thresholds (quarantine, duplicates, late events, open exceptions,
  completeness) stored per run.
* "A high match rate is not proof of correctness" - we report differences, completeness and
  control totals alongside it.

## 7. Auditability and controls
* `reconciliation_history` and `admin_actions` are append-only (trigger blocks UPDATE/DELETE, even
  for the owner).
* Admins cannot set a status. They can re-run the engine, place a review hold, or replay corrected
  quarantined events through Kafka. All need an allow-listed actor and a reason.
* Rule versions are hashed; changing rules without bumping the version fails startup.
* Least-privilege DB roles with column-level grants (runtime can't modify raw payload bytes).

## 8. SQL highlights
* Gold views at one-case-per-payment grain avoid double counting.
* `gold_processing_latency` uses `percentile_cont` per stage.
* Control totals are replaced atomically per business date (rerun never inflates).
* Point to [analytics/reports.sql](../analytics/reports.sql) - each metric documents numerator and denominator.

## 9. Testing story
* Unit: engine decision table, lifecycle, validation codes, normalization hashes, DST windows,
  redaction, offset handling with fakes.
* Integration: migrations/tamper detection, least-privilege, append-only history, ingestion
  idempotency/conflicts, batch rerun/failure/exclusivity, concurrency, admin audit.
* End-to-end through Kafka: 18 required scenarios with status **and** amount assertions, Gold totals.
* Recovery: real DB outage (connections disabled + backends terminated), crash before offset commit.
* Results and coverage: see README "Test results".

## 10. Performance
Explain the target (35 ev/s average), the burst tests, what was measured, the hardware, and the
bottleneck you observed (see [performance_testing.md](performance_testing.md)). Don't quote numbers
that were not measured.

## 11. Trade-offs and what I'd do next
* Views vs materialized Gold tables (freshness vs cost at scale).
* Per-payment snapshot reload (simple, correct) vs incremental state (faster, riskier).
* Single consumer process (simple) vs more partitions/consumers (throughput).
* Next: schema registry (Avro/Protobuf), Kafka TLS/SASL + ACLs, CDC/archival of Bronze,
  materialized Gold refreshed by the batch job, alerting rules, CI pipeline with image scanning.

## 12. Likely questions
* *What if the processor sends the same capture twice with different event ids?* BUSINESS_KEY dedup ->
  `DUPLICATE_BUSINESS_EVENT`, counted once. If the amount differs -> quarantined as conflict.
* *What if two captures have different ids?* `DUPLICATE_FINANCIAL_EFFECT / DUPLICATE_CAPTURE` - flagged
  for humans, not silently netted.
* *What if the ledger posts late?* Case goes `PENDING_MATCH` -> `MISSING_BILLING_POSTING` when the wait
  expires -> `MATCHED` when it arrives, with `RESOLVED_FROM_MISSING_BILLING_POSTING` and full history.
* *How do you avoid two workers producing conflicting outcomes?* Unique case per payment + row lock;
  queue claims with SKIP LOCKED; batch exclusivity via advisory lock.
* *How would you replay?* Reset offsets (idempotent), replay quarantine via Kafka, or backfill dates.
