# Data Dictionary

All tables live in the `recon` schema. Timestamps are `TIMESTAMPTZ` stored in UTC. Monetary amounts
are `NUMERIC(14,2)` (control totals `NUMERIC(18,2)`) **positive magnitudes**; direction comes from the
transaction type (processor) or the explicit `direction` column (ledger). No floating point is used
anywhere for money (Python `Decimal`, JSON decimal strings, PostgreSQL `NUMERIC`).

Source ownership legend: **APP** = MyBell-style application simulator, **PROC** = payment processor
simulator, **LEDGER** = billing ledger simulator, **PLATFORM** = produced by this platform.

## 1. Source event contracts (schema_version `1.0`)

JSON Schemas: [schemas/](../schemas). Executable models: [processing/models.py](../processing/models.py).
Unknown fields are **rejected** (not silently dropped); a new field requires a new schema version.

### Common envelope (all sources)

| Field | Type | Required | Rules |
|---|---|---|---|
| event_id | string(3-64) `[A-Za-z0-9][A-Za-z0-9_.:-]*` | yes | Unique per emitted event; re-deliveries reuse it |
| event_type | enum | yes | Source specific (below) |
| schema_version | string | yes | Only `"1.0"` supported; anything else -> `UNSUPPORTED_SCHEMA_VERSION` |
| source_system | enum | yes | Must match the topic (`mybell_app`, `payment_processor`, `billing_ledger`) |
| event_time | ISO 8601 with offset | yes | Normalized to UTC; rejected if > 300 s in the future or > 400 days old |
| correlation_id | identifier | yes | Tracing only; never used for matching |
| amount | decimal **string**, >0, <=2 dp, <=12 integer digits | yes | JSON numbers are rejected |
| currency | ISO 4217 alpha-3 | yes | Must be in the platform's ISO list |

### Application (`mybell.payment.events`, owner APP)

| Field | Notes |
|---|---|
| event_type | PAYMENT_INITIATED, PAYMENT_SUCCEEDED, PAYMENT_FAILED, PAYMENT_TIMEOUT, PAYMENT_STATUS_UPDATED |
| payment_id | Shared payment reference (matching key) |
| account_ref | Synthetic account reference (masked in logs) |
| payment_method | CREDIT_CARD, DEBIT_CARD, PRE_AUTHORIZED_DEBIT, ONLINE_BANKING |
| source_status | INITIATED / SUCCEEDED / FAILED / UNKNOWN; must agree with event_type (STATUS_UPDATED may not return to INITIATED) |

### Processor (`processor.transaction.events`, owner PROC)

| Field | Notes |
|---|---|
| event_type | AUTHORIZATION, CAPTURE, DECLINE, REVERSAL, REFUND |
| processor_transaction_id | Unique per financial transaction; a re-send of the same transaction reuses it |
| payment_id | Original payment (also on refunds/reversals) |
| original_processor_transaction_id | CAPTURE->AUTHORIZATION (optional); REFUND/REVERSAL->CAPTURE (required); forbidden on AUTHORIZATION/DECLINE |
| processor_status | AUTHORIZED / CAPTURED / DECLINED / REVERSED / REFUNDED, fixed per event_type |

### Ledger (`billing.ledger.events`, owner LEDGER)

| Field | Notes |
|---|---|
| event_type | CHARGE, PAYMENT_CREDIT, POSTING_CONFIRMATION, REFUND_DEBIT, REVERSAL, ADJUSTMENT |
| ledger_entry_id | Unique per ledger entry |
| payment_id | Required except for CHARGE |
| account_ref | Required |
| direction | CREDIT reduces the balance owed, DEBIT increases it. Fixed: CHARGE=DEBIT, PAYMENT_CREDIT=CREDIT, REFUND_DEBIT=DEBIT |
| posting_status | PENDING / POSTED / REJECTED (POSTING_CONFIRMATION must be POSTED) |
| original_ledger_entry_id | Required for POSTING_CONFIRMATION and REVERSAL |
| processor_reference | Required for REFUND_DEBIT (processor refund id); used on REVERSAL (processor reversal id) |
| adjustment_reason | Required for ADJUSTMENT |

## 2. Bronze

### raw_payment_events (owner PLATFORM; append-oriented)

| Column | Type | Null | Notes |
|---|---|---|---|
| raw_event_id | bigint identity | no | PK |
| source_system | text | no | From the topic (not trusted from payload) |
| source_topic, source_partition, source_offset | text, int, bigint | no | **UNIQUE** - durable Kafka position identity; redelivery is skipped |
| kafka_key, kafka_timestamp | text, timestamptz | yes | Broker metadata |
| source_event_id, event_type, payment_id, event_time, schema_version | | yes | Best-effort extraction; may be NULL for invalid events |
| ingestion_time | timestamptz | no | `clock_timestamp()` at insert |
| payload_bytes | bytea | no | **Original bytes, never rewritten** |
| payload_json | jsonb | yes | Parsed copy cast by PostgreSQL from the original text (exact numerics); NULL if not JSON |
| payload_hash | char(64) | no | SHA-256 of payload_bytes |
| processing_status | text | no | PROCESSED, DUPLICATE_DELIVERY, DUPLICATE_BUSINESS_EVENT, QUARANTINED |
| error_code, validation_error | text | yes | Required iff QUARANTINED (check constraint) |

Runtime role may only update `processing_status, error_code, validation_error` (column privileges).

### deduplication_keys (owner PLATFORM)

PK `(source_system, key_type, key_value)`. `key_type` = `EVENT_ID` (event re-delivery) or
`BUSINESS_KEY` (processor_transaction_id / ledger_entry_id re-sent under a new event_id).
`content_hash` distinguishes a true duplicate (same hash) from a conflict (different hash -> quarantine).

### quarantined_events (owner PLATFORM)

| Column | Notes |
|---|---|
| quarantine_id | PK |
| raw_event_id | FK raw_payment_events |
| source_topic/partition/offset | UNIQUE |
| error_category | VALIDATION, SERIALIZATION, UNSUPPORTED_SCHEMA, DUPLICATE_CONFLICT, DATABASE, PROCESSING |
| error_code, error_detail | Field names and messages only - input values are not echoed |
| payload_bytes | Original bytes |
| status | OPEN / REPLAYED / DISCARDED; `resolved_at` required iff not OPEN |

## 3. Silver

| Table | PK | Unique | Notes |
|---|---|---|---|
| payment_requests | payment_id | | First PAYMENT_INITIATED is authoritative; created from the first app event if initiation is late; `initiation_count` > 1 = client retry |
| application_payment_events | source_event_id | | Every distinct app event (status history) |
| processor_transactions | processor_transaction_id | source_event_id | AUTHORIZATION / CAPTURE / DECLINE only |
| payment_refunds | refund_id | source_event_id | `original_processor_transaction_id` NOT NULL |
| payment_reversals | reversal_id | source_event_id | Processor reversals of a capture |
| billing_ledger_entries | ledger_entry_id | source_event_id | All ledger entry types; `payment_id` NULL only for CHARGE |

All Silver rows carry `raw_event_id` (FK to Bronze), `event_time`, `correlation_id`,
`schema_version`, `ingested_at`. Cross-source references are deliberately **not** foreign keys
(independent sources, out-of-order arrival); the engine detects orphans instead.

## 4. Reconciliation

### reconciliation_cases (one row per payment_id)

| Column | Notes |
|---|---|
| reconciliation_case_id | UUID PK |
| payment_id | UNIQUE |
| business_date | Reference time (initiation, else earliest event) in BUSINESS_TIMEZONE |
| reconciliation_status | Effective status (see business_rules.md) |
| computed_status | Engine status; differs only under a review hold |
| reason_code | Machine-readable reason (e.g. `PAYMENT_CREDIT_MISSING`) |
| app_status, processor_outcome | Source status summary (distinct from reconciliation status) |
| expected_amount | App request amount |
| processor_amount | Gross captured |
| refunded_amount, reversed_amount | Distinct refunds/reversals of this payment's capture |
| processor_net_amount | gross - refunded - reversed |
| billing_amount | Signed sum of effective ledger entries |
| billing_pending_amount | Signed sum of PENDING (unconfirmed) entries |
| difference_amount | observed - expected for the comparison that set the status |
| source_refs | JSON lists of matched source ids |
| first_seen_at, last_evaluated_at, next_evaluation_at | Timing; `next_evaluation_at` = waiting-period deadline |
| exception_created_at, resolved_at, resolution_reason | Lifecycle (see lifecycle.py) |
| review_hold | Operator hold; check constraint ties it to UNDER_REVIEW |
| rule_version | FK reconciliation_rule_versions |

### reconciliation_history (append-only - UPDATE/DELETE/TRUNCATE blocked by trigger)

previous_status, new_status, previous_reason_code, reason_code, computed_status, rule_version,
trigger_type (STREAM, TIMEOUT_SWEEP, BATCH, BACKFILL, ADMIN_*), run_id (batch run / admin action),
actor, source_refs, amounts, findings, note, evaluated_at.

### Other platform tables

| Table | Purpose |
|---|---|
| reconciliation_rule_versions | rule_version -> SHA-256 of the rule file; a changed file with the same version is refused |
| reconciliation_work_queue | Payments awaiting (re-)evaluation; claimed with `FOR UPDATE SKIP LOCKED` |
| reconciliation_outbox | Case changes awaiting publication to `payment.reconciliation.events` |
| reconciliation_run_history | One row per batch run (RUNNING -> SUCCEEDED/FAILED) |
| source_control_totals | Latest totals per (business_date, source_system, metric_name, currency); replaced atomically on rerun |
| data_quality_results | Threshold checks per run |
| admin_actions | Append-only audit of operator actions |
| reporting_settings | Business timezone and late-event threshold used by Gold views |
| schema_migrations | Applied migrations with checksums |

## 5. Gold views

See [analytics/gold_tables.sql](../analytics/gold_tables.sql); metric definitions are in
[business_rules.md](business_rules.md#metrics) and [analytics/reports.sql](../analytics/reports.sql).
