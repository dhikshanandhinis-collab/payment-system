-- Gold analytics layer: reporting views over Silver and reconciliation tables.
--
-- Views (not materialized tables) are used so reports are never stale and a rerun can never
-- double-count: every metric is recomputed from deduplicated Silver rows or from the single
-- reconciliation case per payment. At higher volumes the same SQL can back materialized views
-- refreshed by the batch job (see docs/performance_testing.md).
--
-- {{schema}}, {{app_role}} and {{reporting_role}} are substituted by the migration runner.
-- Business dates use reporting_settings.business_timezone; all stored timestamps are UTC.

DROP VIEW IF EXISTS gold_daily_payment_summary, gold_reconciliation_exceptions, gold_financial_differences,
    gold_source_completeness, gold_processing_latency, gold_refund_summary,
    gold_reconciliation_run_summary CASCADE;

CREATE OR REPLACE FUNCTION business_date(ts TIMESTAMPTZ) RETURNS DATE
LANGUAGE sql STABLE AS $$
    SELECT (ts AT TIME ZONE (SELECT business_timezone FROM {{schema}}.reporting_settings WHERE id = 1))::date
$$;

-- 1. One row per business date and currency. Grain of the underlying data = one reconciliation case
--    per payment, so multiple processor events for the same payment are never double-counted.
CREATE VIEW gold_daily_payment_summary AS
SELECT
    c.business_date,
    c.currency,
    count(*)                                                                    AS payments,
    count(*) FILTER (WHERE c.expected_amount IS NOT NULL)                       AS payment_requests,
    coalesce(sum(c.expected_amount), 0)                                         AS payment_request_amount,
    count(*) FILTER (WHERE c.app_status = 'SUCCEEDED')                          AS app_succeeded,
    count(*) FILTER (WHERE c.app_status = 'FAILED')                             AS app_failed,
    count(*) FILTER (WHERE c.app_status IN ('UNKNOWN', 'INITIATED'))            AS app_ambiguous,
    count(*) FILTER (WHERE c.processor_outcome = 'CAPTURED')                    AS captured_payments,
    coalesce(sum(c.processor_amount), 0)                                        AS gross_captured_amount,
    count(*) FILTER (WHERE c.processor_outcome = 'DECLINED')                    AS declined_payments,
    coalesce(sum(c.refunded_amount), 0)                                         AS refunded_amount,
    coalesce(sum(c.reversed_amount), 0)                                         AS reversed_amount,
    coalesce(sum(c.processor_net_amount), 0)                                    AS net_captured_amount,
    count(*) FILTER (WHERE c.billing_amount <> 0)                               AS payments_with_ledger_effect,
    coalesce(sum(c.billing_amount), 0)                                          AS billing_posted_amount,
    count(*) FILTER (WHERE c.reconciliation_status = 'MATCHED')                 AS matched_payments,
    coalesce(sum(c.processor_net_amount) FILTER (WHERE c.reconciliation_status = 'MATCHED'), 0)
                                                                                AS matched_net_amount,
    count(*) FILTER (WHERE c.reconciliation_status = 'NO_FINANCIAL_EFFECT')     AS no_financial_effect_payments,
    count(*) FILTER (WHERE c.reconciliation_status IN ('PENDING_MATCH', 'REFUND_PENDING', 'REVERSAL_PENDING'))
                                                                                AS pending_payments,
    count(*) FILTER (WHERE c.reconciliation_status NOT IN ('MATCHED', 'NO_FINANCIAL_EFFECT',
        'PENDING_MATCH', 'REFUND_PENDING', 'REVERSAL_PENDING'))                AS unmatched_payments,
    coalesce(sum(coalesce(c.expected_amount, c.processor_amount)) FILTER (WHERE c.reconciliation_status NOT IN
        ('MATCHED', 'NO_FINANCIAL_EFFECT', 'PENDING_MATCH', 'REFUND_PENDING', 'REVERSAL_PENDING')), 0)
                                                                                AS unmatched_amount,
    coalesce(sum(abs(c.difference_amount)), 0)                                  AS absolute_difference_amount
FROM reconciliation_cases c
GROUP BY c.business_date, c.currency;

-- 2. Open exceptions (and manual review holds) with age, oldest first.
CREATE VIEW gold_reconciliation_exceptions AS
SELECT
    c.reconciliation_case_id,
    c.payment_id,
    c.business_date,
    c.payment_method,
    c.currency,
    c.reconciliation_status,
    c.computed_status,
    c.reason_code,
    c.expected_amount,
    c.processor_amount,
    c.processor_net_amount,
    c.billing_amount,
    c.difference_amount,
    c.exception_created_at,
    round(extract(epoch FROM (now() - coalesce(c.exception_created_at, c.first_seen_at))) / 3600.0, 2)
                                                                                AS age_hours,
    c.last_evaluated_at,
    c.rule_version
FROM reconciliation_cases c
WHERE c.reconciliation_status IN ('AMOUNT_MISMATCH', 'STATUS_MISMATCH', 'MISSING_PROCESSOR_RECORD',
    'MISSING_BILLING_POSTING', 'MISSING_APPLICATION_RECORD', 'DUPLICATE_FINANCIAL_EFFECT',
    'DATA_QUALITY_ERROR', 'UNDER_REVIEW');

-- 3. Financial differences grouped by the comparison that failed.
CREATE VIEW gold_financial_differences AS
SELECT
    c.business_date,
    c.currency,
    CASE
        WHEN c.reason_code LIKE 'APP_PROCESSOR%' THEN 'APPLICATION_VS_PROCESSOR'
        WHEN c.reason_code IN ('PROCESSOR_LEDGER_AMOUNT_MISMATCH', 'PAYMENT_CREDIT_MISSING',
             'PAYMENT_CREDIT_UNCONFIRMED') THEN 'PROCESSOR_VS_LEDGER'
        WHEN c.reason_code LIKE 'REFUND%' OR c.reason_code LIKE 'REVERSAL%' THEN 'REFUND_REVERSAL_VS_LEDGER'
        WHEN c.reason_code LIKE 'DUPLICATE%' THEN 'DUPLICATE_FINANCIAL_EFFECT'
        WHEN c.reason_code IN ('PROCESSOR_RECORD_MISSING', 'CAPTURE_MISSING') THEN 'APPLICATION_VS_PROCESSOR'
        WHEN c.reason_code = 'APPLICATION_RECORD_MISSING' THEN 'MISSING_APPLICATION'
        ELSE 'OTHER'
    END                                         AS comparison,
    c.reconciliation_status,
    c.reason_code,
    count(*)                                    AS case_count,
    sum(c.difference_amount)                    AS net_difference_amount,
    sum(abs(c.difference_amount))               AS absolute_difference_amount
FROM reconciliation_cases c
WHERE c.computed_status NOT IN ('MATCHED', 'NO_FINANCIAL_EFFECT', 'PENDING_MATCH', 'REFUND_PENDING', 'REVERSAL_PENDING')
GROUP BY 1, 2, 3, 4, 5;

-- 4. Source completeness per business date (see docs/business_rules.md for denominators).
CREATE VIEW gold_source_completeness AS
WITH base AS (
    SELECT
        c.business_date,
        c.expected_amount IS NOT NULL                                      AS has_app,
        c.processor_outcome IN ('CAPTURED', 'DECLINED')                    AS has_processor_outcome,
        c.processor_outcome = 'CAPTURED'                                   AS captured,
        jsonb_array_length(c.source_refs -> 'ledger_entry_ids') > 0        AS has_ledger,
        c.reason_code = 'APP_FAILED_NO_PROCESSOR_ACTIVITY'                 AS app_failed_before_processor
    FROM reconciliation_cases c
)
SELECT
    business_date,
    count(*)                                                                         AS payments,
    count(*) FILTER (WHERE has_app)                                                  AS with_application_record,
    count(*) FILTER (WHERE has_processor_outcome)                                    AS with_processor_outcome,
    count(*) FILTER (WHERE captured)                                                 AS with_capture,
    count(*) FILTER (WHERE captured AND has_ledger)                                  AS captured_with_ledger_record,
    count(*) FILTER (WHERE has_app AND has_processor_outcome AND (NOT captured OR has_ledger))
                                                                                     AS complete_payments,
    round(count(*) FILTER (WHERE has_app)::numeric / nullif(count(*), 0), 4)         AS application_completeness,
    round(count(*) FILTER (WHERE has_app AND has_processor_outcome)::numeric
          / nullif(count(*) FILTER (WHERE has_app AND NOT app_failed_before_processor), 0), 4)
                                                                                     AS processor_completeness,
    round(count(*) FILTER (WHERE captured AND has_ledger)::numeric
          / nullif(count(*) FILTER (WHERE captured), 0), 4)                          AS ledger_completeness
FROM base
GROUP BY business_date;

-- 5. Processing latency by UTC hour, source and stage (milliseconds).
CREATE VIEW gold_processing_latency AS
WITH raw AS (
    SELECT
        date_trunc('hour', r.ingestion_time)                                       AS hour_utc,
        r.source_system,
        extract(epoch FROM (r.ingestion_time - r.kafka_timestamp)) * 1000          AS kafka_to_bronze_ms,
        extract(epoch FROM (r.ingestion_time - r.event_time)) * 1000               AS event_to_bronze_ms,
        (r.ingestion_time - r.event_time) > make_interval(secs => s.late_event_threshold_seconds) AS is_late
    FROM raw_payment_events r
    CROSS JOIN reporting_settings s
    WHERE r.processing_status <> 'QUARANTINED'
), reconciled AS (
    SELECT
        date_trunc('hour', c.last_evaluated_at)                                    AS hour_utc,
        extract(epoch FROM (c.last_evaluated_at - l.last_ingested)) * 1000         AS bronze_to_case_ms
    FROM reconciliation_cases c
    JOIN LATERAL (
        SELECT max(r.ingestion_time) AS last_ingested
        FROM raw_payment_events r
        WHERE r.payment_id = c.payment_id AND r.processing_status = 'PROCESSED'
    ) l ON l.last_ingested IS NOT NULL AND c.last_evaluated_at >= l.last_ingested
)
SELECT hour_utc, source_system, 'KAFKA_TO_BRONZE' AS stage, count(*) AS events,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY kafka_to_bronze_ms)::numeric, 1) AS p50_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY kafka_to_bronze_ms)::numeric, 1) AS p95_ms,
       round(max(kafka_to_bronze_ms)::numeric, 1) AS max_ms,
       NULL::bigint AS late_events
FROM raw WHERE kafka_to_bronze_ms IS NOT NULL GROUP BY 1, 2
UNION ALL
SELECT hour_utc, source_system, 'SOURCE_EVENT_TO_BRONZE', count(*),
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY event_to_bronze_ms)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY event_to_bronze_ms)::numeric, 1),
       round(max(event_to_bronze_ms)::numeric, 1),
       count(*) FILTER (WHERE is_late)
FROM raw WHERE event_to_bronze_ms IS NOT NULL GROUP BY 1, 2
UNION ALL
SELECT hour_utc, 'all', 'LAST_EVENT_TO_RECONCILED', count(*),
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY bronze_to_case_ms)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY bronze_to_case_ms)::numeric, 1),
       round(max(bronze_to_case_ms)::numeric, 1),
       NULL
FROM reconciled GROUP BY 1;

-- 6. Refunds and reversals by business date of the processor event. Each refund/reversal id is
--    counted once (Silver primary keys), and only ledger debits that reference it are "posted".
CREATE VIEW gold_refund_summary AS
WITH refunds AS (
    SELECT business_date(r.event_time) AS business_date, r.currency, 'REFUND'::text AS kind, r.refund_id AS id,
           r.amount, r.payment_id,
           EXISTS (SELECT 1 FROM billing_ledger_entries l
                   WHERE l.entry_type = 'REFUND_DEBIT' AND l.processor_reference = r.refund_id
                     AND l.posting_status <> 'REJECTED') AS posted
    FROM payment_refunds r
), reversals AS (
    SELECT business_date(v.event_time), v.currency, 'REVERSAL'::text, v.reversal_id, v.amount, v.payment_id,
           EXISTS (SELECT 1 FROM billing_ledger_entries l
                   WHERE l.entry_type = 'REVERSAL' AND l.processor_reference = v.reversal_id
                     AND l.posting_status <> 'REJECTED')
    FROM payment_reversals v
), combined AS (
    SELECT * FROM refunds UNION ALL SELECT * FROM reversals
)
SELECT
    x.business_date,
    x.currency,
    x.kind,
    count(*)                                                      AS transactions,
    sum(x.amount)                                                 AS amount,
    count(DISTINCT x.payment_id)                                  AS payments_affected,
    count(DISTINCT x.payment_id) FILTER (WHERE x.kind = 'REFUND' AND c.processor_net_amount = 0
                                          AND c.processor_amount > 0)  AS fully_refunded_payments,
    count(DISTINCT x.payment_id) FILTER (WHERE x.kind = 'REFUND' AND c.processor_net_amount > 0)
                                                                  AS partially_refunded_payments,
    count(*) FILTER (WHERE x.posted)                              AS posted_to_ledger,
    count(*) FILTER (WHERE NOT x.posted)                          AS not_posted_to_ledger,
    coalesce(sum(x.amount) FILTER (WHERE NOT x.posted), 0)        AS not_posted_amount
FROM combined x
LEFT JOIN reconciliation_cases c ON c.payment_id = x.payment_id
GROUP BY x.business_date, x.currency, x.kind;

-- 7. Batch run summary.
CREATE VIEW gold_reconciliation_run_summary AS
SELECT
    r.run_id,
    r.run_type,
    r.business_date,
    r.status,
    r.rule_version,
    r.triggered_by,
    r.started_at,
    r.finished_at,
    round(extract(epoch FROM (coalesce(r.finished_at, now()) - r.started_at))::numeric, 3) AS duration_seconds,
    r.payments_evaluated,
    r.status_changes,
    r.status_counts,
    (SELECT count(*) FROM data_quality_results d WHERE d.run_id = r.run_id AND NOT d.passed) AS failed_quality_checks,
    r.error_message
FROM reconciliation_run_history r;

GRANT EXECUTE ON FUNCTION business_date(TIMESTAMPTZ) TO {{app_role}}, {{reporting_role}};
GRANT SELECT ON gold_daily_payment_summary, gold_reconciliation_exceptions, gold_financial_differences,
    gold_source_completeness, gold_processing_latency, gold_refund_summary, gold_reconciliation_run_summary
    TO {{app_role}}, {{reporting_role}};
