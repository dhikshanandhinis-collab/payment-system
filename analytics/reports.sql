-- Operational and financial report queries.
-- Each query starts with "-- name: <id>" followed by "-- metric:" documentation lines.
-- Run with: python -m scripts.report <id>   (or: python -m scripts.report --all)
-- Business dates use reporting_settings.business_timezone.

-- name: payment_requests_by_day
-- metric: count of distinct payment_ids with an application payment request, by business date of initiation.
-- metric: amount = sum of requested amounts (customer intent, not money moved).
SELECT business_date(initiated_at) AS business_date, currency, count(*) AS payment_requests, sum(amount) AS requested_amount
FROM payment_requests GROUP BY 1, 2 ORDER BY 1 DESC, 2;

-- name: successful_captures
-- metric: distinct processor CAPTURE transactions by business date of capture (duplicate re-sends are excluded in Silver).
-- metric: numerator only; payments with more than one distinct capture are reported separately as DUPLICATE_FINANCIAL_EFFECT.
SELECT business_date(event_time) AS business_date, currency, count(*) AS captures, count(DISTINCT payment_id) AS payments,
       sum(amount) AS captured_amount
FROM processor_transactions WHERE transaction_type = 'CAPTURE' GROUP BY 1, 2 ORDER BY 1 DESC, 2;

-- name: declined_transactions
-- metric: payments with a processor DECLINE and no capture, by business date; decline rate = declined / payments with a processor outcome.
SELECT business_date, count(*) FILTER (WHERE processor_outcome = 'DECLINED') AS declined_payments,
       count(*) FILTER (WHERE processor_outcome IN ('CAPTURED', 'DECLINED')) AS payments_with_outcome,
       round(count(*) FILTER (WHERE processor_outcome = 'DECLINED')::numeric
             / nullif(count(*) FILTER (WHERE processor_outcome IN ('CAPTURED', 'DECLINED')), 0), 4) AS decline_rate
FROM reconciliation_cases GROUP BY 1 ORDER BY 1 DESC;

-- name: ledger_postings
-- metric: ledger entries by business date and type; "effective" = POSTED or confirmed by POSTING_CONFIRMATION.
SELECT business_date(l.event_time) AS business_date, l.entry_type, l.direction, count(*) AS entries,
       count(*) FILTER (WHERE l.posting_status = 'POSTED' OR EXISTS (SELECT 1 FROM billing_ledger_entries c
           WHERE c.entry_type = 'POSTING_CONFIRMATION' AND c.original_ledger_entry_id = l.ledger_entry_id)) AS effective_entries,
       sum(l.amount) AS amount
FROM billing_ledger_entries l GROUP BY 1, 2, 3 ORDER BY 1 DESC, 2, 3;

-- name: matched_payments
-- metric: payments whose case status is MATCHED; amount = net captured (gross - refunds - reversals) of those payments.
-- metric: rate denominator = payments past their waiting periods (excludes pending). A high match rate is not proof all records are correct.
SELECT business_date, matched_payments, matched_net_amount, no_financial_effect_payments, pending_payments,
       round(matched_payments::numeric / nullif(payments - pending_payments, 0), 4) AS match_rate_excluding_pending
FROM gold_daily_payment_summary ORDER BY business_date DESC;

-- name: unmatched_payments
-- metric: payments in an exception or review status; amount = requested amount (or captured amount if no request exists).
SELECT business_date, currency, unmatched_payments, unmatched_amount, absolute_difference_amount
FROM gold_daily_payment_summary ORDER BY business_date DESC;

-- name: amount_mismatches_by_source
-- metric: AMOUNT_MISMATCH cases grouped by which comparison failed; net and absolute differences (observed - expected).
SELECT business_date, comparison, reason_code, case_count, net_difference_amount, absolute_difference_amount
FROM gold_financial_differences WHERE reconciliation_status = 'AMOUNT_MISMATCH'
ORDER BY business_date DESC, absolute_difference_amount DESC;

-- name: oldest_unresolved_exceptions
-- metric: open exception cases ordered by age since the exception was raised.
SELECT payment_id, reconciliation_status, reason_code, payment_method, expected_amount, processor_amount,
       billing_amount, difference_amount, exception_created_at, age_hours
FROM gold_reconciliation_exceptions ORDER BY exception_created_at NULLS LAST LIMIT 25;

-- name: exceptions_by_payment_method
-- metric: open exceptions per payment method and status; rate = exceptions / all cases for that method.
SELECT c.payment_method, c.reconciliation_status, count(*) AS exceptions,
       round(count(*)::numeric / nullif((SELECT count(*) FROM reconciliation_cases x
            WHERE x.payment_method IS NOT DISTINCT FROM c.payment_method), 0), 4) AS exception_rate
FROM reconciliation_cases c
WHERE c.reconciliation_status NOT IN ('MATCHED', 'NO_FINANCIAL_EFFECT', 'PENDING_MATCH', 'REFUND_PENDING', 'REVERSAL_PENDING')
GROUP BY 1, 2 ORDER BY 3 DESC;

-- name: duplicate_delivery_rates
-- metric: numerator = raw events detected as duplicate delivery (same event_id) or duplicate business event
-- metric: (same processor/ledger transaction id under a new event_id); denominator = all raw events for the source.
SELECT source_system, count(*) AS raw_events,
       count(*) FILTER (WHERE processing_status = 'DUPLICATE_DELIVERY') AS duplicate_deliveries,
       count(*) FILTER (WHERE processing_status = 'DUPLICATE_BUSINESS_EVENT') AS duplicate_business_events,
       round(count(*) FILTER (WHERE processing_status IN ('DUPLICATE_DELIVERY', 'DUPLICATE_BUSINESS_EVENT'))::numeric
             / nullif(count(*), 0), 4) AS duplicate_rate
FROM raw_payment_events GROUP BY 1 ORDER BY 1;

-- name: quarantine_reasons
-- metric: quarantined events by source, category and error code (status OPEN = awaiting operator action).
SELECT source_system, error_category, error_code, status, count(*) AS events,
       min(quarantined_at) AS first_seen, max(quarantined_at) AS last_seen
FROM quarantined_events GROUP BY 1, 2, 3, 4 ORDER BY events DESC;

-- name: refunds_and_reversals
-- metric: refunds/reversals by business date; each refund/reversal id counted once; "posted" = matching ledger debit exists.
SELECT * FROM gold_refund_summary ORDER BY business_date DESC, kind;

-- name: late_arriving_events
-- metric: numerator = valid raw events whose ingestion_time - event_time exceeds late_event_threshold_seconds;
-- metric: denominator = valid (non-quarantined) raw events, by source and UTC day of ingestion.
SELECT date_trunc('day', r.ingestion_time) AS ingestion_day_utc, r.source_system, count(*) AS events,
       count(*) FILTER (WHERE r.ingestion_time - r.event_time > make_interval(secs => s.late_event_threshold_seconds)) AS late_events,
       round(count(*) FILTER (WHERE r.ingestion_time - r.event_time > make_interval(secs => s.late_event_threshold_seconds))::numeric
             / nullif(count(*), 0), 4) AS late_event_rate
FROM raw_payment_events r CROSS JOIN reporting_settings s
WHERE r.processing_status <> 'QUARANTINED' AND r.event_time IS NOT NULL
GROUP BY 1, 2 ORDER BY 1 DESC, 2;

-- name: processing_latency
-- metric: p50/p95/max latency in ms per hour and stage (Kafka->Bronze, source event->Bronze, last event->case evaluated).
SELECT * FROM gold_processing_latency ORDER BY hour_utc DESC, stage, source_system LIMIT 60;

-- name: ingestion_throughput
-- metric: raw events committed per minute (last 60 minutes). Consumer lag is exported as the Prometheus gauge
-- metric: recon_consumer_lag_messages and can be printed with: python -m scripts.report --kafka-lag
SELECT date_trunc('minute', ingestion_time) AS minute_utc, count(*) AS events,
       round(count(*) / 60.0, 2) AS events_per_second
FROM raw_payment_events WHERE ingestion_time > now() - interval '60 minutes'
GROUP BY 1 ORDER BY 1 DESC;

-- name: source_completeness
-- metric: see gold_source_completeness; processor completeness denominator excludes payments that failed in the app before
-- metric: reaching the processor; ledger completeness denominator = captured payments.
SELECT * FROM gold_source_completeness ORDER BY business_date DESC;

-- name: daily_summary
-- metric: one row per business date and currency from the single reconciliation case per payment.
SELECT * FROM gold_daily_payment_summary ORDER BY business_date DESC, currency;

-- name: run_summary
-- metric: batch reconciliation runs with duration and failed quality checks.
SELECT run_id, run_type, business_date, status, duration_seconds, payments_evaluated, status_changes,
       failed_quality_checks, error_message
FROM gold_reconciliation_run_summary ORDER BY started_at DESC LIMIT 20;

-- name: case_history
-- metric: audit trail for the most recently changed cases.
SELECT payment_id, previous_status, new_status, reason_code, trigger_type, actor, rule_version, evaluated_at
FROM reconciliation_history ORDER BY history_id DESC LIMIT 30;
