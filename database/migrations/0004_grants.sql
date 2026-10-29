-- 0004: Least-privilege grants.
-- {{schema}}, {{app_role}} and {{reporting_role}} are substituted (validated identifiers) by the migration runner.
-- The runtime role cannot run DDL, cannot delete Bronze/Silver/audit data, and can only update the
-- specific columns its workflow requires (column-level privileges).

GRANT USAGE ON SCHEMA {{schema}} TO {{app_role}}, {{reporting_role}};

-- Bronze
GRANT SELECT, INSERT ON raw_payment_events TO {{app_role}};
GRANT UPDATE (processing_status, error_code, validation_error) ON raw_payment_events TO {{app_role}};
GRANT SELECT, INSERT ON deduplication_keys TO {{app_role}};
GRANT SELECT, INSERT ON quarantined_events TO {{app_role}};
GRANT UPDATE (status, resolved_at, resolved_by, resolution_note) ON quarantined_events TO {{app_role}};

-- Silver
GRANT SELECT, INSERT ON application_payment_events, processor_transactions, payment_refunds,
    payment_reversals, billing_ledger_entries TO {{app_role}};
GRANT SELECT, INSERT ON payment_requests TO {{app_role}};
GRANT UPDATE (account_ref, amount, currency, payment_method, initiated_at, initiated_event_id,
    initiation_count, updated_at) ON payment_requests TO {{app_role}};

-- Reconciliation
GRANT SELECT, INSERT ON reconciliation_rule_versions TO {{app_role}};
GRANT SELECT, INSERT, UPDATE ON reconciliation_cases TO {{app_role}};
GRANT SELECT, INSERT ON reconciliation_history, admin_actions TO {{app_role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON reconciliation_work_queue TO {{app_role}};
GRANT SELECT, INSERT ON reconciliation_outbox TO {{app_role}};
GRANT UPDATE (published_at) ON reconciliation_outbox TO {{app_role}};
GRANT SELECT, INSERT, UPDATE ON reconciliation_run_history TO {{app_role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON source_control_totals TO {{app_role}};  -- derived; replaced atomically per date
GRANT SELECT, INSERT ON data_quality_results TO {{app_role}};
GRANT SELECT ON reporting_settings TO {{app_role}};

GRANT USAGE ON ALL SEQUENCES IN SCHEMA {{schema}} TO {{app_role}};

-- Reporting: read-only access to Silver, reconciliation results and control tables (no raw payloads).
GRANT SELECT ON payment_requests, application_payment_events, processor_transactions, payment_refunds,
    payment_reversals, billing_ledger_entries, reconciliation_cases, reconciliation_history,
    reconciliation_run_history, source_control_totals, data_quality_results, reporting_settings,
    reconciliation_rule_versions TO {{reporting_role}};
