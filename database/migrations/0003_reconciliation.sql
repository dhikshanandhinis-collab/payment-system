-- 0003: Reconciliation cases, append-only audit history, work queue, outbox and batch control tables.

CREATE TABLE reconciliation_rule_versions (
    rule_version    TEXT PRIMARY KEY,
    config_hash     CHAR(64)    NOT NULL,
    config_json     JSONB       NOT NULL,
    description     TEXT        NOT NULL,
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE reconciliation_cases (
    reconciliation_case_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id             TEXT          NOT NULL UNIQUE,
    business_date          DATE          NOT NULL,
    payment_method         TEXT,
    currency               CHAR(3),
    reconciliation_status  TEXT          NOT NULL,
    computed_status        TEXT          NOT NULL,
    reason_code            TEXT          NOT NULL,
    app_status             TEXT,
    processor_outcome      TEXT          NOT NULL,
    expected_amount        NUMERIC(14,2),
    processor_amount       NUMERIC(14,2) NOT NULL DEFAULT 0,
    refunded_amount        NUMERIC(14,2) NOT NULL DEFAULT 0,
    reversed_amount        NUMERIC(14,2) NOT NULL DEFAULT 0,
    processor_net_amount   NUMERIC(14,2) NOT NULL DEFAULT 0,
    billing_amount         NUMERIC(14,2) NOT NULL DEFAULT 0,
    billing_pending_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    difference_amount      NUMERIC(14,2) NOT NULL DEFAULT 0,
    source_refs            JSONB         NOT NULL DEFAULT '{}'::jsonb,
    findings               JSONB         NOT NULL DEFAULT '[]'::jsonb,
    reference_time         TIMESTAMPTZ   NOT NULL,
    first_seen_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_evaluated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    next_evaluation_at     TIMESTAMPTZ,
    exception_created_at   TIMESTAMPTZ,
    resolved_at            TIMESTAMPTZ,
    resolution_reason      TEXT,
    review_hold            BOOLEAN       NOT NULL DEFAULT FALSE,
    rule_version           TEXT          NOT NULL REFERENCES reconciliation_rule_versions (rule_version),
    evaluation_count       INTEGER       NOT NULL DEFAULT 0,
    version                INTEGER       NOT NULL DEFAULT 0,
    created_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT reconciliation_cases_status_ck CHECK (reconciliation_status IN (
        'PENDING_MATCH', 'REFUND_PENDING', 'REVERSAL_PENDING', 'MATCHED', 'NO_FINANCIAL_EFFECT',
        'AMOUNT_MISMATCH', 'STATUS_MISMATCH', 'MISSING_PROCESSOR_RECORD', 'MISSING_BILLING_POSTING',
        'MISSING_APPLICATION_RECORD', 'DUPLICATE_FINANCIAL_EFFECT', 'DATA_QUALITY_ERROR', 'UNDER_REVIEW')),
    CONSTRAINT reconciliation_cases_computed_ck CHECK (computed_status <> 'UNDER_REVIEW'),
    CONSTRAINT reconciliation_cases_hold_ck CHECK (review_hold = (reconciliation_status = 'UNDER_REVIEW')),
    CONSTRAINT reconciliation_cases_resolved_ck CHECK (
        (resolved_at IS NOT NULL) = (reconciliation_status IN ('MATCHED', 'NO_FINANCIAL_EFFECT')))
);

COMMENT ON COLUMN reconciliation_cases.computed_status IS
    'Status produced by the engine. Equals reconciliation_status unless an operator placed the case on review hold.';
COMMENT ON COLUMN reconciliation_cases.difference_amount IS
    'Observed minus expected amount for the comparison that determined the status (0 when matched).';

CREATE INDEX reconciliation_cases_status_idx ON reconciliation_cases (reconciliation_status);
CREATE INDEX reconciliation_cases_business_date_idx ON reconciliation_cases (business_date, reconciliation_status);
CREATE INDEX reconciliation_cases_next_eval_idx ON reconciliation_cases (next_evaluation_at)
    WHERE next_evaluation_at IS NOT NULL;

CREATE TABLE reconciliation_history (
    history_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reconciliation_case_id UUID        NOT NULL REFERENCES reconciliation_cases (reconciliation_case_id),
    payment_id             TEXT        NOT NULL,
    previous_status        TEXT,
    new_status             TEXT        NOT NULL,
    previous_reason_code   TEXT,
    reason_code            TEXT        NOT NULL,
    computed_status        TEXT        NOT NULL,
    rule_version           TEXT        NOT NULL,
    trigger_type           TEXT        NOT NULL CHECK (trigger_type IN
        ('STREAM', 'TIMEOUT_SWEEP', 'BATCH', 'BACKFILL', 'ADMIN_REPROCESS', 'ADMIN_REVIEW_HOLD', 'ADMIN_REVIEW_RELEASE')),
    run_id                 TEXT,
    actor                  TEXT        NOT NULL DEFAULT 'system',
    source_refs            JSONB       NOT NULL,
    amounts                JSONB       NOT NULL,
    findings               JSONB       NOT NULL DEFAULT '[]'::jsonb,
    note                   TEXT,
    evaluated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX reconciliation_history_case_idx ON reconciliation_history (reconciliation_case_id, history_id);
CREATE INDEX reconciliation_history_payment_idx ON reconciliation_history (payment_id);
CREATE INDEX reconciliation_history_time_idx ON reconciliation_history (evaluated_at);

-- History and admin actions are append-only, even for the table owner.
CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; % is not permitted', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER reconciliation_history_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON reconciliation_history
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();

CREATE TABLE admin_actions (
    action_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action_type    TEXT        NOT NULL CHECK (action_type IN
        ('REPROCESS_CASE', 'PLACE_REVIEW_HOLD', 'RELEASE_REVIEW_HOLD', 'REPLAY_QUARANTINE', 'DISCARD_QUARANTINE',
         'REEVALUATE_OPEN_CASES')),
    actor          TEXT        NOT NULL,
    reason         TEXT        NOT NULL CHECK (length(reason) >= 10),
    target         TEXT        NOT NULL,
    details        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER admin_actions_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON admin_actions
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();

CREATE TABLE reconciliation_work_queue (
    payment_id       TEXT PRIMARY KEY,
    trigger_type     TEXT        NOT NULL,
    first_enqueued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_enqueued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    available_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    attempts         INTEGER     NOT NULL DEFAULT 0,
    last_error       TEXT
);
CREATE INDEX reconciliation_work_queue_available_idx ON reconciliation_work_queue (available_at);

CREATE TABLE reconciliation_outbox (
    outbox_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id               UUID        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    reconciliation_case_id UUID        NOT NULL REFERENCES reconciliation_cases (reconciliation_case_id),
    payment_id             TEXT        NOT NULL,
    event_type             TEXT        NOT NULL,
    payload                JSONB       NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at           TIMESTAMPTZ
);
CREATE INDEX reconciliation_outbox_unpublished_idx ON reconciliation_outbox (outbox_id) WHERE published_at IS NULL;

CREATE TABLE reconciliation_run_history (
    run_id            UUID PRIMARY KEY,
    run_type          TEXT        NOT NULL CHECK (run_type IN ('DAILY', 'BACKFILL')),
    business_date     DATE        NOT NULL,
    business_timezone TEXT        NOT NULL,
    window_start_utc  TIMESTAMPTZ NOT NULL,
    window_end_utc    TIMESTAMPTZ NOT NULL,
    status            TEXT        NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    rule_version      TEXT        NOT NULL,
    triggered_by      TEXT        NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at       TIMESTAMPTZ,
    payments_evaluated INTEGER    NOT NULL DEFAULT 0,
    status_changes    INTEGER     NOT NULL DEFAULT 0,
    status_counts     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    summary           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error_message     TEXT,
    CONSTRAINT reconciliation_run_finished_ck CHECK ((status = 'RUNNING') = (finished_at IS NULL))
);
CREATE INDEX reconciliation_run_history_date_idx ON reconciliation_run_history (business_date, started_at);

CREATE TABLE source_control_totals (
    business_date  DATE          NOT NULL,
    source_system  TEXT          NOT NULL,
    metric_name    TEXT          NOT NULL,
    currency       CHAR(3)       NOT NULL DEFAULT 'XXX',
    record_count   BIGINT        NOT NULL CHECK (record_count >= 0),
    amount         NUMERIC(18,2) NOT NULL,
    run_id         UUID          NOT NULL REFERENCES reconciliation_run_history (run_id),
    computed_at    TIMESTAMPTZ   NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (business_date, source_system, metric_name, currency)
);
COMMENT ON TABLE source_control_totals IS
    'Latest control totals per business date. Re-running a date replaces (not adds to) its totals. currency XXX = not applicable.';

CREATE TABLE data_quality_results (
    run_id         UUID          NOT NULL REFERENCES reconciliation_run_history (run_id),
    business_date  DATE          NOT NULL,
    check_name     TEXT          NOT NULL,
    observed_value NUMERIC(12,6),
    threshold      NUMERIC(12,6),
    comparison     TEXT          NOT NULL CHECK (comparison IN ('<=', '>=')),
    passed         BOOLEAN       NOT NULL,
    numerator      BIGINT,
    denominator    BIGINT,
    details        JSONB         NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (run_id, check_name)
);

CREATE TABLE reporting_settings (
    id                           SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    business_timezone            TEXT     NOT NULL,
    late_event_threshold_seconds INTEGER  NOT NULL CHECK (late_event_threshold_seconds >= 0),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);
