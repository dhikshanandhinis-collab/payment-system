-- 0002: Silver normalized transaction tables.
--
-- Amounts are positive magnitudes (NUMERIC(14,2)); the financial direction is given by the
-- transaction type (processor) or the explicit direction column (ledger).
-- Cross-source references (e.g. a refund's original capture) are intentionally NOT foreign keys:
-- sources are independent and events arrive out of order, so referential integrity is enforced by
-- the reconciliation engine (orphan detection) instead of rejecting valid late-arriving data.

CREATE TABLE payment_requests (
    payment_id          TEXT PRIMARY KEY,
    account_ref         TEXT          NOT NULL,
    amount              NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    currency            CHAR(3)       NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    payment_method      TEXT          NOT NULL CHECK (payment_method IN
        ('CREDIT_CARD', 'DEBIT_CARD', 'PRE_AUTHORIZED_DEBIT', 'ONLINE_BANKING')),
    initiated_at        TIMESTAMPTZ   NOT NULL,
    initiated_event_id  TEXT,
    first_seen_event_id TEXT          NOT NULL,
    initiation_count    INTEGER       NOT NULL DEFAULT 0 CHECK (initiation_count >= 0),
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

COMMENT ON TABLE payment_requests IS
    'One row per payment_id. Values come from the first PAYMENT_INITIATED event (or the first app event if initiation has not arrived yet).';
COMMENT ON COLUMN payment_requests.initiation_count IS
    'Number of distinct PAYMENT_INITIATED events (>1 = client retry / duplicate business transaction).';

CREATE TABLE application_payment_events (
    source_event_id     TEXT PRIMARY KEY,
    raw_event_id        BIGINT        NOT NULL REFERENCES raw_payment_events (raw_event_id),
    payment_id          TEXT          NOT NULL,
    event_type          TEXT          NOT NULL CHECK (event_type IN ('PAYMENT_INITIATED', 'PAYMENT_SUCCEEDED',
        'PAYMENT_FAILED', 'PAYMENT_TIMEOUT', 'PAYMENT_STATUS_UPDATED')),
    source_status       TEXT          NOT NULL CHECK (source_status IN ('INITIATED', 'SUCCEEDED', 'FAILED', 'UNKNOWN')),
    account_ref         TEXT          NOT NULL,
    payment_method      TEXT          NOT NULL,
    amount              NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    currency            CHAR(3)       NOT NULL,
    event_time          TIMESTAMPTZ   NOT NULL,
    correlation_id      TEXT          NOT NULL,
    schema_version      TEXT          NOT NULL,
    ingested_at         TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX application_payment_events_payment_idx ON application_payment_events (payment_id);
CREATE INDEX application_payment_events_time_idx ON application_payment_events (event_time);

CREATE TABLE processor_transactions (
    processor_transaction_id          TEXT PRIMARY KEY,
    source_event_id                   TEXT          NOT NULL UNIQUE,
    raw_event_id                      BIGINT        NOT NULL REFERENCES raw_payment_events (raw_event_id),
    payment_id                        TEXT          NOT NULL,
    transaction_type                  TEXT          NOT NULL CHECK (transaction_type IN ('AUTHORIZATION', 'CAPTURE', 'DECLINE')),
    original_processor_transaction_id TEXT,
    processor_status                  TEXT          NOT NULL CHECK (processor_status IN ('AUTHORIZED', 'CAPTURED', 'DECLINED')),
    amount                            NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    currency                          CHAR(3)       NOT NULL,
    event_time                        TIMESTAMPTZ   NOT NULL,
    correlation_id                    TEXT          NOT NULL,
    schema_version                    TEXT          NOT NULL,
    ingested_at                       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT processor_transactions_type_status_ck CHECK (
        (transaction_type = 'AUTHORIZATION' AND processor_status = 'AUTHORIZED') OR
        (transaction_type = 'CAPTURE' AND processor_status = 'CAPTURED') OR
        (transaction_type = 'DECLINE' AND processor_status = 'DECLINED'))
);
CREATE INDEX processor_transactions_payment_idx ON processor_transactions (payment_id);
CREATE INDEX processor_transactions_time_idx ON processor_transactions (event_time);

CREATE TABLE payment_refunds (
    refund_id                         TEXT PRIMARY KEY,
    source_event_id                   TEXT          NOT NULL UNIQUE,
    raw_event_id                      BIGINT        NOT NULL REFERENCES raw_payment_events (raw_event_id),
    payment_id                        TEXT          NOT NULL,
    original_processor_transaction_id TEXT          NOT NULL,
    processor_status                  TEXT          NOT NULL CHECK (processor_status = 'REFUNDED'),
    amount                            NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    currency                          CHAR(3)       NOT NULL,
    event_time                        TIMESTAMPTZ   NOT NULL,
    correlation_id                    TEXT          NOT NULL,
    schema_version                    TEXT          NOT NULL,
    ingested_at                       TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX payment_refunds_payment_idx ON payment_refunds (payment_id);
CREATE INDEX payment_refunds_original_idx ON payment_refunds (original_processor_transaction_id);
CREATE INDEX payment_refunds_time_idx ON payment_refunds (event_time);

CREATE TABLE payment_reversals (
    reversal_id                       TEXT PRIMARY KEY,
    source_event_id                   TEXT          NOT NULL UNIQUE,
    raw_event_id                      BIGINT        NOT NULL REFERENCES raw_payment_events (raw_event_id),
    payment_id                        TEXT          NOT NULL,
    original_processor_transaction_id TEXT          NOT NULL,
    processor_status                  TEXT          NOT NULL CHECK (processor_status = 'REVERSED'),
    amount                            NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    currency                          CHAR(3)       NOT NULL,
    event_time                        TIMESTAMPTZ   NOT NULL,
    correlation_id                    TEXT          NOT NULL,
    schema_version                    TEXT          NOT NULL,
    ingested_at                       TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX payment_reversals_payment_idx ON payment_reversals (payment_id);
CREATE INDEX payment_reversals_original_idx ON payment_reversals (original_processor_transaction_id);
CREATE INDEX payment_reversals_time_idx ON payment_reversals (event_time);

CREATE TABLE billing_ledger_entries (
    ledger_entry_id          TEXT PRIMARY KEY,
    source_event_id          TEXT          NOT NULL UNIQUE,
    raw_event_id             BIGINT        NOT NULL REFERENCES raw_payment_events (raw_event_id),
    payment_id               TEXT,
    account_ref              TEXT          NOT NULL,
    entry_type               TEXT          NOT NULL CHECK (entry_type IN ('CHARGE', 'PAYMENT_CREDIT',
        'POSTING_CONFIRMATION', 'REFUND_DEBIT', 'REVERSAL', 'ADJUSTMENT')),
    direction                TEXT          NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    posting_status           TEXT          NOT NULL CHECK (posting_status IN ('PENDING', 'POSTED', 'REJECTED')),
    amount                   NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    currency                 CHAR(3)       NOT NULL,
    original_ledger_entry_id TEXT,
    processor_reference      TEXT,
    adjustment_reason        TEXT,
    event_time               TIMESTAMPTZ   NOT NULL,
    correlation_id           TEXT          NOT NULL,
    schema_version           TEXT          NOT NULL,
    ingested_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT billing_ledger_payment_required_ck CHECK (entry_type = 'CHARGE' OR payment_id IS NOT NULL),
    CONSTRAINT billing_ledger_direction_ck CHECK (
        (entry_type = 'CHARGE' AND direction = 'DEBIT') OR
        (entry_type = 'PAYMENT_CREDIT' AND direction = 'CREDIT') OR
        (entry_type = 'REFUND_DEBIT' AND direction = 'DEBIT') OR
        entry_type IN ('POSTING_CONFIRMATION', 'REVERSAL', 'ADJUSTMENT'))
);
CREATE INDEX billing_ledger_entries_payment_idx ON billing_ledger_entries (payment_id) WHERE payment_id IS NOT NULL;
CREATE INDEX billing_ledger_entries_time_idx ON billing_ledger_entries (event_time);
CREATE INDEX billing_ledger_entries_account_idx ON billing_ledger_entries (account_ref);
