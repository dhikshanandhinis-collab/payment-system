-- 0006: Processor settlement (funding) records and the per-case settlement dimension.
-- Settlement is reconciled alongside, not instead of, the operational status: a MATCHED payment can still
-- be UNSETTLED or have a SETTLEMENT_MISMATCH (cash reconciliation is owned by Finance).

CREATE TABLE settlement_records (
    settlement_id            TEXT PRIMARY KEY,
    source_event_id          TEXT          NOT NULL UNIQUE,
    raw_event_id             BIGINT        NOT NULL REFERENCES raw_payment_events (raw_event_id),
    settlement_batch_id      TEXT          NOT NULL,
    settlement_date          DATE          NOT NULL,
    processor_transaction_id TEXT          NOT NULL,
    payment_id               TEXT          NOT NULL,
    transaction_type         TEXT          NOT NULL CHECK (transaction_type IN
        ('CAPTURE', 'REFUND', 'REVERSAL', 'CHARGEBACK', 'CHARGEBACK_REVERSAL')),
    gross_amount             NUMERIC(14,2) NOT NULL CHECK (gross_amount > 0),
    fee_amount               NUMERIC(14,2) NOT NULL CHECK (fee_amount >= 0),
    net_amount               NUMERIC(14,2) NOT NULL,
    currency                 CHAR(3)       NOT NULL,
    event_time               TIMESTAMPTZ   NOT NULL,
    correlation_id           TEXT          NOT NULL,
    schema_version           TEXT          NOT NULL,
    ingested_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT settlement_records_net_ck CHECK (
        net_amount = CASE WHEN transaction_type IN ('CAPTURE', 'CHARGEBACK_REVERSAL') THEN gross_amount ELSE -gross_amount END
                     - fee_amount)
);
COMMENT ON TABLE settlement_records IS
    'Silver: lines of processor settlement files. net_amount = signed gross - fee (merchant point of view).';
CREATE INDEX settlement_records_payment_idx ON settlement_records (payment_id);
CREATE INDEX settlement_records_transaction_idx ON settlement_records (processor_transaction_id);
CREATE INDEX settlement_records_date_idx ON settlement_records (settlement_date);

ALTER TABLE reconciliation_cases
    ADD COLUMN settlement_status TEXT NOT NULL DEFAULT 'NOT_APPLICABLE'
        CONSTRAINT reconciliation_cases_settlement_status_ck CHECK (settlement_status IN
            ('NOT_APPLICABLE', 'PENDING', 'SETTLED', 'UNSETTLED', 'SETTLEMENT_MISMATCH')),
    ADD COLUMN settlement_reason TEXT,
    ADD COLUMN settled_net_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    ADD COLUMN settlement_fee_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    ADD COLUMN settlement_difference_amount NUMERIC(14,2) NOT NULL DEFAULT 0;
CREATE INDEX reconciliation_cases_settlement_idx ON reconciliation_cases (settlement_status)
    WHERE settlement_status IN ('UNSETTLED', 'SETTLEMENT_MISMATCH');

GRANT SELECT, INSERT ON settlement_records TO {{app_role}};
GRANT SELECT ON settlement_records TO {{reporting_role}};
