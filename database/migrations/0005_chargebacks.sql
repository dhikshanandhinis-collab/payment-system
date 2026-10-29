-- 0005: Chargebacks (processor disputes) and their billing-ledger counterparts; CHARGEBACK_PENDING status.

CREATE TABLE payment_chargebacks (
    chargeback_id                     TEXT PRIMARY KEY,
    source_event_id                   TEXT          NOT NULL UNIQUE,
    raw_event_id                      BIGINT        NOT NULL REFERENCES raw_payment_events (raw_event_id),
    payment_id                        TEXT          NOT NULL,
    chargeback_type                   TEXT          NOT NULL CHECK (chargeback_type IN ('CHARGEBACK', 'CHARGEBACK_REVERSAL')),
    original_processor_transaction_id TEXT          NOT NULL,
    processor_status                  TEXT          NOT NULL,
    amount                            NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    currency                          CHAR(3)       NOT NULL,
    event_time                        TIMESTAMPTZ   NOT NULL,
    correlation_id                    TEXT          NOT NULL,
    schema_version                    TEXT          NOT NULL,
    ingested_at                       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT payment_chargebacks_type_status_ck CHECK (
        (chargeback_type = 'CHARGEBACK' AND processor_status = 'CHARGED_BACK') OR
        (chargeback_type = 'CHARGEBACK_REVERSAL' AND processor_status = 'CHARGEBACK_REVERSED'))
);
COMMENT ON TABLE payment_chargebacks IS
    'Silver: card-network disputes. CHARGEBACK references the CAPTURE; CHARGEBACK_REVERSAL (dispute won) references the CHARGEBACK.';
CREATE INDEX payment_chargebacks_payment_idx ON payment_chargebacks (payment_id);
CREATE INDEX payment_chargebacks_original_idx ON payment_chargebacks (original_processor_transaction_id);
CREATE INDEX payment_chargebacks_time_idx ON payment_chargebacks (event_time);

ALTER TABLE billing_ledger_entries DROP CONSTRAINT billing_ledger_entries_entry_type_check;
ALTER TABLE billing_ledger_entries ADD CONSTRAINT billing_ledger_entries_entry_type_check CHECK (entry_type IN (
    'CHARGE', 'PAYMENT_CREDIT', 'POSTING_CONFIRMATION', 'REFUND_DEBIT', 'REVERSAL', 'ADJUSTMENT',
    'CHARGEBACK_DEBIT', 'CHARGEBACK_CREDIT'));
ALTER TABLE billing_ledger_entries DROP CONSTRAINT billing_ledger_direction_ck;
ALTER TABLE billing_ledger_entries ADD CONSTRAINT billing_ledger_direction_ck CHECK (
    (entry_type = 'CHARGE' AND direction = 'DEBIT') OR
    (entry_type = 'PAYMENT_CREDIT' AND direction = 'CREDIT') OR
    (entry_type = 'REFUND_DEBIT' AND direction = 'DEBIT') OR
    (entry_type = 'CHARGEBACK_DEBIT' AND direction = 'DEBIT') OR
    (entry_type = 'CHARGEBACK_CREDIT' AND direction = 'CREDIT') OR
    entry_type IN ('POSTING_CONFIRMATION', 'REVERSAL', 'ADJUSTMENT'));

ALTER TABLE reconciliation_cases DROP CONSTRAINT reconciliation_cases_status_ck;
ALTER TABLE reconciliation_cases ADD CONSTRAINT reconciliation_cases_status_ck CHECK (reconciliation_status IN (
    'PENDING_MATCH', 'REFUND_PENDING', 'REVERSAL_PENDING', 'CHARGEBACK_PENDING', 'MATCHED', 'NO_FINANCIAL_EFFECT',
    'AMOUNT_MISMATCH', 'STATUS_MISMATCH', 'MISSING_PROCESSOR_RECORD', 'MISSING_BILLING_POSTING',
    'MISSING_APPLICATION_RECORD', 'DUPLICATE_FINANCIAL_EFFECT', 'DATA_QUALITY_ERROR', 'UNDER_REVIEW'));

GRANT SELECT, INSERT ON payment_chargebacks TO {{app_role}};
GRANT SELECT ON payment_chargebacks TO {{reporting_role}};
