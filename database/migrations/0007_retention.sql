-- 0007: Retention support.
--  * reconciliation_history becomes RANGE-partitioned by month on evaluated_at, so expired audit history is
--    removed by dropping whole partitions (no row-level DELETE, which the append-only rule forbids anyway).
--  * raw_payment_events gets archival columns: old payloads are exported to the archive and blanked, while the
--    row (Kafka position, hashes, ids) stays for idempotency and Silver foreign keys. Bronze is not partitioned:
--    six Silver tables reference raw_event_id, and a partitioned table's unique keys must include the
--    partition column.

ALTER TABLE reconciliation_history RENAME TO reconciliation_history_unpartitioned;
ALTER INDEX reconciliation_history_pkey RENAME TO reconciliation_history_unpartitioned_pkey;
ALTER INDEX reconciliation_history_case_idx RENAME TO reconciliation_history_unpartitioned_case_idx;
ALTER INDEX reconciliation_history_payment_idx RENAME TO reconciliation_history_unpartitioned_payment_idx;
ALTER INDEX reconciliation_history_time_idx RENAME TO reconciliation_history_unpartitioned_time_idx;

CREATE SEQUENCE reconciliation_history_id_seq;

CREATE TABLE reconciliation_history (
    history_id             BIGINT      NOT NULL DEFAULT nextval('reconciliation_history_id_seq'),
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
    evaluated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (history_id, evaluated_at)
) PARTITION BY RANGE (evaluated_at);
ALTER SEQUENCE reconciliation_history_id_seq OWNED BY reconciliation_history.history_id;
COMMENT ON TABLE reconciliation_history IS
    'Append-only audit trail, partitioned by month (reconciliation_history_YYYYMM). Retention drops whole partitions.';

-- Safety net only: ensure_history_partitions keeps monthly partitions created ahead of time.
CREATE TABLE reconciliation_history_default PARTITION OF reconciliation_history DEFAULT;

CREATE FUNCTION create_history_partition(month DATE) RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    first_day DATE := date_trunc('month', month)::date;
    name TEXT := 'reconciliation_history_' || to_char(first_day, 'YYYYMM');
BEGIN
    IF to_regclass(name) IS NOT NULL THEN
        RETURN NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM reconciliation_history_default
               WHERE evaluated_at >= first_day AND evaluated_at < first_day + INTERVAL '1 month') THEN
        RAISE WARNING 'rows for % are in reconciliation_history_default; partition not created', name;
        RETURN NULL;
    END IF;
    EXECUTE format('CREATE TABLE %I PARTITION OF reconciliation_history FOR VALUES FROM (%L) TO (%L)',
                   name, first_day, (first_day + INTERVAL '1 month')::date);
    RETURN name;
END;
$$;

CREATE FUNCTION ensure_history_partitions(from_time TIMESTAMPTZ, months_ahead INTEGER) RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
    month DATE := date_trunc('month', least(from_time, now()))::date;
    last_month DATE := (date_trunc('month', now()) + make_interval(months => months_ahead))::date;
    created INTEGER := 0;
BEGIN
    WHILE month <= last_month LOOP
        IF create_history_partition(month) IS NOT NULL THEN
            created := created + 1;
        END IF;
        month := (month + INTERVAL '1 month')::date;
    END LOOP;
    RETURN created;
END;
$$;

SELECT ensure_history_partitions(coalesce((SELECT min(evaluated_at) FROM reconciliation_history_unpartitioned), now()), 3);
INSERT INTO reconciliation_history SELECT * FROM reconciliation_history_unpartitioned;
SELECT setval('reconciliation_history_id_seq', coalesce((SELECT max(history_id) FROM reconciliation_history), 0) + 1, false);
DROP TABLE reconciliation_history_unpartitioned;

CREATE INDEX reconciliation_history_case_idx ON reconciliation_history (reconciliation_case_id, history_id);
CREATE INDEX reconciliation_history_payment_idx ON reconciliation_history (payment_id);
CREATE INDEX reconciliation_history_time_idx ON reconciliation_history (evaluated_at);

-- Statement trigger: any UPDATE/DELETE/TRUNCATE through the parent. Row trigger: also DML addressed directly to a
-- partition. Dropping an expired partition (DDL, table owner only) is the one sanctioned way to remove history.
CREATE TRIGGER reconciliation_history_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON reconciliation_history
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER reconciliation_history_append_only_rows
    BEFORE UPDATE OR DELETE ON reconciliation_history
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

GRANT SELECT, INSERT ON reconciliation_history TO {{app_role}};
GRANT USAGE ON SEQUENCE reconciliation_history_id_seq TO {{app_role}};
GRANT SELECT ON reconciliation_history TO {{reporting_role}};

ALTER TABLE raw_payment_events
    ADD COLUMN payload_archived_at TIMESTAMPTZ,
    ADD COLUMN archive_uri TEXT;
COMMENT ON COLUMN raw_payment_events.payload_archived_at IS
    'Set when the payload was moved to the archive (archive_uri); payload_bytes is then empty and payload_json NULL. payload_hash still identifies the original bytes.';
CREATE INDEX raw_payment_events_unarchived_idx ON raw_payment_events (ingestion_time) WHERE payload_archived_at IS NULL;
