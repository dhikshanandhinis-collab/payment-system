-- 0001: Bronze raw event storage and idempotency keys.
-- Executed by the migrator role inside the "recon" schema (search_path set by the migration runner).

CREATE TABLE raw_payment_events (
    raw_event_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_system       TEXT        NOT NULL,
    source_topic        TEXT        NOT NULL,
    source_partition    INTEGER     NOT NULL CHECK (source_partition >= 0),
    source_offset       BIGINT      NOT NULL CHECK (source_offset >= 0),
    kafka_key           TEXT,
    kafka_timestamp     TIMESTAMPTZ,
    source_event_id     TEXT,
    event_type          TEXT,
    payment_id          TEXT,
    event_time          TIMESTAMPTZ,
    ingestion_time      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    schema_version      TEXT,
    payload_bytes       BYTEA       NOT NULL,
    payload_json        JSONB,
    payload_hash        CHAR(64)    NOT NULL,
    processing_status   TEXT        NOT NULL,
    error_code          TEXT,
    validation_error    TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT raw_payment_events_source_position_uk UNIQUE (source_topic, source_partition, source_offset),
    CONSTRAINT raw_payment_events_status_ck CHECK (
        processing_status IN ('PROCESSED', 'DUPLICATE_DELIVERY', 'DUPLICATE_BUSINESS_EVENT', 'QUARANTINED')
    ),
    CONSTRAINT raw_payment_events_error_ck CHECK (
        (processing_status = 'QUARANTINED') = (error_code IS NOT NULL)
    )
);

COMMENT ON TABLE raw_payment_events IS
    'Bronze: every Kafka record exactly as received (bytes preserved). Identity = topic/partition/offset.';
COMMENT ON COLUMN raw_payment_events.payload_bytes IS 'Original message bytes, never rewritten.';
COMMENT ON COLUMN raw_payment_events.payload_json IS 'Parsed copy for querying; NULL when the payload is not valid JSON.';

CREATE INDEX raw_payment_events_source_event_idx ON raw_payment_events (source_system, source_event_id);
CREATE INDEX raw_payment_events_payment_idx ON raw_payment_events (payment_id) WHERE payment_id IS NOT NULL;
CREATE INDEX raw_payment_events_ingestion_time_idx ON raw_payment_events (ingestion_time);
CREATE INDEX raw_payment_events_event_time_idx ON raw_payment_events (event_time);
CREATE INDEX raw_payment_events_status_idx ON raw_payment_events (processing_status)
    WHERE processing_status <> 'PROCESSED';

CREATE TABLE deduplication_keys (
    source_system       TEXT        NOT NULL,
    key_type            TEXT        NOT NULL CHECK (key_type IN ('EVENT_ID', 'BUSINESS_KEY')),
    key_value           TEXT        NOT NULL,
    content_hash        CHAR(64)    NOT NULL,
    first_raw_event_id  BIGINT      NOT NULL REFERENCES raw_payment_events (raw_event_id),
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_system, key_type, key_value)
);

COMMENT ON TABLE deduplication_keys IS
    'Idempotency registry. EVENT_ID detects re-delivery; BUSINESS_KEY detects the same processor/ledger transaction re-sent under a new event_id.';

CREATE TABLE quarantined_events (
    quarantine_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_event_id        BIGINT REFERENCES raw_payment_events (raw_event_id),
    source_topic        TEXT        NOT NULL,
    source_partition    INTEGER     NOT NULL,
    source_offset       BIGINT      NOT NULL,
    source_system       TEXT        NOT NULL,
    source_event_id     TEXT,
    payment_id          TEXT,
    error_category      TEXT        NOT NULL CHECK (error_category IN (
        'VALIDATION', 'SERIALIZATION', 'UNSUPPORTED_SCHEMA', 'DUPLICATE_CONFLICT', 'DATABASE', 'PROCESSING')),
    error_code          TEXT        NOT NULL,
    error_detail        TEXT        NOT NULL,
    payload_bytes       BYTEA       NOT NULL,
    quarantined_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    status              TEXT        NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'REPLAYED', 'DISCARDED')),
    resolved_at         TIMESTAMPTZ,
    resolved_by         TEXT,
    resolution_note     TEXT,
    CONSTRAINT quarantined_events_source_position_uk UNIQUE (source_topic, source_partition, source_offset),
    CONSTRAINT quarantined_events_resolution_ck CHECK (
        (status = 'OPEN') = (resolved_at IS NULL)
    )
);

CREATE INDEX quarantined_events_open_idx ON quarantined_events (quarantined_at) WHERE status = 'OPEN';
CREATE INDEX quarantined_events_reason_idx ON quarantined_events (error_category, error_code);
