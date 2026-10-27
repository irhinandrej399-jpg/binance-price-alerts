-- Per-pair row serializes generation changes and acceptance, including first trade.
CREATE TABLE app.market_streams (
    pair text PRIMARY KEY CHECK (pair ~ '^[A-Z0-9]+$'),
    generation bigint NOT NULL CHECK (generation > 0),
    sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    state text NOT NULL CHECK (state IN ('unconfirmed','live')),
    gap_detected boolean NOT NULL DEFAULT false,
    latest jsonb
);
CREATE TABLE app.price_outbox (
    position bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id text NOT NULL UNIQUE CHECK (btrim(event_id) <> ''),
    pair text NOT NULL REFERENCES app.market_streams(pair),
    data bytea NOT NULL,
    published boolean NOT NULL DEFAULT false,
    claim_token bigint NOT NULL DEFAULT 0 CHECK (claim_token >= 0),
    lease_until timestamptz
);
CREATE INDEX price_outbox_pending ON app.price_outbox(pair,position) WHERE NOT published;
-- No runtime DELETE: publishing alone does not authorize retention/compaction.
GRANT SELECT, INSERT, UPDATE ON app.market_streams TO market_data_runtime;
REVOKE ALL ON app.price_outbox FROM market_data_runtime;
GRANT SELECT, INSERT ON app.price_outbox TO market_data_runtime;
GRANT UPDATE(published,claim_token,lease_until) ON app.price_outbox TO market_data_runtime;
GRANT USAGE ON SEQUENCE app.price_outbox_position_seq TO market_data_runtime;
