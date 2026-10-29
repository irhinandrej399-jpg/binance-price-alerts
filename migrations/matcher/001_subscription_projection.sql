-- C is a uint64 journal boundary; Kafka offset is an independent int64 scale.
CREATE TABLE app.matcher_checkpoints (
    projection_id text PRIMARY KEY CHECK (btrim(projection_id) <> ''),
    topic text NOT NULL CHECK (btrim(topic) <> ''),
    partition integer NOT NULL CHECK (partition >= 0),
    journal_c numeric(20,0) NOT NULL DEFAULT 0
        CHECK (journal_c BETWEEN 0 AND 18446744073709551615),
    next_offset bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0)
);
CREATE TABLE app.matcher_subscriptions (
    projection_id text NOT NULL REFERENCES app.matcher_checkpoints(projection_id),
    subscription_id text NOT NULL,
    version numeric(20,0) NOT NULL CHECK (version BETWEEN 1 AND 18446744073709551615),
    -- Full after-image including terminal tombstones prevents resurrection.
    body bytea NOT NULL CHECK (octet_length(body) > 0),
    PRIMARY KEY (projection_id, subscription_id)
);
GRANT SELECT, INSERT, UPDATE ON app.matcher_checkpoints TO matcher_runtime;
GRANT SELECT, INSERT, UPDATE ON app.matcher_subscriptions TO matcher_runtime;
