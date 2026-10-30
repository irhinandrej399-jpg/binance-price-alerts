CREATE TABLE app.delivery_jobs (
    trigger_id text PRIMARY KEY CHECK (btrim(trigger_id) <> ''),
    subscription_id text NOT NULL CHECK (btrim(subscription_id) <> ''),
    user_id text NOT NULL CHECK (btrim(user_id) <> ''),
    event_id text NOT NULL CHECK (btrim(event_id) <> ''),
    -- Original validated request, including exact uint64 versions and prices.
    body bytea NOT NULL CHECK (octet_length(body) > 0),
    state text NOT NULL DEFAULT 'pending' CHECK (btrim(state) <> ''),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE app.delivery_checkpoints (
    topic text NOT NULL CHECK (btrim(topic) <> ''),
    partition integer NOT NULL CHECK (partition >= 0),
    next_offset bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    PRIMARY KEY (topic, partition)
);
GRANT SELECT, INSERT, UPDATE ON app.delivery_jobs, app.delivery_checkpoints TO notifications_runtime;
