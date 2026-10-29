-- Staging generations are deliberately independent of live checkpoints.
-- A complete snapshot still requires replay and current-price connection.
CREATE TABLE app.matcher_snapshot_generations (
    snapshot_id text PRIMARY KEY,
    boundary_c numeric(20,0) NOT NULL CHECK (boundary_c BETWEEN 0 AND 18446744073709551615),
    expires_at timestamptz NOT NULL,
    -- Preserve nanoseconds: PostgreSQL timestamptz alone has microsecond precision.
    expires_text text NOT NULL,
    complete boolean NOT NULL DEFAULT false,
    row_count bigint NOT NULL DEFAULT 0 CHECK (row_count >= 0)
);
CREATE TABLE app.matcher_snapshot_rows (
    snapshot_id text NOT NULL REFERENCES app.matcher_snapshot_generations(snapshot_id),
    subscription_id text NOT NULL,
    -- Full SnapshotSubscription protobuf, including optional audit/workflow fields.
    body bytea NOT NULL CHECK (octet_length(body) > 0),
    PRIMARY KEY (snapshot_id, subscription_id)
);
GRANT SELECT, INSERT, UPDATE ON app.matcher_snapshot_generations TO matcher_runtime;
GRANT SELECT, INSERT ON app.matcher_snapshot_rows TO matcher_runtime;
