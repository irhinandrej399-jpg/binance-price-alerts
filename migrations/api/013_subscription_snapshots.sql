-- Publication of S/C and its retention pin is one builder transaction.
CREATE TABLE app.subscription_snapshots (
    id uuid PRIMARY KEY,
    owner text NOT NULL CHECK (length(owner) BETWEEN 1 AND 200),
    boundary bigint NOT NULL CHECK (boundary >= 0),
    expires_at timestamptz NOT NULL
);
CREATE TABLE app.subscription_snapshot_rows (
    snapshot_id uuid NOT NULL REFERENCES app.subscription_snapshots(id),
    LIKE app.subscriptions,
    PRIMARY KEY (snapshot_id,id)
);
CREATE TABLE app.subscription_replay_pins (
    id uuid PRIMARY KEY REFERENCES app.subscription_snapshots(id),
    owner text NOT NULL CHECK (length(owner) BETWEEN 1 AND 200),
    progress bigint NOT NULL CHECK (progress >= 0),
    issued bigint NOT NULL CHECK (issued >= progress),
    phase text NOT NULL CHECK (phase IN ('snapshot','replay','abandoned')),
    expires_at timestamptz NOT NULL
);
CREATE INDEX subscription_replay_pins_expiry ON app.subscription_replay_pins(expires_at);
REVOKE ALL ON app.subscription_snapshots,app.subscription_snapshot_rows,app.subscription_replay_pins FROM api_runtime;
GRANT SELECT,INSERT ON app.subscription_snapshots,app.subscription_snapshot_rows,app.subscription_replay_pins TO api_runtime;
GRANT UPDATE(progress,issued,phase,expires_at) ON app.subscription_replay_pins TO api_runtime;
