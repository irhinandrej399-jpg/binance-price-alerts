-- API-owned canonical subscription after-image. Quota, workflow/attempt tables,
-- journal and outbox operations are added by their own tickets.
CREATE TABLE app.subscriptions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id uuid NOT NULL REFERENCES app.users(id),
    pair text NOT NULL CHECK (pair ~ '^[A-Z0-9]+$'),
    -- No numeric(p,s) typmod: it would silently round excess input precision.
    threshold numeric NOT NULL CHECK (threshold >= 0 AND threshold < 1000000000000000000 AND scale(threshold) <= 18),
    direction text NOT NULL CHECK (direction IN ('up','down')),
    creation_price numeric NOT NULL CHECK (creation_price > 0 AND creation_price < 1000000000000000000 AND scale(creation_price) <= 18),
    creation_trade_time timestamptz NOT NULL CHECK (creation_trade_time > '0001-01-01 00:00:00+00' AND creation_trade_time < '10000-01-01 00:00:00+00'),
    percent text CHECK (percent ~ '^[+-]?[0-9]+([.][0-9]{1,18})?$'
        AND percent::numeric > -100 AND percent::numeric <> 0 AND percent::numeric < 1000000000000000000),
    stale boolean NOT NULL DEFAULT false,
    stream_unconfirmed boolean NOT NULL DEFAULT false,
    gap_detected boolean NOT NULL DEFAULT false,
    state text NOT NULL CHECK (state IN ('active','pending_delivery','sending','delivery_blocked','delivered','cancelled')),
    version bigint NOT NULL CHECK (version > 0),
    -- Activation is a policy, not a historical market revision or journal C.
    activation text NOT NULL CHECK (activation = 'latest_price'),
    workflow_id text UNIQUE CHECK (btrim(workflow_id) <> ''),
    workflow_version bigint,
    block_reason text,
    blocked_binding_version bigint,
    CONSTRAINT subscriptions_workflow_valid CHECK (
        (workflow_id IS NULL AND workflow_version IS NULL)
        OR (workflow_id IS NOT NULL AND workflow_version IS NOT NULL AND workflow_version > 0)),
    CONSTRAINT subscriptions_state_workflow CHECK (
        state NOT IN ('pending_delivery','sending','delivered') OR workflow_id IS NOT NULL),
    CONSTRAINT subscriptions_block_valid CHECK (
        (state = 'delivery_blocked' AND block_reason IS NOT NULL
            AND block_reason IN ('channel_unavailable','attempts_exhausted','permanent_failure')
            AND blocked_binding_version IS NOT NULL AND blocked_binding_version >= 0)
        OR (state <> 'delivery_blocked' AND block_reason IS NULL AND blocked_binding_version IS NULL))
);
CREATE INDEX subscriptions_owner_id ON app.subscriptions(owner_id,id);
GRANT SELECT, INSERT, UPDATE ON app.subscriptions TO api_runtime;
