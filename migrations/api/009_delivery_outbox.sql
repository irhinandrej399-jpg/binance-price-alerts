-- One immutable delivery work request for the lifetime of each subscription.
-- Canonical workflow identity/version remain in app.subscriptions. The bytea
-- envelope keeps stable event_id, trigger_id, exact quote and diagnostic times.
-- Publication/retention are separate operations (SUB-009 and later tickets).
CREATE TABLE app.delivery_outbox (
    -- Logical identity, like the journal: deleting canonical data must never
    -- cascade-delete retained work. The command inserts both in one tx.
    subscription_id uuid PRIMARY KEY,
    trigger_id text NOT NULL UNIQUE CHECK (btrim(trigger_id) <> ''),
    event_id text NOT NULL UNIQUE CHECK (btrim(event_id) <> ''),
    body bytea NOT NULL CHECK (octet_length(body) > 0)
);
REVOKE ALL ON app.delivery_outbox FROM api_runtime;
GRANT SELECT, INSERT ON app.delivery_outbox TO api_runtime;
