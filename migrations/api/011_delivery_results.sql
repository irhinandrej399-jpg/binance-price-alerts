-- First confirmed success is canonical; additional late successes remain audit.
ALTER TABLE app.subscriptions
    ADD COLUMN delivered_attempt_id text,
    ADD COLUMN delivered_message_id bigint,
    ADD CONSTRAINT subscriptions_receipt_valid CHECK (
        (delivered_attempt_id IS NULL AND delivered_message_id IS NULL)
        OR (state='delivered' AND delivered_attempt_id IS NOT NULL
            AND btrim(delivered_attempt_id) <> '' AND delivered_message_id IS NOT NULL
            AND delivered_message_id > 0));

-- Append-only observations of known permissions. Uncertainty/failure never
-- overwrites confirmed success. Repeated observations keep the original bytes.
-- Logical references do not cascade-delete retained audit/work.
CREATE TABLE app.delivery_results (
    subscription_id uuid NOT NULL,
    workflow_id text NOT NULL CHECK (btrim(workflow_id) <> ''),
    attempt_id text NOT NULL CHECK (btrim(attempt_id) <> ''),
    outcome text NOT NULL CHECK (outcome IN ('succeeded','retryable','uncertain','blocked')),
    message_id bigint,
    block_reason text,
    binding_version bigint NOT NULL CHECK (binding_version > 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (attempt_id,outcome),
    CHECK ((outcome='succeeded' AND message_id IS NOT NULL AND message_id>0 AND block_reason IS NULL)
        OR (outcome IN ('retryable','uncertain') AND message_id IS NULL AND block_reason IS NULL)
        OR (outcome='blocked' AND message_id IS NULL AND block_reason IS NOT NULL
            AND block_reason IN ('channel_unavailable','attempts_exhausted','permanent_failure')))
);
CREATE INDEX delivery_results_subscription ON app.delivery_results(subscription_id);
REVOKE ALL ON app.delivery_results FROM api_runtime;
GRANT SELECT, INSERT ON app.delivery_results TO api_runtime;
