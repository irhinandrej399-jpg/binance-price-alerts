ALTER TABLE app.delivery_jobs
    ADD COLUMN lease_fence bigint NOT NULL DEFAULT 0 CHECK (lease_fence >= 0),
    ADD COLUMN lease_id text,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT '-infinity',
    ADD CONSTRAINT job_lease_shape CHECK (
        (lease_id IS NULL AND lease_expires_at IS NULL) OR
        (lease_id IS NOT NULL AND btrim(lease_id) <> '' AND lease_expires_at IS NOT NULL AND lease_fence > 0));

-- Local work claims, not Telegram receipts or API send permissions.
-- Retain expired identities for later attempt/outcome correlation.
CREATE TABLE app.delivery_claims (
    claim_id text PRIMARY KEY CHECK (btrim(claim_id) <> ''),
    trigger_id text NOT NULL REFERENCES app.delivery_jobs(trigger_id),
    fence bigint NOT NULL CHECK (fence > 0),
    claimed_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > claimed_at),
    status text NOT NULL CHECK (status IN ('claimed','expired','released')),
    UNIQUE (trigger_id,fence)
);
GRANT SELECT, INSERT, UPDATE ON app.delivery_claims TO notifications_runtime;
