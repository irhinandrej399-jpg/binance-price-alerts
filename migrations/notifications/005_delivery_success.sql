-- Local registration correlates an API permission with one durable work claim.
-- It does not grant permission to send HTTP.
ALTER TABLE app.delivery_claims
    DROP CONSTRAINT delivery_claims_status_check,
    ADD CONSTRAINT delivery_claims_status_check CHECK (status IN ('claimed','expired','released','completed')),
    ADD CONSTRAINT delivery_claim_identity UNIQUE (claim_id,trigger_id,fence);
CREATE TABLE app.delivery_attempts (
    attempt_id text PRIMARY KEY CHECK (btrim(attempt_id) <> ''),
    claim_id text NOT NULL UNIQUE,
    trigger_id text NOT NULL,
    fence bigint NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (claim_id,trigger_id,fence) REFERENCES app.delivery_claims(claim_id,trigger_id,fence)
);
CREATE INDEX delivery_attempts_workflow ON app.delivery_attempts(trigger_id);
CREATE TABLE app.delivery_successes (
    attempt_id text PRIMARY KEY REFERENCES app.delivery_attempts(attempt_id),
    message_id bigint NOT NULL CHECK (message_id > 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Created in the same transaction as evidence; API reconciliation is NTF-014.
CREATE TABLE app.delivery_success_reports (
    attempt_id text PRIMARY KEY REFERENCES app.delivery_successes(attempt_id),
    pending boolean NOT NULL DEFAULT true
);
GRANT SELECT, INSERT ON app.delivery_attempts, app.delivery_successes TO notifications_runtime;
GRANT SELECT, INSERT, UPDATE ON app.delivery_success_reports TO notifications_runtime;
