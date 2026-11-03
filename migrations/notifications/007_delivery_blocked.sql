ALTER TABLE app.delivery_jobs
    ADD COLUMN blocked_reason text CHECK (blocked_reason IN ('channel_unavailable','permanent_failure','attempts_exhausted')),
    ADD COLUMN blocked_binding_version bigint CHECK (blocked_binding_version >= 0),
    ADD CONSTRAINT blocked_metadata_shape CHECK ((blocked_reason IS NULL) = (blocked_binding_version IS NULL));
CREATE TABLE app.delivery_blocks (
    attempt_id text PRIMARY KEY REFERENCES app.delivery_attempts(attempt_id),
    reason text NOT NULL CHECK (reason IN ('channel_unavailable','permanent_failure','attempts_exhausted')),
    binding_version bigint NOT NULL CHECK (binding_version >= 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE app.delivery_blocked_reports (
    attempt_id text PRIMARY KEY REFERENCES app.delivery_blocks(attempt_id),
    pending boolean NOT NULL DEFAULT true
);
-- API has already blocked a denied admission; no fabricated send attempt/report.
CREATE TABLE app.delivery_block_parks (
    claim_id text PRIMARY KEY REFERENCES app.delivery_claims(claim_id),
    reason text NOT NULL CHECK (reason IN ('channel_unavailable','permanent_failure','attempts_exhausted')),
    binding_version bigint NOT NULL CHECK (binding_version >= 0)
);
GRANT SELECT, INSERT ON app.delivery_blocks, app.delivery_block_parks TO notifications_runtime;
GRANT SELECT, INSERT, UPDATE ON app.delivery_blocked_reports TO notifications_runtime;
