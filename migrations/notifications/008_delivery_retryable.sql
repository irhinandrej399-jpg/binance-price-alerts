-- Immutable proven temporary failures, including the FIRST committed policy plan.
-- Count evidence joined through delivery_attempts.trigger_id for the durable budget.
CREATE TABLE app.delivery_retryables (
    attempt_id text PRIMARY KEY REFERENCES app.delivery_attempts(attempt_id),
    kind text NOT NULL CHECK (kind IN ('rate_limited','server')),
    retry_after_seconds bigint NOT NULL CHECK (retry_after_seconds >= 0),
    binding_version bigint NOT NULL CHECK (binding_version >= 0),
    failures bigint NOT NULL CHECK (failures BETWEEN 1 AND 4294967295),
    exhausted boolean NOT NULL,
    next_attempt_at timestamptz,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((exhausted AND next_attempt_at IS NULL) OR
           (NOT exhausted AND next_attempt_at IS NOT NULL AND isfinite(next_attempt_at)))
);
CREATE TABLE app.delivery_retryable_reports (
    attempt_id text PRIMARY KEY REFERENCES app.delivery_retryables(attempt_id),
    pending boolean NOT NULL DEFAULT true
);
-- init.sh grants UPDATE/DELETE by default; explicitly remove those rights.
REVOKE ALL ON app.delivery_retryables, app.delivery_retryable_reports FROM notifications_runtime;
GRANT SELECT, INSERT ON app.delivery_retryables TO notifications_runtime;
GRANT SELECT, INSERT, UPDATE ON app.delivery_retryable_reports TO notifications_runtime;
