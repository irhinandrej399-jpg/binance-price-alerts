-- Evidence is immutable and distinct from later confirmed success.
CREATE TABLE app.delivery_uncertainties (
    attempt_id text PRIMARY KEY REFERENCES app.delivery_attempts(attempt_id),
    next_attempt_at timestamptz NOT NULL CHECK (isfinite(next_attempt_at)),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE app.delivery_uncertain_reports (
    attempt_id text PRIMARY KEY REFERENCES app.delivery_uncertainties(attempt_id),
    pending boolean NOT NULL DEFAULT true
);
GRANT SELECT, INSERT ON app.delivery_uncertainties TO notifications_runtime;
GRANT SELECT, INSERT, UPDATE ON app.delivery_uncertain_reports TO notifications_runtime;
