-- A deadline for checking API admission, never a Telegram send permission.
ALTER TABLE app.delivery_jobs ADD COLUMN next_check_at timestamptz;
CREATE INDEX delivery_channel_checks ON app.delivery_jobs(next_check_at,trigger_id)
    WHERE next_check_at IS NOT NULL AND state='blocked';
