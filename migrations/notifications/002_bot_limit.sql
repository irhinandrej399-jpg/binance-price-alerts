-- One pacing budget per stable numeric Telegram bot ID, never per replica/token.
CREATE TABLE app.bot_limits (
    bot_id text PRIMARY KEY CHECK (bot_id ~ '^[1-9][0-9]*$'),
    interval_us bigint NOT NULL CHECK (interval_us > 0),
    next_allowed_at timestamptz NOT NULL
);
GRANT SELECT, INSERT, UPDATE ON app.bot_limits TO notifications_runtime;
