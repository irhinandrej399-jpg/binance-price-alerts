-- One durable pacing budget per (stable bot ID, Telegram chat ID).
CREATE TABLE app.chat_limits (
    bot_id text NOT NULL CHECK (bot_id ~ '^[1-9][0-9]*$'),
    chat_id bigint NOT NULL CHECK (chat_id <> 0),
    interval_us bigint NOT NULL CHECK (interval_us > 0),
    next_allowed_at timestamptz NOT NULL,
    PRIMARY KEY (bot_id, chat_id)
);
GRANT SELECT, INSERT, UPDATE ON app.chat_limits TO notifications_runtime;
