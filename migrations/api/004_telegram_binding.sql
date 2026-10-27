-- The user row is the common serialization point for channel changes,
-- subscription creation and delivery admission (DEC-005/006).
ALTER TABLE app.users
    ADD COLUMN telegram_chat_id bigint,
    ADD COLUMN telegram_version bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT users_telegram_chat_key UNIQUE (telegram_chat_id),
    ADD CONSTRAINT users_telegram_binding_valid CHECK (
        (telegram_chat_id IS NULL OR telegram_chat_id > 0)
        AND telegram_version >= 0
        AND (telegram_chat_id IS NULL OR telegram_version > 0)
    );
