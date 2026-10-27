-- API owns accounts. This durable row is the first lock in the DEC-006 order:
-- user (quota / Telegram binding) -> subscription -> workflow -> journal counter.
-- Quota and binding columns are added by their own implementation tickets.
CREATE TABLE app.users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email text NOT NULL CONSTRAINT users_email_key UNIQUE,
    password_hash text NOT NULL,
    CONSTRAINT users_email_normalized CHECK (email <> '' AND email = lower(email) AND email !~ '[[:space:]]'),
    CONSTRAINT users_password_hash_present CHECK (btrim(password_hash) <> '')
);
