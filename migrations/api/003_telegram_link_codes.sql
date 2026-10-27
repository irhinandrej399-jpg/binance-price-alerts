-- API owns one current linking credential per account. No plaintext code.
-- Expiration is enforced by consumption, not by an asynchronous cleanup job.
CREATE TABLE app.telegram_link_codes (
    user_id uuid PRIMARY KEY REFERENCES app.users(id) ON DELETE CASCADE,
    code_hash bytea NOT NULL UNIQUE CHECK (octet_length(code_hash) = 32),
    expires_at timestamptz NOT NULL
);
