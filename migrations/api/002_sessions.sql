-- Bearer credentials are never stored in plaintext. API is the sole owner.
CREATE TABLE app.sessions (
    token_hash bytea PRIMARY KEY CHECK (octet_length(token_hash) = 32),
    user_id uuid NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL
);
CREATE INDEX sessions_user_id_idx ON app.sessions(user_id);
