-- Transactional C, not a PostgreSQL sequence: a writer holds this row until
-- commit/rollback, so a later commit can never appear below a reader's C.
CREATE TABLE app.api_journal_counter (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    position bigint NOT NULL CHECK (position >= 0)
);
INSERT INTO app.api_journal_counter (singleton,position) VALUES (true,0);
REVOKE ALL ON app.api_journal_counter FROM api_runtime;
GRANT SELECT ON app.api_journal_counter TO api_runtime;
GRANT UPDATE(position) ON app.api_journal_counter TO api_runtime;

-- Exactly one complete batch per top-level transaction. xid8 avoids 32-bit
-- transaction ID wraparound. transaction_id is identity only, never ordering.
CREATE TABLE app.api_journal_batches (
    transaction_id xid8 PRIMARY KEY,
    transaction_end bigint NOT NULL UNIQUE CHECK (transaction_end > 0)
);
REVOKE ALL ON app.api_journal_batches FROM api_runtime;
GRANT SELECT, INSERT ON app.api_journal_batches TO api_runtime;

-- The validated wire envelope is stored verbatim (bytea), retaining exact JSON
-- integers, decimal strings, IDs and nanosecond timestamps for replay/publish.
-- Runtime may append/read only; retention and publication have separate tickets.
CREATE TABLE app.api_journal (
    position bigint PRIMARY KEY CHECK (position > 0),
    transaction_end bigint NOT NULL REFERENCES app.api_journal_batches(transaction_end),
    event_id text NOT NULL UNIQUE CHECK (btrim(event_id) <> ''),
    body bytea NOT NULL CHECK (octet_length(body) > 0),
    CHECK (position <= transaction_end)
);
CREATE INDEX api_journal_transaction ON app.api_journal(transaction_end,position);
REVOKE ALL ON app.api_journal FROM api_runtime;
GRANT SELECT, INSERT ON app.api_journal TO api_runtime;
