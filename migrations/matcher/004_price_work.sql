-- Resumable price evaluation is independent of subscription projection C/offsets.
CREATE TABLE app.matcher_price_work (
    scope text NOT NULL CHECK (btrim(scope) <> ''),
    work_key text NOT NULL CHECK (btrim(work_key) <> ''),
    input bytea NOT NULL CHECK (octet_length(input) > 0),
    total integer NOT NULL CHECK (total >= 0),
    completed integer NOT NULL DEFAULT 0 CHECK (completed >= 0 AND completed <= total),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','completed','superseded')),
    superseded_by bytea,
    CHECK ((state = 'superseded') = (superseded_by IS NOT NULL)),
    CHECK (state <> 'completed' OR completed = total),
    PRIMARY KEY (scope,work_key)
);
CREATE INDEX matcher_price_pending ON app.matcher_price_work(scope,work_key) WHERE state = 'pending';
CREATE TABLE app.matcher_price_candidates (
    scope text NOT NULL,
    work_key text NOT NULL,
    subscription_id text NOT NULL CHECK (btrim(subscription_id) <> ''),
    candidate bytea NOT NULL CHECK (octet_length(candidate) > 0),
    result bytea CHECK (octet_length(result) > 0),
    PRIMARY KEY (scope,work_key,subscription_id),
    FOREIGN KEY (scope,work_key) REFERENCES app.matcher_price_work(scope,work_key)
);
CREATE TABLE app.matcher_price_checkpoints (
    scope text NOT NULL CHECK (btrim(scope) <> ''),
    topic text NOT NULL CHECK (btrim(topic) <> ''),
    partition integer NOT NULL CHECK (partition >= 0),
    next_offset bigint NOT NULL CHECK (next_offset >= 0),
    PRIMARY KEY (scope,topic,partition)
);
GRANT SELECT, INSERT ON app.matcher_price_work, app.matcher_price_candidates, app.matcher_price_checkpoints TO matcher_runtime;
GRANT UPDATE (completed,state,superseded_by) ON app.matcher_price_work TO matcher_runtime;
GRANT UPDATE (result) ON app.matcher_price_candidates TO matcher_runtime;
GRANT UPDATE (next_offset) ON app.matcher_price_checkpoints TO matcher_runtime;
