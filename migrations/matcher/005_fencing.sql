-- Compatible SHARE locks allow concurrent writes. Assignment/revocation takes
-- UPDATE on this row, serializing authority changes with every guarded commit.
CREATE TABLE app.matcher_fence_gate (id integer PRIMARY KEY CHECK (id = 1));
INSERT INTO app.matcher_fence_gate VALUES (1);
CREATE TABLE app.matcher_assignments (
    assignment_key text PRIMARY KEY,
    scope text NOT NULL UNIQUE CHECK (btrim(scope) <> ''),
    token bigint NOT NULL CHECK (token > 0),
    grant_body bytea NOT NULL,
    active boolean NOT NULL
);
CREATE TABLE app.matcher_assignment_requests (
    assignment_key text NOT NULL REFERENCES app.matcher_assignments(assignment_key),
    request_id text NOT NULL CHECK (btrim(request_id) <> ''),
    request_body bytea NOT NULL,
    grant_body bytea NOT NULL,
    PRIMARY KEY (assignment_key, request_id)
);
-- Membership is durable, including obsolete generations. No unprotected writer
-- may reuse an enrolled projection identity after handover or revocation.
CREATE TABLE app.matcher_projection_grants (
    projection_id text NOT NULL CHECK (btrim(projection_id) <> ''),
    assignment_key text NOT NULL REFERENCES app.matcher_assignments(assignment_key),
    grant_body bytea NOT NULL,
    PRIMARY KEY (projection_id, assignment_key)
);
GRANT SELECT ON app.matcher_fence_gate TO matcher_runtime;
-- PostgreSQL requires UPDATE privilege for row locking.
GRANT UPDATE (id) ON app.matcher_fence_gate TO matcher_runtime;
GRANT SELECT, INSERT ON app.matcher_assignments, app.matcher_assignment_requests,
    app.matcher_projection_grants TO matcher_runtime;
GRANT UPDATE (token,grant_body,active) ON app.matcher_assignments TO matcher_runtime;
GRANT UPDATE (grant_body) ON app.matcher_projection_grants TO matcher_runtime;
