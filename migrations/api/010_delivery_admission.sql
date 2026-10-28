-- Canonical current permission identity; old permissions remain immutable audit
-- data. Result writers must fence negative results by this identity and token,
-- while verified late success follows DEC-004 (SUB-019).
ALTER TABLE app.subscriptions
    ADD COLUMN admission_fence bigint NOT NULL DEFAULT 0 CHECK (admission_fence >= 0),
    ADD COLUMN current_attempt_id text,
    ADD CONSTRAINT subscriptions_attempt_valid CHECK (current_attempt_id IS NULL
        OR (btrim(current_attempt_id) <> '' AND admission_fence > 0));

-- Retained command results, including wait/busy/blocked/terminal. A retry never
-- extends a permission. Logical identities deliberately have no cascading FK.
CREATE TABLE app.delivery_admissions (
    subscription_id uuid NOT NULL,
    request_id text NOT NULL CHECK (length(request_id) BETWEEN 1 AND 200),
    workflow_id text NOT NULL CHECK (btrim(workflow_id) <> ''),
    attempt_id text UNIQUE,
    fence bigint,
    expires_at timestamptz,
    chat_id bigint,
    binding_version bigint,
    response bytea NOT NULL CHECK (octet_length(response)>0),
    PRIMARY KEY (subscription_id,request_id),
    UNIQUE (subscription_id,fence),
    CHECK ((attempt_id IS NULL AND fence IS NULL AND expires_at IS NULL AND chat_id IS NULL AND binding_version IS NULL)
        OR (attempt_id IS NOT NULL AND btrim(attempt_id) <> '' AND fence IS NOT NULL AND fence > 0
            AND expires_at IS NOT NULL AND chat_id IS NOT NULL AND chat_id > 0 AND binding_version IS NOT NULL AND binding_version > 0))
);
REVOKE ALL ON app.delivery_admissions FROM api_runtime;
GRANT SELECT, INSERT ON app.delivery_admissions TO api_runtime;
