-- Permanent request tombstones survive physical removal of snapshots/pins.
-- No FK: cleaner may remove a resource but must never make its key reusable.
CREATE TABLE app.subscription_snapshot_requests (
    owner text NOT NULL CHECK (length(owner) BETWEEN 1 AND 200),
    request_id text NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128 AND request_id !~ '[^A-Za-z0-9_.-]'),
    ttl_ns bigint NOT NULL CHECK (ttl_ns BETWEEN 1000 AND 86400000000000),
    snapshot_id uuid NOT NULL UNIQUE,
    PRIMARY KEY (owner,request_id)
);
REVOKE ALL ON app.subscription_snapshot_requests FROM api_runtime;
GRANT SELECT,INSERT ON app.subscription_snapshot_requests TO api_runtime;
