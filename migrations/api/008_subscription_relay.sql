-- Publication progress is separate from immutable replay data and future pins.
CREATE TABLE app.subscription_relay (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    position bigint NOT NULL DEFAULT 0 CHECK (position >= 0),
    token bigint NOT NULL DEFAULT 0 CHECK (token >= 0),
    lease_until timestamptz
);
INSERT INTO app.subscription_relay (singleton) VALUES (true);
REVOKE ALL ON app.subscription_relay FROM api_runtime;
GRANT SELECT ON app.subscription_relay TO api_runtime;
GRANT UPDATE(position,token,lease_until) ON app.subscription_relay TO api_runtime;
