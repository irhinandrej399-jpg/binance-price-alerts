-- Source rows remain immutable. An acknowledgement is per identity: UUID order
-- is not commit order, so a high-water cursor would skip late commits.
CREATE TABLE app.delivery_relay (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 token bigint NOT NULL DEFAULT 0 CHECK (token >= 0),
 lease_until timestamptz
);
INSERT INTO app.delivery_relay (singleton) VALUES (true);
CREATE TABLE app.delivery_published (
 subscription_id uuid PRIMARY KEY REFERENCES app.delivery_outbox(subscription_id)
);
REVOKE ALL ON app.delivery_relay, app.delivery_published FROM api_runtime;
GRANT SELECT ON app.delivery_relay, app.delivery_published TO api_runtime;
GRANT UPDATE(token,lease_until) ON app.delivery_relay TO api_runtime;
GRANT INSERT ON app.delivery_published TO api_runtime;
