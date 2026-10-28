-- User row is the shared binding/quota serialization point (DEC-006).
ALTER TABLE app.users ADD COLUMN active_subscriptions smallint NOT NULL DEFAULT 0
    CONSTRAINT users_active_subscriptions_range CHECK (active_subscriptions BETWEEN 0 AND 15);
-- Durable release marker: terminal retries must not decrement twice. Raw
-- persistence Insert is NOT a quota-aware creation operation.
ALTER TABLE app.subscriptions ADD COLUMN quota_reserved boolean NOT NULL DEFAULT false;
UPDATE app.subscriptions SET quota_reserved=true WHERE state NOT IN ('delivered','cancelled');
UPDATE app.users u SET active_subscriptions=(
    SELECT count(*) FROM app.subscriptions s WHERE s.owner_id=u.id AND s.quota_reserved
);
-- Existing data over quota fails the migration atomically; never truncate it.
