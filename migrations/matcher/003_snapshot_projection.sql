-- SQL replay has no Kafka transport identity until a real Kafka prefix commits.
ALTER TABLE app.matcher_checkpoints
    ALTER COLUMN topic DROP NOT NULL,
    ALTER COLUMN partition DROP NOT NULL,
    ADD COLUMN snapshot_id text REFERENCES app.matcher_snapshot_generations(snapshot_id),
    ADD CONSTRAINT matcher_transport_presence CHECK (
        (topic IS NULL AND partition IS NULL AND next_offset = 0) OR
        (topic IS NOT NULL AND partition IS NOT NULL));
-- Preserve the complete snapshot protobuf without inventing journal positions.
-- A newer journal after-image replaces the snapshot representation atomically.
ALTER TABLE app.matcher_subscriptions
    ALTER COLUMN body DROP NOT NULL,
    ADD COLUMN snapshot_body bytea,
    ADD CONSTRAINT matcher_image_presence CHECK (
        (body IS NOT NULL AND snapshot_body IS NULL) OR
        (body IS NULL AND snapshot_body IS NOT NULL AND octet_length(snapshot_body) > 0));
