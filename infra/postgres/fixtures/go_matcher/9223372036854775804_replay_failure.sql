-- Fail only the replay checkpoint update, after the newer image was written.
CREATE FUNCTION app.reject_replay_checkpoint() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.projection_id = 'TestReplayRollback' THEN
        RAISE EXCEPTION 'injected replay checkpoint failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER replay_checkpoint_failure BEFORE UPDATE ON app.matcher_checkpoints
FOR EACH ROW EXECUTE FUNCTION app.reject_replay_checkpoint();

-- Initialization must roll back its checkpoint and every preceding image.
CREATE FUNCTION app.reject_initialized_image() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.projection_id = 'TestInitializeRollback'
       AND NEW.subscription_id = '00000000-0000-4000-8000-000000000002' THEN
        RAISE EXCEPTION 'injected initialize image failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER initialize_image_failure BEFORE INSERT ON app.matcher_subscriptions
FOR EACH ROW EXECUTE FUNCTION app.reject_initialized_image();
