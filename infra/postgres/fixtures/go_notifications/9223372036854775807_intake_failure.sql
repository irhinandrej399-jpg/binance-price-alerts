CREATE FUNCTION app.reject_intake_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.topic = 'fail-checkpoint' AND NEW.next_offset > 0 THEN
        RAISE EXCEPTION 'injected checkpoint failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER intake_checkpoint_failure BEFORE UPDATE ON app.delivery_checkpoints
FOR EACH ROW EXECUTE FUNCTION app.reject_intake_checkpoint();
CREATE FUNCTION app.reject_intake_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.trigger_id = 'fail-commit' THEN
        RAISE EXCEPTION 'injected deferred commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER intake_commit_failure AFTER INSERT ON app.delivery_jobs
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_intake_commit();
