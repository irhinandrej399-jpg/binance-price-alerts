-- Test-only failures after earlier writes, plus a deferred commit failure.
CREATE FUNCTION app.reject_price_work() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'matcher_price_candidates' THEN
        IF NEW.scope = 'TestTransactionalFailures' AND NEW.work_key = 'fail-accept' AND NEW.subscription_id = 'sub-001' THEN
            RAISE EXCEPTION 'injected candidate insert failure';
        END IF;
    ELSIF TG_TABLE_NAME = 'matcher_price_work' THEN
        IF NEW.scope = 'TestTransactionalFailures' AND NEW.work_key = 'fail-record' AND NEW.completed > 0 THEN
            RAISE EXCEPTION 'injected local progress failure';
        END IF;
    ELSIF TG_TABLE_NAME = 'matcher_price_checkpoints' THEN
        IF NEW.scope = 'TestTransactionalFailures' THEN
            RAISE EXCEPTION 'injected price checkpoint failure';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER price_candidate_failure BEFORE INSERT ON app.matcher_price_candidates FOR EACH ROW EXECUTE FUNCTION app.reject_price_work();
CREATE TRIGGER price_progress_failure BEFORE UPDATE ON app.matcher_price_work FOR EACH ROW EXECUTE FUNCTION app.reject_price_work();
CREATE TRIGGER price_checkpoint_failure BEFORE UPDATE ON app.matcher_price_checkpoints FOR EACH ROW EXECUTE FUNCTION app.reject_price_work();
CREATE FUNCTION app.reject_price_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.scope = 'TestCommitFailure' AND NEW.state <> 'pending' THEN
        RAISE EXCEPTION 'injected deferred commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER price_commit_failure AFTER UPDATE ON app.matcher_price_work DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_price_commit();
