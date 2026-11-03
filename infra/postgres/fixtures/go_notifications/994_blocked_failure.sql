CREATE FUNCTION app.reject_blocked_report() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.attempt_id = 'api-TestBlockedRollback' THEN
        RAISE EXCEPTION 'injected blocked commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER blocked_commit_failure AFTER INSERT ON app.delivery_blocked_reports
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_blocked_report();

CREATE FUNCTION app.reject_block_park() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM app.delivery_claims WHERE claim_id=NEW.claim_id AND trigger_id='TestParkBlockedStaleAndRollback') THEN
        RAISE EXCEPTION 'injected block park commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER block_park_commit_failure AFTER INSERT ON app.delivery_block_parks
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_block_park();
