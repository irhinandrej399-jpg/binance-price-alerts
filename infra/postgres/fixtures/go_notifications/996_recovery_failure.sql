CREATE FUNCTION app.reject_recovery_schedule() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.trigger_id='TestRecoveryScheduleRollback' AND NEW.next_check_at IS NOT NULL THEN
        RAISE EXCEPTION 'injected recovery schedule commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER recovery_schedule_commit_failure AFTER UPDATE ON app.delivery_jobs
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_recovery_schedule();
CREATE FUNCTION app.reject_recovery_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.trigger_id='TestRecoveryClaimRollback' AND NEW.fence=2 THEN
        RAISE EXCEPTION 'injected recovery claim commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER recovery_claim_commit_failure AFTER INSERT ON app.delivery_claims
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_recovery_claim();
