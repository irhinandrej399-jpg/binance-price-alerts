CREATE FUNCTION app.reject_uncertain_report() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.attempt_id IN ('api-TestUncertainRollback','api-TestExpiredUncertainRollback') THEN
        RAISE EXCEPTION 'injected uncertain commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER uncertain_commit_failure AFTER INSERT ON app.delivery_uncertain_reports
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_uncertain_report();
