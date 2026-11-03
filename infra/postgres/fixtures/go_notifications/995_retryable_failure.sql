CREATE FUNCTION app.reject_retryable_report() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.attempt_id LIKE 'api-TestRetryableRollback/%' THEN
        RAISE EXCEPTION 'injected retryable commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER retryable_commit_failure AFTER INSERT ON app.delivery_retryable_reports
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_retryable_report();
