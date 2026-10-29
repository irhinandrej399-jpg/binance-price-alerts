-- Test-only failure after projection writes, at the checkpoint update.
CREATE FUNCTION app.reject_matcher_checkpoint() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.projection_id = 'TestWholeTransactionRollbackAndInvalidInput' THEN
        RAISE EXCEPTION 'injected checkpoint failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER matcher_checkpoint_failure BEFORE UPDATE ON app.matcher_checkpoints
FOR EACH ROW EXECUTE FUNCTION app.reject_matcher_checkpoint();
