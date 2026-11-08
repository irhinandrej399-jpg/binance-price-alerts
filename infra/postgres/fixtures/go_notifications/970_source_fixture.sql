-- TEST ONLY. check_postgres runs the isolated binding package before all other
-- notification writers. This reset must never be applied to a working database.
CREATE FUNCTION app.test_reset_delivery_source() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
BEGIN
 TRUNCATE app.delivery_intake_source,app.delivery_checkpoints,app.delivery_jobs CASCADE;
END $$;
REVOKE ALL ON FUNCTION app.test_reset_delivery_source() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.test_reset_delivery_source() TO notifications_runtime;
CREATE FUNCTION app.test_bootstrap_failure() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.topic='fail-bootstrap' OR NEW.topic='fail-bootstrap-commit' THEN
  RAISE EXCEPTION 'injected bootstrap failure';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER source_checkpoint_failure BEFORE INSERT ON app.delivery_checkpoints
 FOR EACH ROW WHEN (NEW.topic='fail-bootstrap' AND NEW.partition=1)
 EXECUTE FUNCTION app.test_bootstrap_failure();
CREATE CONSTRAINT TRIGGER source_commit_failure AFTER INSERT ON app.delivery_intake_source
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
 WHEN (NEW.topic='fail-bootstrap-commit') EXECUTE FUNCTION app.test_bootstrap_failure();
