-- TEST ONLY: disposable process runner; never install on a working database.
CREATE FUNCTION app.test_reset_delivery_source() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
BEGIN
 TRUNCATE app.delivery_intake_source,app.delivery_checkpoints,app.delivery_jobs CASCADE;
END $$;
CREATE TABLE app.test_saved_delivery_source (LIKE app.delivery_intake_source INCLUDING ALL);
REVOKE ALL ON app.test_saved_delivery_source FROM PUBLIC, notifications_runtime;
CREATE FUNCTION app.test_legacy_delivery_source() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
BEGIN
 INSERT INTO app.test_saved_delivery_source SELECT * FROM app.delivery_intake_source;
 DELETE FROM app.delivery_intake_source;
END $$;
CREATE FUNCTION app.test_restore_delivery_source() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
BEGIN
 INSERT INTO app.delivery_intake_source SELECT * FROM app.test_saved_delivery_source;
 DELETE FROM app.test_saved_delivery_source;
END $$;
REVOKE ALL ON FUNCTION app.test_reset_delivery_source(),app.test_legacy_delivery_source(),app.test_restore_delivery_source() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.test_reset_delivery_source(),app.test_legacy_delivery_source(),app.test_restore_delivery_source() TO notifications_runtime;
