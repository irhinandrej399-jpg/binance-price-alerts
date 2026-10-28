-- Disposable-test cleaner fault only; production runtime has no DELETE rights.
CREATE FUNCTION app.test_snapshot_remove(resource uuid, remove_snapshot boolean) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
BEGIN
    DELETE FROM app.subscription_replay_pins WHERE id=resource;
    IF remove_snapshot THEN
        DELETE FROM app.subscription_snapshot_rows WHERE snapshot_id=resource;
        DELETE FROM app.subscription_snapshots WHERE id=resource;
    END IF;
END
$body$;
REVOKE ALL ON FUNCTION app.test_snapshot_remove(uuid,boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.test_snapshot_remove(uuid,boolean) TO api_runtime;
