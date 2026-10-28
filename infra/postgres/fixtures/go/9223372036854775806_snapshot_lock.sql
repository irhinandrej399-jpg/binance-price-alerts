-- Disposable-test fault gate only; never installed in an application database.
-- Runtime keeps its real SELECT/INSERT-only snapshot permissions.
CREATE FUNCTION app.test_snapshot_lock(rows_table boolean) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
BEGIN
    IF rows_table THEN
        LOCK TABLE app.subscription_snapshot_rows IN SHARE MODE;
    ELSE
        LOCK TABLE app.subscription_snapshots IN SHARE MODE;
    END IF;
END
$body$;
REVOKE ALL ON FUNCTION app.test_snapshot_lock(boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.test_snapshot_lock(boolean) TO api_runtime;
