-- Fail after snapshot rows are written, before marking the page durable.
CREATE FUNCTION app.reject_snapshot_page() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.snapshot_id = '33333333-3333-4333-8333-333333333333' THEN
        RAISE EXCEPTION 'injected snapshot page failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER snapshot_page_failure BEFORE UPDATE ON app.matcher_snapshot_generations
FOR EACH ROW EXECUTE FUNCTION app.reject_snapshot_page();
