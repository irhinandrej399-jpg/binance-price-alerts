CREATE FUNCTION app.reject_assignment_request() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.request_id = 'write-failure' AND convert_from(NEW.request_body,'UTF8')::jsonb->>'Scope' = 'TestAssignmentRollback' THEN
        RAISE EXCEPTION 'injected assignment request write failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER assignment_write_failure BEFORE INSERT ON app.matcher_assignment_requests FOR EACH ROW EXECUTE FUNCTION app.reject_assignment_request();
CREATE FUNCTION app.reject_assignment_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.request_id = 'commit-failure' AND convert_from(NEW.request_body,'UTF8')::jsonb->>'Scope' = 'TestAssignmentRollback' THEN
        RAISE EXCEPTION 'injected assignment commit failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER assignment_commit_failure AFTER INSERT ON app.matcher_assignment_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_assignment_commit();
