CREATE FUNCTION app.reject_source_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='matcher_checkpoints' THEN
  IF NEW.projection_id='TestSourceProjectionRollback' AND NEW.next_offset>0 THEN
   RAISE EXCEPTION 'injected source projection failure';
  END IF;
 ELSIF NEW.scope='TestSourcePriceRollback' AND NEW.next_offset>0 THEN
  RAISE EXCEPTION 'injected source price checkpoint failure';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER source_projection_failure BEFORE UPDATE ON app.matcher_checkpoints FOR EACH ROW EXECUTE FUNCTION app.reject_source_checkpoint();
CREATE TRIGGER source_price_failure BEFORE UPDATE ON app.matcher_price_checkpoints FOR EACH ROW EXECUTE FUNCTION app.reject_source_checkpoint();
CREATE FUNCTION app.reject_source_accept() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.scope='TestSourceAcceptRollback' AND NEW.subscription_id='sub-001' THEN
  RAISE EXCEPTION 'injected source accept failure';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER source_accept_failure BEFORE INSERT ON app.matcher_price_candidates FOR EACH ROW EXECUTE FUNCTION app.reject_source_accept();
CREATE FUNCTION app.reject_source_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.scope='TestSourcePriceCommitRollback' AND NEW.state<>'pending' THEN
  RAISE EXCEPTION 'injected source deferred commit failure';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER source_commit_failure AFTER UPDATE ON app.matcher_price_work DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_source_commit();
CREATE FUNCTION app.reject_source_projection_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.projection_id='TestSourceProjectionCommitRollback' AND NEW.next_offset>0 THEN
  RAISE EXCEPTION 'injected source projection deferred commit failure';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER source_projection_commit_failure AFTER UPDATE ON app.matcher_checkpoints DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.reject_source_projection_commit();
