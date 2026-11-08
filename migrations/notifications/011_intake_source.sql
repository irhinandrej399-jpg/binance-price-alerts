-- One logical intake per notifications database. No legacy backfill.
CREATE TABLE app.delivery_intake_source (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 consumer_id text NOT NULL CHECK (btrim(consumer_id)<>''),
 generation text NOT NULL CHECK (btrim(generation)<>''),
 source_cluster text NOT NULL CHECK (btrim(source_cluster)<>''),
 source_topic_id bytea NOT NULL CHECK (octet_length(source_topic_id)=16 AND source_topic_id<>decode(repeat('00',16),'hex')),
 topic text NOT NULL CHECK (btrim(topic)<>''),
 partitions integer[] NOT NULL CHECK (cardinality(partitions)>0)
);
REVOKE ALL ON app.delivery_intake_source FROM notifications_runtime;
GRANT SELECT ON app.delivery_intake_source TO notifications_runtime;
REVOKE DELETE ON app.delivery_checkpoints FROM notifications_runtime;

-- Legacy transactions and bootstrap share this lock. READ COMMITTED ensures
-- the query after waiting sees a binding committed by a concurrent bootstrap.
-- Bound functions run as migration owner; no user-settable GUC is trusted.
CREATE FUNCTION app.guard_delivery_intake() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,app AS $$
BEGIN
 IF current_user <> 'notifications_migration' THEN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
   RAISE EXCEPTION 'legacy intake requires read committed' USING ERRCODE='25000';
  END IF;
  PERFORM pg_advisory_xact_lock(64064,1);
  IF EXISTS (SELECT FROM app.delivery_intake_source) THEN
   RAISE EXCEPTION 'bound intake requires source API' USING ERRCODE='42501';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER delivery_checkpoint_source_guard BEFORE INSERT OR UPDATE ON app.delivery_checkpoints
 FOR EACH ROW EXECUTE FUNCTION app.guard_delivery_intake();
CREATE TRIGGER delivery_job_source_guard BEFORE INSERT OR UPDATE OF trigger_id,subscription_id,user_id,event_id,body ON app.delivery_jobs
 FOR EACH ROW EXECUTE FUNCTION app.guard_delivery_intake();

CREATE FUNCTION app.delivery_require_source(p_consumer text,p_generation text,p_cluster text,p_tid bytea,p_topic text)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,app AS $$
DECLARE r app.delivery_intake_source;
BEGIN
 SELECT * INTO r FROM app.delivery_intake_source;
 IF NOT FOUND THEN
  IF EXISTS(SELECT FROM app.delivery_checkpoints) OR EXISTS(SELECT FROM app.delivery_jobs) THEN
   RAISE EXCEPTION 'legacy intake has no provenance' USING ERRCODE='P0640';
  END IF;
  RAISE EXCEPTION 'intake not bootstrapped' USING ERRCODE='P0641';
 END IF;
 IF (r.consumer_id,r.generation,r.source_cluster,r.source_topic_id,r.topic)
  IS DISTINCT FROM (p_consumer,p_generation,p_cluster,p_tid,p_topic) THEN
  RAISE EXCEPTION 'intake identity mismatch' USING ERRCODE='P0642';
 END IF;
END $$;
CREATE FUNCTION app.delivery_bootstrap(p_consumer text,p_generation text,p_cluster text,p_tid bytea,p_topic text,p_parts integer[])
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
DECLARE parts integer[];
BEGIN
 IF current_setting('transaction_isolation') <> 'read committed' THEN
  RAISE EXCEPTION 'bootstrap requires read committed' USING ERRCODE='25000';
 END IF;
 IF p_consumer IS NULL OR btrim(p_consumer)='' OR p_generation IS NULL OR btrim(p_generation)='' OR
    p_cluster IS NULL OR btrim(p_cluster)='' OR p_tid IS NULL OR octet_length(p_tid)<>16 OR
    p_tid=decode(repeat('00',16),'hex') OR p_topic IS NULL OR btrim(p_topic)='' OR
    p_parts IS NULL OR cardinality(p_parts)=0 OR array_position(p_parts,NULL) IS NOT NULL OR
    EXISTS(SELECT FROM unnest(p_parts) v WHERE v<0) THEN
  RAISE EXCEPTION 'invalid intake bootstrap' USING ERRCODE='22023';
 END IF;
 SELECT array_agg(DISTINCT v ORDER BY v) INTO parts FROM unnest(p_parts) v;
 IF cardinality(parts)<>cardinality(p_parts) THEN
  RAISE EXCEPTION 'duplicate partition' USING ERRCODE='22023';
 END IF;
 PERFORM pg_advisory_xact_lock(64064,1);
 IF EXISTS(SELECT FROM app.delivery_intake_source) THEN
  PERFORM app.delivery_require_source(p_consumer,p_generation,p_cluster,p_tid,p_topic);
  IF (SELECT partitions FROM app.delivery_intake_source) IS DISTINCT FROM parts THEN
   RAISE EXCEPTION 'partition set mismatch' USING ERRCODE='P0642';
  END IF;
  RETURN;
 END IF;
 IF EXISTS(SELECT FROM app.delivery_checkpoints) OR EXISTS(SELECT FROM app.delivery_jobs) THEN
  RAISE EXCEPTION 'legacy intake has no provenance' USING ERRCODE='P0640';
 END IF;
 INSERT INTO app.delivery_intake_source VALUES(true,p_consumer,p_generation,p_cluster,p_tid,p_topic,parts);
 INSERT INTO app.delivery_checkpoints(topic,partition,next_offset) SELECT p_topic,v,0 FROM unnest(parts) v;
END $$;
CREATE FUNCTION app.delivery_accept_bound(p_consumer text,p_generation text,p_cluster text,p_tid bytea,p_topic text,
 p_partition integer,p_expected bigint,p_offset bigint,p_trigger text,p_subscription text,p_user text,p_event text,p_body bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
DECLARE n bigint;
BEGIN
 -- Check provenance even for duplicates below the durable next position.
 PERFORM app.delivery_require_source(p_consumer,p_generation,p_cluster,p_tid,p_topic);
 IF p_expected IS NULL OR p_offset IS NULL OR p_expected<0 OR p_offset<p_expected OR p_offset=9223372036854775807 THEN
  RAISE EXCEPTION 'invalid prefix' USING ERRCODE='P0643';
 END IF;
 SELECT next_offset INTO n FROM app.delivery_checkpoints WHERE topic=p_topic AND partition=p_partition FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'partition not bootstrapped' USING ERRCODE='P0641'; END IF;
 IF p_offset<n THEN RETURN; END IF;
 IF n<>p_expected THEN RAISE EXCEPTION 'prefix conflict' USING ERRCODE='P0643'; END IF;
 INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body)
 VALUES(p_trigger,p_subscription,p_user,p_event,p_body) ON CONFLICT(trigger_id) DO NOTHING;
 UPDATE app.delivery_checkpoints SET next_offset=p_offset+1 WHERE topic=p_topic AND partition=p_partition;
END $$;
REVOKE ALL ON FUNCTION app.delivery_require_source(text,text,text,bytea,text),
 app.delivery_bootstrap(text,text,text,bytea,text,integer[]),
 app.delivery_accept_bound(text,text,text,bytea,text,integer,bigint,bigint,text,text,text,text,bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.delivery_bootstrap(text,text,text,bytea,text,integer[]),
 app.delivery_accept_bound(text,text,text,bytea,text,integer,bigint,bigint,text,text,text,text,bytea) TO notifications_runtime;
