-- No backfill: every pre-migration cursor remains unconfirmed.
ALTER TABLE app.matcher_checkpoints
 ADD COLUMN source_cluster text,
 ADD COLUMN source_topic_id bytea,
 ADD CONSTRAINT matcher_source_presence CHECK (
  (source_cluster IS NULL AND source_topic_id IS NULL) OR
  (btrim(source_cluster) <> '' AND source_cluster IS NOT NULL AND
   source_topic_id IS NOT NULL AND octet_length(source_topic_id)=16 AND
   source_topic_id <> decode(repeat('00',16),'hex') AND topic IS NOT NULL));
CREATE TABLE app.matcher_price_sources (
 scope text PRIMARY KEY CHECK (btrim(scope) <> ''),
 topic text NOT NULL CHECK (btrim(topic) <> ''),
 partition integer NOT NULL CHECK (partition >= 0),
 source_cluster text NOT NULL CHECK (btrim(source_cluster) <> ''),
 source_topic_id bytea NOT NULL CHECK (octet_length(source_topic_id)=16 AND source_topic_id <> decode(repeat('00',16),'hex'))
);
-- Default privileges must not expose the binding ledger to runtime writes.
REVOKE ALL ON app.matcher_price_sources FROM matcher_runtime;
GRANT SELECT ON app.matcher_price_sources TO matcher_runtime;
REVOKE DELETE ON app.matcher_checkpoints, app.matcher_price_checkpoints FROM matcher_runtime;

-- INVOKER triggers distinguish ordinary runtime SQL from the narrow owner
-- functions below. No GUC, application_name or caller-supplied flag is trusted.
CREATE FUNCTION app.guard_projection_source() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,app AS $$
BEGIN
 IF current_user <> 'matcher_migration' THEN
  IF TG_OP='INSERT' THEN
   IF NEW.source_cluster IS NOT NULL THEN
    RAISE EXCEPTION 'bound projection requires source API' USING ERRCODE='42501';
   END IF;
  ELSE
   IF OLD.source_cluster IS NOT NULL OR NEW.source_cluster IS NOT NULL THEN
    RAISE EXCEPTION 'bound projection requires source API' USING ERRCODE='42501';
   END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER matcher_source_guard BEFORE INSERT OR UPDATE OF projection_id,topic,partition,next_offset,source_cluster,source_topic_id ON app.matcher_checkpoints
 FOR EACH ROW EXECUTE FUNCTION app.guard_projection_source();

CREATE FUNCTION app.guard_price_source() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,app AS $$
DECLARE k text;
BEGIN
 k:=NEW.scope;
 -- Reject bound legacy updates before acquiring the scope lock. PostgreSQL has
 -- already taken a row lock for UPDATE; waiting for a bound writer's advisory
 -- lock here would invert its advisory -> row lock order.
 IF current_user <> 'matcher_migration' AND
   (EXISTS (SELECT FROM app.matcher_price_sources WHERE scope=k) OR
    (TG_OP='UPDATE' AND EXISTS (SELECT FROM app.matcher_price_sources WHERE scope=OLD.scope))) THEN
  RAISE EXCEPTION 'bound prices require source API' USING ERRCODE='42501';
 END IF;
 -- Same lock as initial binding: legacy writes cannot slip in during binding.
 PERFORM pg_advisory_xact_lock(hashtextextended(k, 61061));
 IF current_user <> 'matcher_migration' AND
   (EXISTS (SELECT FROM app.matcher_price_sources WHERE scope=k) OR
    (TG_OP='UPDATE' AND EXISTS (SELECT FROM app.matcher_price_sources WHERE scope=OLD.scope))) THEN
  RAISE EXCEPTION 'bound prices require source API' USING ERRCODE='42501';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER matcher_price_source_guard BEFORE INSERT OR UPDATE ON app.matcher_price_checkpoints
 FOR EACH ROW EXECUTE FUNCTION app.guard_price_source();

-- Functions remain in the caller's transaction, including all projection/work
-- effects. The migration owner is not a role available to matcher_runtime.
CREATE FUNCTION app.matcher_projection_source(p_id text,p_topic text,p_partition integer,p_cluster text,p_tid bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
DECLARE r app.matcher_checkpoints;
BEGIN
 IF btrim(p_cluster)='' OR p_cluster IS NULL OR p_tid IS NULL OR octet_length(p_tid)<>16 OR p_tid=decode(repeat('00',16),'hex') THEN
  RAISE EXCEPTION 'invalid source' USING ERRCODE='22023';
 END IF;
 INSERT INTO app.matcher_checkpoints(projection_id) VALUES(p_id) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT r FROM app.matcher_checkpoints WHERE projection_id=p_id FOR UPDATE;
 IF r.source_cluster IS NULL THEN
  IF r.topic IS NOT NULL OR r.next_offset<>0 THEN
   RAISE EXCEPTION 'unbound projection cursor' USING ERRCODE='P0610';
  END IF;
  UPDATE app.matcher_checkpoints SET topic=p_topic,partition=p_partition,source_cluster=p_cluster,source_topic_id=p_tid WHERE projection_id=p_id;
 ELSIF (r.topic,r.partition,r.source_cluster,r.source_topic_id) IS DISTINCT FROM (p_topic,p_partition,p_cluster,p_tid) THEN
  RAISE EXCEPTION 'source mismatch' USING ERRCODE='P0611';
 END IF;
END $$;
CREATE FUNCTION app.matcher_projection_advance(p_id text,p_topic text,p_partition integer,p_cluster text,p_tid bytea,p_c numeric,p_next bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
BEGIN
 PERFORM app.matcher_projection_source(p_id,p_topic,p_partition,p_cluster,p_tid);
 UPDATE app.matcher_checkpoints SET journal_c=p_c,next_offset=p_next WHERE projection_id=p_id AND journal_c<=p_c AND next_offset<=p_next;
 IF NOT FOUND THEN RAISE EXCEPTION 'checkpoint regression' USING ERRCODE='P0611'; END IF;
END $$;
CREATE FUNCTION app.matcher_prices_source(p_scope text,p_topic text,p_partition integer,p_cluster text,p_tid bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
DECLARE r app.matcher_price_sources;
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(p_scope,61061));
 SELECT * INTO r FROM app.matcher_price_sources WHERE scope=p_scope;
 IF NOT FOUND THEN
  IF EXISTS (SELECT FROM app.matcher_price_work WHERE scope=p_scope) OR EXISTS (SELECT FROM app.matcher_price_checkpoints WHERE scope=p_scope) THEN
   RAISE EXCEPTION 'unbound prices state' USING ERRCODE='P0610';
  END IF;
  INSERT INTO app.matcher_price_sources VALUES(p_scope,p_topic,p_partition,p_cluster,p_tid);
 ELSIF (r.topic,r.partition,r.source_cluster,r.source_topic_id) IS DISTINCT FROM (p_topic,p_partition,p_cluster,p_tid) THEN
  RAISE EXCEPTION 'source mismatch' USING ERRCODE='P0611';
 END IF;
END $$;
CREATE FUNCTION app.matcher_prices_advance(p_scope text,p_topic text,p_partition integer,p_cluster text,p_tid bytea,p_from bigint,p_next bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,app AS $$
DECLARE n bigint;
BEGIN
 PERFORM app.matcher_prices_source(p_scope,p_topic,p_partition,p_cluster,p_tid);
 IF p_from<0 OR p_next<=p_from THEN RAISE EXCEPTION 'invalid prefix' USING ERRCODE='22023'; END IF;
 INSERT INTO app.matcher_price_checkpoints VALUES(p_scope,p_topic,p_partition,p_from) ON CONFLICT DO NOTHING;
 SELECT next_offset INTO STRICT n FROM app.matcher_price_checkpoints WHERE scope=p_scope AND topic=p_topic AND partition=p_partition FOR UPDATE;
 IF n<p_next THEN
  IF n<>p_from THEN RAISE EXCEPTION 'prefix conflict' USING ERRCODE='P0612'; END IF;
  UPDATE app.matcher_price_checkpoints SET next_offset=p_next WHERE scope=p_scope AND topic=p_topic AND partition=p_partition;
 END IF;
END $$;
REVOKE ALL ON FUNCTION app.matcher_projection_source(text,text,integer,text,bytea),
 app.matcher_projection_advance(text,text,integer,text,bytea,numeric,bigint),
 app.matcher_prices_source(text,text,integer,text,bytea),
 app.matcher_prices_advance(text,text,integer,text,bytea,bigint,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.matcher_projection_source(text,text,integer,text,bytea),
 app.matcher_projection_advance(text,text,integer,text,bytea,numeric,bigint),
 app.matcher_prices_source(text,text,integer,text,bytea),
 app.matcher_prices_advance(text,text,integer,text,bytea,bigint,bigint) TO matcher_runtime;

-- Freeze provenance on acceptance. The scope lock also serializes an old INSERT
-- racing the first binding; it either commits first (binding refuses legacy)
-- or sees binding and refuses an unproven transport input.
CREATE FUNCTION app.guard_price_input_source() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,app AS $$
DECLARE s app.matcher_price_sources; p jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.scope,61061));
 SELECT * INTO s FROM app.matcher_price_sources WHERE scope=NEW.scope;
 IF FOUND THEN
  p:=convert_from(NEW.input,'UTF8')::jsonb->'Position';
  IF p IS NOT NULL AND p<>'null'::jsonb AND (
    p->'Source' IS NULL OR p->'Source'='null'::jsonb OR
    p->'Source'->>'ClusterID' IS DISTINCT FROM s.source_cluster OR
    p->'Source'->>'Topic' IS DISTINCT FROM s.topic OR
    p->>'Topic' IS DISTINCT FROM s.topic OR
    (p->>'Partition')::integer IS DISTINCT FROM s.partition OR
    p->'Source'->'TopicID' IS DISTINCT FROM (
      SELECT jsonb_agg(get_byte(s.source_topic_id,n) ORDER BY n) FROM generate_series(0,15) n)) THEN
   RAISE EXCEPTION 'unproven price input' USING ERRCODE='P0610';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER matcher_price_input_source_guard BEFORE INSERT ON app.matcher_price_work
 FOR EACH ROW EXECUTE FUNCTION app.guard_price_input_source();
