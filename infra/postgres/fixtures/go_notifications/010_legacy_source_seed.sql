-- Before application migration 011: real legacy bytes, attempts and reports.
INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body,state)
SELECT 'pre-source-'||v,'legacy-sub','legacy-user','legacy-event-'||v,decode('00ff0102','hex'),'legacy-state'
FROM generate_series(1,4) v;
INSERT INTO app.delivery_checkpoints VALUES('pre-source-topic',0,42);
INSERT INTO app.delivery_claims(claim_id,trigger_id,fence,claimed_at,expires_at,status)
SELECT 'pre-source-claim-'||v,'pre-source-'||v,1,'2026-01-01T00:00:00Z','2026-01-02T00:00:00Z','released' FROM generate_series(1,4) v;
INSERT INTO app.delivery_attempts(attempt_id,claim_id,trigger_id,fence)
SELECT 'pre-source-attempt-'||v,'pre-source-claim-'||v,'pre-source-'||v,1 FROM generate_series(1,4) v;
INSERT INTO app.delivery_successes(attempt_id,message_id) VALUES('pre-source-attempt-1',123);
INSERT INTO app.delivery_success_reports(attempt_id) VALUES('pre-source-attempt-1');
INSERT INTO app.delivery_uncertainties(attempt_id,next_attempt_at) VALUES('pre-source-attempt-2','2026-01-03T00:00:00Z');
INSERT INTO app.delivery_uncertain_reports(attempt_id) VALUES('pre-source-attempt-2');
INSERT INTO app.delivery_blocks(attempt_id,reason,binding_version) VALUES('pre-source-attempt-3','channel_unavailable',7);
INSERT INTO app.delivery_blocked_reports(attempt_id) VALUES('pre-source-attempt-3');
INSERT INTO app.delivery_retryables(attempt_id,kind,retry_after_seconds,binding_version,failures,exhausted,next_attempt_at)
VALUES('pre-source-attempt-4','server',0,7,1,false,'2026-01-03T00:00:00Z');
INSERT INTO app.delivery_retryable_reports(attempt_id) VALUES('pre-source-attempt-4');
CREATE FUNCTION app.test_delivery_snapshot() RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,app AS $$
 SELECT jsonb_build_object(
 'jobs',(SELECT jsonb_agg(to_jsonb(v) ORDER BY trigger_id) FROM app.delivery_jobs v),
 'cp',(SELECT jsonb_agg(to_jsonb(v) ORDER BY topic,partition) FROM app.delivery_checkpoints v),
 'claims',(SELECT jsonb_agg(to_jsonb(v) ORDER BY claim_id) FROM app.delivery_claims v),
 'attempts',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_attempts v),
 'success',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_successes v),
 'uncertain',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_uncertainties v),
 'blocks',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_blocks v),
 'retry',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_retryables v),
 'success_reports',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_success_reports v),
 'uncertain_reports',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_uncertain_reports v),
 'blocked_reports',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_blocked_reports v),
 'retryable_reports',(SELECT jsonb_agg(to_jsonb(v) ORDER BY attempt_id) FROM app.delivery_retryable_reports v))
$$;
CREATE TABLE app.test_delivery_legacy_snapshot AS SELECT app.test_delivery_snapshot() AS data;

GRANT EXECUTE ON FUNCTION app.test_delivery_snapshot() TO notifications_runtime;
