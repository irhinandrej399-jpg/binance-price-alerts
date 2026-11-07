-- Test-only version 006 deliberately precedes production 007_source_binding.
-- Literal old-schema rows prove the migration leaves offsets, Input bytes and
-- canonical partial outcomes untouched. Production has no data migration 006.
INSERT INTO app.matcher_checkpoints(projection_id,topic,partition,journal_c,next_offset)
 VALUES('source-pre-migration','changes',0,7,41);
INSERT INTO app.matcher_price_checkpoints VALUES('source-pre-migration','prices',2,41);
INSERT INTO app.matcher_price_work(scope,work_key,input,total,completed)
 VALUES('source-pre-migration','pending',convert_to('{"Key":"pending","ProjectionID":"legacy","C":7,"Quote":{"schema_version":1,"pair":"BTCUSDT","price":"100","source_id":"trade-1","revision":{"stream_id":"stream","generation":1,"sequence":1},"trade_time":"2026-10-29T10:00:00Z","received_at":"2026-10-29T10:00:01Z","stream_state":"live","gap_detected":false},"Position":{"Topic":"prices","Partition":2,"Offset":40},"Candidates":[{"OwnerID":"owner","SubscriptionID":"sub-000","Version":1},{"OwnerID":"owner","SubscriptionID":"sub-001","Version":1}]}','UTF8'),2,1);
INSERT INTO app.matcher_price_candidates(scope,work_key,subscription_id,candidate,result)
 VALUES('source-pre-migration','pending','sub-000',convert_to('{"OwnerID":"owner","SubscriptionID":"sub-000","Version":1}','UTF8'),convert_to('{"Outcome":"workflow","TriggerID":"pre-migration-canonical","WorkflowVersion":1}','UTF8')),
 ('source-pre-migration','pending','sub-001',convert_to('{"OwnerID":"owner","SubscriptionID":"sub-001","Version":1}','UTF8'),NULL);
-- Save exact pre-upgrade bytes and offsets for the read-only test assertion.
CREATE TABLE app.source_upgrade_expected AS SELECT input FROM app.matcher_price_work WHERE scope='source-pre-migration';
REVOKE ALL ON app.source_upgrade_expected FROM matcher_runtime;
GRANT SELECT ON app.source_upgrade_expected TO matcher_runtime;
