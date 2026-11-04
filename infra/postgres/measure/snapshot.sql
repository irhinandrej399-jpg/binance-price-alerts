-- Read-only diagnostics, fresh psql session per snapshot/database. No stats resets.
SELECT jsonb_build_object(
 'database', current_database(), 'started_at', statement_timestamp(),
 'lsn', pg_current_wal_insert_lsn()::text,
 'database_bytes', pg_database_size(current_database()),
 'settings', (SELECT jsonb_object_agg(name, jsonb_build_object('value',setting,'unit',unit))
   FROM pg_settings WHERE name IN ('server_version','block_size','wal_segment_size','fsync',
   'synchronous_commit','full_page_writes','wal_sync_method','wal_compression','wal_level',
   'track_counts','track_io_timing','track_wal_io_timing','checkpoint_timeout','max_wal_size',
   'min_wal_size','checkpoint_completion_target','autovacuum','shared_buffers','data_checksums')),
 'wal', (SELECT to_jsonb(w) FROM pg_stat_wal w),
 'io', (SELECT jsonb_agg(to_jsonb(i)) FROM pg_stat_io i),
 'checkpointer', (SELECT to_jsonb(c) FROM pg_stat_checkpointer c),
 'bgwriter', (SELECT to_jsonb(b) FROM pg_stat_bgwriter b),
 'database_stats', (SELECT to_jsonb(d) - 'datid' FROM pg_stat_database d WHERE datname=current_database()),
 'wal_files', (SELECT jsonb_agg(to_jsonb(w)) FROM pg_ls_waldir() w),
 'temp_files', (SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM pg_ls_tmpdir() t),
 'tablespaces', (SELECT jsonb_agg(spcname) FROM pg_tablespace),
 'relations', (SELECT jsonb_agg(jsonb_build_object('oid',c.oid,'schema',n.nspname,
   'name',c.relname,'kind',c.relkind,'persistence',c.relpersistence,
   'shared',c.relisshared,'toast_oid',c.reltoastrelid,'fork',f.fork,'bytes',pg_relation_size(c.oid,f.fork)))
   FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   CROSS JOIN (VALUES ('main'),('fsm'),('vm'),('init')) f(fork)
   WHERE c.relkind IN ('r','i','t','S','m')),
 'ended_at', clock_timestamp());
