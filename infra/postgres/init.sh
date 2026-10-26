#!/bin/bash
set -euo pipefail
# No xtrace: passwords are read by psql from the environment, never command arguments.
for owner in api market_data matcher notifications; do
  key=${owner^^}
  runtime_key=${key}_RUNTIME_PASSWORD
  migration_key=${key}_MIGRATION_PASSWORD
  export OWNER_RUNTIME_PASSWORD=${!runtime_key:?Missing runtime password}
  export OWNER_MIGRATION_PASSWORD=${!migration_key:?Missing migration password}
  psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -v db="$owner" -v runtime="${owner}_runtime" -v migration="${owner}_migration" <<'SQL'
\getenv runtime_password OWNER_RUNTIME_PASSWORD
\getenv migration_password OWNER_MIGRATION_PASSWORD
CREATE ROLE :"runtime" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'runtime_password';
CREATE ROLE :"migration" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'migration_password';
CREATE DATABASE :"db" OWNER :"migration";
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO :"runtime";
ALTER ROLE :"runtime" IN DATABASE :"db" SET search_path = app, pg_catalog;
ALTER ROLE :"migration" IN DATABASE :"db" SET search_path = app, pg_catalog;
\connect :db
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA app AUTHORIZATION :"migration";
GRANT USAGE ON SCHEMA app TO :"runtime";
ALTER DEFAULT PRIVILEGES FOR ROLE :"migration" IN SCHEMA app GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"runtime";
ALTER DEFAULT PRIVILEGES FOR ROLE :"migration" IN SCHEMA app GRANT USAGE, SELECT ON SEQUENCES TO :"runtime";
-- No implicit EXECUTE by PUBLIC on future application functions.
ALTER DEFAULT PRIVILEGES FOR ROLE :"migration" REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
-- Migration roles can perform DDL, but cannot introduce non-durable tables.
-- Owned by the administrator in a locked schema, so a migration cannot disable it.
CREATE SCHEMA storage_guard AUTHORIZATION postgres;
REVOKE ALL ON SCHEMA storage_guard FROM PUBLIC;
CREATE FUNCTION storage_guard.reject_unlogged() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
  IF EXISTS (SELECT FROM pg_class WHERE relpersistence = 'u') THEN
    RAISE EXCEPTION 'unlogged relations are forbidden by DEC-006';
  END IF;
END;
$$;
REVOKE ALL ON FUNCTION storage_guard.reject_unlogged() FROM PUBLIC;
CREATE EVENT TRIGGER reject_unlogged ON ddl_command_end
EXECUTE FUNCTION storage_guard.reject_unlogged();
SQL
done
psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
REVOKE ALL ON DATABASE postgres FROM PUBLIC;
REVOKE ALL ON DATABASE template1 FROM PUBLIC;
REVOKE ALL ON DATABASE template0 FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
touch "$PGDATA/owners-ready"
