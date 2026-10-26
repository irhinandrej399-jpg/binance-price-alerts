#!/bin/bash
set -euo pipefail
query() {
  local owner=$1 kind=$2 db=$3 sql=$4 key
  key=${owner^^}_${kind^^}_PASSWORD
  export PGPASSWORD=${!key}
  psql -X -h 127.0.0.1 -U "${owner}_${kind}" -d "$db" -v ON_ERROR_STOP=1 -Atqc "$sql"
}
reject() {
  local result
  if result=$(query "$@" 2>&1); then
    echo "FAIL: unexpected SQL success: $*" >&2
    exit 1
  fi
  # Distinguish privilege/guard rejection from network or syntax failures.
  if [[ $result != *'permission denied'* && $result != *'unlogged relations are forbidden'* && $result != *'must be owner of'* ]]; then
    echo "FAIL: unexpected rejection: $result" >&2
    exit 1
  fi
}
for owner in api market_data matcher notifications; do
  for kind in runtime migration; do
    [[ $(query "$owner" "$kind" "$owner" 'SELECT current_user') == "${owner}_${kind}" ]]
    for other in api market_data matcher notifications postgres template1; do
      if [[ $other != "$owner" ]]; then
        reject "$owner" "$kind" "$other" 'SELECT 1'
      fi
    done
    [[ $(query "$owner" "$kind" "$owner" "SELECT count(*) FROM pg_roles WHERE rolname=current_user AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)") == 1 ]]
    [[ $(query "$owner" "$kind" "$owner" 'SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)') == 0 ]]
    [[ $(query "$owner" "$kind" "$owner" 'SHOW search_path') == 'app, pg_catalog' ]]
    for setting in fsync synchronous_commit full_page_writes; do
      [[ $(query "$owner" "$kind" "$owner" "SHOW $setting") == on ]]
    done
  done
  reject "$owner" runtime "$owner" 'CREATE TABLE app.denied(id int)'
  reject "$owner" runtime "$owner" 'CREATE SCHEMA denied'
  reject "$owner" runtime "$owner" 'CREATE TEMP TABLE denied(id int)'
  reject "$owner" runtime "$owner" "SET ROLE ${owner}_migration"
  reject "$owner" runtime "$owner" 'CREATE ROLE denied'
  reject "$owner" migration "$owner" 'CREATE UNLOGGED TABLE app.denied(id int)'
  reject "$owner" migration "$owner" 'ALTER EVENT TRIGGER reject_unlogged DISABLE'
  query "$owner" migration "$owner" 'CREATE TABLE app.storage_check(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text NOT NULL)'
  query "$owner" runtime "$owner" "INSERT INTO app.storage_check(value) VALUES ('durable'); UPDATE app.storage_check SET value='updated'; SELECT value FROM app.storage_check" | { read -r value; [[ $value == updated ]]; }
  reject "$owner" runtime "$owner" 'ALTER TABLE app.storage_check ADD COLUMN denied int'
  reject "$owner" runtime "$owner" 'DROP TABLE app.storage_check'
  reject "$owner" migration "$owner" 'ALTER TABLE app.storage_check SET UNLOGGED'
  [[ $(query "$owner" runtime "$owner" "SELECT relpersistence FROM pg_class WHERE oid='app.storage_check'::regclass") == p ]]
  echo "OK: $owner — own DB, isolation, roles, DDL, CRUD, durability, unlogged guard"
done
# Password authentication must actually be in use over TCP.
if PGPASSWORD=deliberately-wrong psql -X -h 127.0.0.1 -U api_runtime -d api -c 'SELECT 1' >/dev/null 2>&1; then
  echo 'FAIL: wrong password accepted' >&2
  exit 1
fi
