#!/usr/bin/env python3
"""Apply trusted transactional SQL files using the local owner's migration role."""
import argparse
import hashlib
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
OWNERS = ('api', 'market_data', 'matcher', 'notifications')


def literal(value):
    # Dollar quoting preserves SQL bytes, quotes and backslashes. Never interpolate
    # a migration into the surrounding PL/pgSQL program as executable source.
    tag = '$migration_file$'
    while tag in value:
        tag = tag[:-1] + '_$'
    return tag + value + tag


def load(directory):
    if not directory.is_dir():
        raise ValueError(f'not a migration directory: {directory}')
    migrations = []
    versions = set()
    for path in directory.iterdir():
        if path.suffix != '.sql':
            continue
        match = re.fullmatch(r'([0-9]+)_([a-z][a-z0-9_]*)\.sql', path.name)
        if not match or not path.is_file():
            raise ValueError(f'invalid migration file: {path.name}')
        version = int(match[1])
        if not 0 < version <= 9223372036854775807 or version in versions:
            raise ValueError(f'invalid or duplicate version: {path.name}')
        versions.add(version)
        data = path.read_bytes()
        sql = data.decode('utf-8')
        if not sql.strip() or '\x00' in sql:
            raise ValueError(f'empty SQL or NUL byte: {path.name}')
        migrations.append((version, path.name, hashlib.sha256(data).hexdigest(), sql))
    return sorted(migrations)


def program(owner, migrations):
    # Advisory locks are scoped to the current database. Acquire before even
    # creating history so two first-time runners cannot race on catalog DDL.
    parts = [f"""BEGIN;
SET LOCAL search_path = app, pg_catalog;
SET LOCAL synchronous_commit = on;
DO $role_check$ BEGIN
  IF current_user <> '{owner}_migration' OR current_database() <> '{owner}' THEN
    RAISE EXCEPTION 'wrong migration owner';
  END IF;
END $role_check$;
SELECT pg_advisory_xact_lock(180006, 4);
CREATE SCHEMA IF NOT EXISTS migration;
REVOKE ALL ON SCHEMA migration FROM PUBLIC, {owner}_runtime;
CREATE TABLE IF NOT EXISTS migration.history (
  version bigint PRIMARY KEY CHECK (version > 0),
  name text NOT NULL,
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON migration.history FROM PUBLIC, {owner}_runtime;
"""]
    # Migration roles deliberately lack TEMP in the owner setup. Use a CTE,
    # not a temporary table, to compare the complete manifest under the lock.
    values = ',\n'.join(f'({version}::bigint, {literal(name)}, {literal(checksum)})'
                        for version, name, checksum, _ in migrations)
    expected = f'VALUES {values}' if values else 'SELECT NULL::bigint, NULL::text, NULL::text WHERE false'
    parts.append(f"""DO $manifest$ BEGIN
  IF EXISTS (
    WITH expected(version, name, checksum) AS ({expected})
    SELECT FROM migration.history h LEFT JOIN expected e USING (version)
    WHERE e.version IS NULL OR h.name <> e.name OR h.checksum <> e.checksum
  ) THEN
    RAISE EXCEPTION 'migration history mismatch: applied file changed, renamed or missing';
  END IF;
END $manifest$;
""")
    for version, name, checksum, sql in migrations:
        body = f"""BEGIN
  IF NOT EXISTS (SELECT FROM migration.history WHERE version = {version}) THEN
    IF {version} < COALESCE((SELECT max(version) FROM migration.history), 0) THEN
      RAISE EXCEPTION 'out-of-order migration: {name}';
    END IF;
    RAISE NOTICE 'applying {name}';
    EXECUTE {literal(sql)};
    INSERT INTO migration.history(version, name, checksum)
      VALUES ({version}, {literal(name)}, {literal(checksum)});
  END IF;
END"""
        parts.append('DO ' + literal(body) + ';\n')
    parts.append('COMMIT;\n')
    return ''.join(parts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--owner', required=True, choices=OWNERS)
    parser.add_argument('--directory', type=Path, help='default: migrations/<owner>')
    parser.add_argument('--project', default='binance-storage', help='existing Compose project')
    args = parser.parse_args()
    try:
        sql = program(args.owner, load(args.directory or ROOT / 'migrations' / args.owner))
        # Password stays inside the container environment; never in argv or logs.
        command = ['docker', 'compose', '-p', args.project, '-f', str(ROOT / 'infra/postgres/compose.yaml'),
                   'exec', '-T', 'postgres', 'bash', '-c',
                   'key=${1^^}_MIGRATION_PASSWORD; export PGPASSWORD=${!key:?Missing migration password}; '
                   'exec psql -X -h 127.0.0.1 -U "${1}_migration" -d "$1" -v ON_ERROR_STOP=1',
                   'migrate', args.owner]
        return subprocess.run(command, input=sql, text=True).returncode
    except (OSError, ValueError) as error:
        print(f'migrate: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
