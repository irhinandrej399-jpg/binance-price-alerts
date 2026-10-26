"""Real PostgreSQL checks. Opt in with MIGRATION_TEST_PROJECT."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
PROJECT = os.environ.get('MIGRATION_TEST_PROJECT')
if PROJECT and PROJECT != 'binance-migrations-check':
    raise RuntimeError('migration tests require the dedicated binance-migrations-check project')


@unittest.skipUnless(PROJECT, 'requires dedicated PostgreSQL project; see migrations/README.md')
class MigrationIntegrationTests(unittest.TestCase):
    def sql(self, owner, statement):
        return subprocess.check_output([
            'docker', 'compose', '-p', PROJECT, '-f', str(ROOT / 'infra/postgres/compose.yaml'),
            'exec', '-T', 'postgres', 'psql', '-X', '-U', 'postgres', '-d', owner,
            '-v', 'ON_ERROR_STOP=1', '-Atqc', 'SET client_min_messages=warning; ' + statement], text=True).strip()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        for owner in ('api', 'market_data', 'matcher', 'notifications'):
            self.sql(owner, 'DROP SCHEMA IF EXISTS migration CASCADE; DROP TABLE IF EXISTS app.migration_probe')

    def write(self, name, sql):
        (self.directory / name).write_text(sql)

    def command(self, owner='api'):
        return [sys.executable, str(ROOT / 'scripts/migrate.py'), '--owner', owner,
                '--directory', str(self.directory), '--project', PROJECT]

    def run_migration(self, owner='api', success=True):
        result = subprocess.run(self.command(owner), text=True, capture_output=True, timeout=30)
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result

    def test_repeat_checksum_and_owner_isolation(self):
        self.write('001_probe.sql', 'CREATE TABLE app.migration_probe(value int); INSERT INTO app.migration_probe VALUES (1);')
        for owner in ('api', 'market_data', 'matcher', 'notifications'):
            self.run_migration(owner)
            self.run_migration(owner)
            self.assertEqual(self.sql(owner, 'SELECT count(*) FROM app.migration_probe'), '1')
            self.assertEqual(self.sql(owner, 'SELECT count(*) FROM migration.history'), '1')
            self.assertEqual(self.sql(owner, f"SELECT has_schema_privilege('{owner}_runtime', 'migration', 'USAGE')"), 'f')
        self.write('001_probe.sql', 'SELECT 2;')
        result = self.run_migration(success=False)
        self.assertIn('migration history mismatch', result.stderr)
        self.assertEqual(self.sql('api', 'SELECT count(*) FROM app.migration_probe'), '1')

    def test_ddl_and_history_rollback_then_retry(self):
        self.write('001_probe.sql', 'CREATE TABLE app.migration_probe(value int);')
        self.write('002_bad.sql', 'ALTER TABLE app.migration_probe ADD COLUMN other int; SELECT missing_column;')
        self.assertIn('missing_column', self.run_migration(success=False).stderr)
        self.assertEqual(self.sql('api', "SELECT to_regclass('app.migration_probe') IS NULL"), 't')
        self.assertEqual(self.sql('api', "SELECT to_regclass('migration.history') IS NULL"), 't')
        (self.directory / '002_bad.sql').unlink()
        self.run_migration()
        self.write('002_bad.sql', 'ALTER TABLE app.migration_probe ADD COLUMN other int; SELECT missing_column;')
        self.assertIn('missing_column', self.run_migration(success=False).stderr)
        self.assertEqual(self.sql('api', 'SELECT count(*) FROM migration.history'), '1')
        self.assertEqual(self.sql('api', "SELECT count(*) FROM information_schema.columns WHERE table_schema='app' AND table_name='migration_probe' AND column_name='other'"), '0')

    def wait_sql(self, statement, expected):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.sql('api', statement) == expected:
                return
            time.sleep(0.1)
        self.fail(f'timed out waiting for {expected}: {statement}')

    def process(self, command):
        process = subprocess.Popen(command, text=True, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.communicate()
        self.addCleanup(cleanup)
        return process

    def test_concurrent_runners_and_independent_owner_lock(self):
        self.write('001_probe.sql', 'CREATE TABLE app.migration_probe(value int); INSERT INTO app.migration_probe VALUES (1);')
        holder = self.process([
            'docker', 'compose', '-p', PROJECT, '-f', str(ROOT / 'infra/postgres/compose.yaml'),
            'exec', '-T', 'postgres', 'psql', '-X', '-U', 'postgres', '-d', 'api',
            '-v', 'ON_ERROR_STOP=1', '-Atq'])
        holder.stdin.write('BEGIN; SELECT pg_advisory_xact_lock(180006, 4);\n')
        holder.stdin.flush()
        locks = "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=180006 AND objid=4 AND database=(SELECT oid FROM pg_database WHERE datname='api')"
        self.wait_sql(locks + ' AND granted', '1')
        first = self.process(self.command())
        second = self.process(self.command())
        self.wait_sql(locks + ' AND NOT granted', '2')
        # The same lock key in another owner DB must not block its migration.
        self.run_migration('matcher')
        self.assertEqual(self.sql('api', locks + ' AND NOT granted'), '2')
        _, err = holder.communicate('COMMIT;\n', timeout=10)
        self.assertEqual(holder.returncode, 0, err)
        for process in (first, second):
            _, err = process.communicate(timeout=30)
            self.assertEqual(process.returncode, 0, err)
        self.assertEqual(self.sql('api', 'SELECT count(*) FROM app.migration_probe'), '1')
        self.assertEqual(self.sql('api', 'SELECT count(*) FROM migration.history'), '1')

    def test_missing_or_renamed_applied_file_is_error(self):
        self.write('001_probe.sql', 'SELECT 1;')
        self.run_migration()
        (self.directory / '001_probe.sql').rename(self.directory / '001_renamed.sql')
        self.assertIn('migration history mismatch', self.run_migration(success=False).stderr)
        (self.directory / '001_renamed.sql').unlink()
        self.assertIn('migration history mismatch', self.run_migration(success=False).stderr)

    def test_invalid_and_out_of_order_versions(self):
        self.write('002_probe.sql', 'SELECT 1;')
        self.run_migration()
        self.write('001_old.sql', 'SELECT 2;')
        self.assertIn('out-of-order migration', self.run_migration(success=False).stderr)
        self.assertEqual(self.sql('api', 'SELECT count(*) FROM migration.history'), '1')
        self.write('2_duplicate.sql', 'SELECT 3;')
        self.assertIn('duplicate version', self.run_migration(success=False).stderr)
        self.assertEqual(self.sql('api', 'SELECT count(*) FROM migration.history'), '1')

    def test_dollar_quoted_sql_and_empty_directory(self):
        self.run_migration()
        self.assertEqual(self.sql('api', 'SELECT count(*) FROM migration.history'), '0')
        self.write('001_probe.sql', '''DO $migration_file$ BEGIN
          PERFORM 'a quote'' and a backslash\\ and $migration_file_$';
        END $migration_file$;
        CREATE TABLE app.migration_probe(value int);''')
        self.run_migration()
        self.assertEqual(self.sql('api', 'SELECT count(*) FROM migration.history'), '1')

    def test_transaction_control_cannot_escape_rollback(self):
        self.write('001_probe.sql', 'CREATE TABLE app.migration_probe(value int); COMMIT;')
        self.assertIn('EXECUTE of transaction commands is not implemented', self.run_migration(success=False).stderr)
        self.assertEqual(self.sql('api', "SELECT to_regclass('app.migration_probe') IS NULL"), 't')


if __name__ == '__main__':
    unittest.main()
