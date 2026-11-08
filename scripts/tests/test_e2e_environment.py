"""Runner contract: credentials stay in the parent and cleanup survives failure."""
import importlib.util
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('process_runner', Path(__file__).resolve().parents[1] / 'check_api_process.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class E2EEnvironmentTest(unittest.TestCase):
    def exercise(self, fail=False):
        calls = []
        def run(command, env, **kwargs):
            calls.append((list(command), dict(env)))
            if command[0] == 'go' and './internal/testutil/e2eenvironment' in command and fail:
                raise RuntimeError('injected owner failure')
            if 'printf %s' in ' '.join(command):
                return 'private/password'
            return ''
        with patch.object(runner, 'free_port', side_effect=[15432, 19092]), patch.object(runner, 'run', side_effect=run), patch.object(sys, 'argv', ['check']), patch.dict(os.environ, {'DATABASE_URL': 'inherited-secret', 'API_PROCESS_INTEGRATION': '1', 'TELEGRAM_BOT_TOKEN': 'inherited-secret'}):
            if fail:
                with self.assertRaisesRegex(RuntimeError, 'injected owner failure'):
                    runner.main('e2e')
            else:
                runner.main('e2e')
        migrations = [c for c, _ in calls if '--owner' in c]
        self.assertEqual([c[c.index('--owner')+1] for c in migrations], ['api', 'market_data', 'matcher', 'notifications'])
        self.assertTrue(all('--directory' not in c for c in migrations))
        go_env = next(e for c, e in calls if c[0] == 'go' and './internal/testutil/e2eenvironment' in c)
        for key in ('DATABASE_URL', 'MARKET_DATA_DATABASE_URL', 'MATCHER_DATABASE_URL', 'NOTIFICATIONS_DATABASE_URL'):
            self.assertIn('private%2Fpassword', go_env[key])
        self.assertEqual(go_env['E2E_PROCESS_INTEGRATION'], '1')
        self.assertNotIn('API_PROCESS_INTEGRATION', go_env)
        self.assertNotIn('TELEGRAM_BOT_TOKEN', go_env)
        self.assertFalse(Path(go_env['TMPDIR']).exists())
        self.assertFalse(Path(go_env['API_PROCESS_PID_DIR']).exists())
        self.assertEqual(sum('down' in c for c, _ in calls), 2)
        self.assertFalse(any('private/password' in ' '.join(c) or 'inherited-secret' in ' '.join(c) for c, _ in calls))

    def test_joint_environment(self):
        self.exercise()

    def test_cleanup_after_owner_failure(self):
        self.exercise(fail=True)

    def test_existing_modes_keep_credentials_and_migrations(self):
        for owner, expected in {
            'api': ['api'], 'market_data': ['market_data'],
            'matcher': ['api', 'matcher'], 'notifications': ['api', 'notifications'],
            'cleaner': ['api', 'matcher', 'notifications'],
        }.items():
            with self.subTest(owner=owner):
                calls = []
                def run(command, env, **kwargs):
                    calls.append((list(command), dict(env)))
                    return 'private/password' if 'printf %s' in ' '.join(command) else ''
                with patch.object(runner, 'free_port', side_effect=[15432, 19092]), patch.object(runner, 'run', side_effect=run), patch.object(sys, 'argv', ['check']):
                    runner.main(owner)
                migrations = [c for c, _ in calls if '--owner' in c]
                self.assertEqual([c[c.index('--owner')+1] for c in migrations], expected)
                self.assertEqual(sum('--directory' in c for c in migrations), int(owner == 'notifications'))
                env = next(e for c, e in calls if c[0] == 'go')
                expected_keys = {'DATABASE_URL' if db == 'api' else db.upper()+'_DATABASE_URL' for db in expected}
                self.assertEqual({k for k in env if k.endswith('DATABASE_URL')}, expected_keys)
                self.assertNotIn('E2E_PROCESS_INTEGRATION', env)
