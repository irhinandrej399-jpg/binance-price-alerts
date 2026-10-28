#!/usr/bin/env python3
"""Run Go storage tests against a disposable, isolated PostgreSQL project."""
import fcntl
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
PROJECT = 'binance-go-check'


def main():
    # Fixed project prevents accidentally targeting the developer's database.
    # Serialize runs so one invocation cannot remove another's test database.
    with (Path(tempfile.gettempdir()) / 'binance-go-check.lock').open('w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            sys.exit('another Go PostgreSQL check is running')
        env = os.environ.copy()
        port = int(env.get('GO_POSTGRES_CHECK_PORT', '55436'))
        if not 1024 <= port <= 65535 or port == 5432:
            sys.exit('GO_POSTGRES_CHECK_PORT must be 1024..65535, excluding 5432')
        env['POSTGRES_PORT'] = str(port)
        env['GOTOOLCHAIN'] = 'local'
        compose = ['docker', 'compose', '-p', PROJECT, '-f',
                   str(ROOT / 'infra/postgres/compose.yaml')]

        def run(command, **kwargs):
            return subprocess.run(command, cwd=ROOT, env=env, check=True, **kwargs)

        try:
            run(compose + ['up', '-d', '--wait', '--wait-timeout', '120'])
            with tempfile.TemporaryDirectory(prefix='binance-go-migrations-') as tmp:
                directory = Path(tmp)
                for source in (ROOT / 'migrations/api', ROOT / 'infra/postgres/fixtures/go'):
                    for sql in source.glob('*.sql'):
                        if (directory / sql.name).exists():
                            raise RuntimeError('fixture migration name conflicts with application')
                        shutil.copyfile(sql, directory / sql.name)
                run([sys.executable, str(ROOT / 'scripts/migrate.py'), '--owner', 'api',
                     '--project', PROJECT, '--directory', str(directory)])
            run([sys.executable, str(ROOT / 'scripts/migrate.py'), '--owner', 'market_data',
                 '--project', PROJECT])
            market_password = run(compose + ['exec', '-T', 'postgres', 'sh', '-c',
                                             'printf %s "$MARKET_DATA_RUNTIME_PASSWORD"'],
                                  capture_output=True, text=True).stdout
            if not market_password:
                raise RuntimeError('test market-data password is missing')
            env['MARKET_DATA_DATABASE_URL'] = (f'postgres://market_data_runtime:{quote(market_password, safe="")}@'
                                               f'127.0.0.1:{port}/market_data?sslmode=disable&connect_timeout=5')
            # Capture only the runtime password. Never print it or put it in argv.
            result = run(compose + ['exec', '-T', 'postgres', 'sh', '-c',
                                    'printf %s "$API_RUNTIME_PASSWORD"'],
                         capture_output=True, text=True)
            password = result.stdout
            if not password:
                raise RuntimeError('test runtime password is missing')
            env['DATABASE_URL'] = (f'postgres://api_runtime:{quote(password, safe="")}@'
                                   f'127.0.0.1:{port}/api?sslmode=disable&connect_timeout=5')
            env['POSTGRES_TEST'] = '1'
            # Relay claims the entire fixed shard. Run its recovery scenario on
            # the fresh outbox before other packages create unpublished work.
            relay = 'github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/relay'
            run(['go', 'test', '-race', '-count=1', '-v', relay])
            # Storage claims the whole outbox too; Latest fixtures must follow it.
            storage = 'github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage'
            run(['go', 'test', '-race', '-count=1', '-v', storage])
            packages = run(['go', 'list', './...'], capture_output=True, text=True).stdout.splitlines()
            # Journal tests assert global C; subscription HTTP now writes that journal.
            # Keep packages sequential on the shared disposable database.
            run(['go', 'test', '-p=1', '-race', '-count=1', '-v'] + [p for p in packages if p not in (relay, storage)])
        finally:
            # This project is explicitly disposable. Working binance-storage,
            # migration-check and role-check projects are never addressed.
            run(compose + ['down', '--volumes', '--remove-orphans'])
    print('OK: Go PostgreSQL checks complete; disposable test volume removed')


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as exc:
        # Do not render exception.cmd/output: future subprocess args or errors
        # may carry configuration. Commands already stream their safe output.
        sys.exit(f'Go PostgreSQL check failed (exit {exc.returncode})')
    except KeyboardInterrupt:
        sys.exit('Go PostgreSQL check interrupted')
