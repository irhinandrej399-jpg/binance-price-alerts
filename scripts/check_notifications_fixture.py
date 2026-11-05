#!/usr/bin/env python3
"""OPS-028 infrastructure verification on four clean disposable datasets.

This checks setup/RPC correctness; it is not a measurement runner or collector.
"""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
PROJECT = 'binance-ops028-check'


def main():
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    with (Path(tempfile.gettempdir()) / 'binance-ops028-check.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        env = dict(os.environ, POSTGRES_PORT='55448', GOTOOLCHAIN='local',
                   OPS028_FIXTURE='1',
                   OPS028_SOURCE=str(ROOT / 'docs/measurements/OPS-024/run-a2/identities.json'),
                   OPS028_MATCHER=str(ROOT / 'docs/measurements/OPS-025/run-a3/identities.json'))
        compose = ['docker', 'compose', '-p', PROJECT, '-f',
                   str(ROOT / 'infra/postgres/compose.yaml')]

        def run(command, **kwargs):
            return subprocess.run(command, cwd=ROOT, env=env, check=True, **kwargs)

        for repeat in ('a', 'b'):
            for boundary in ('reports', 'handler'):
                name = f'{repeat}-{boundary}'
                evidence = output / f'{name}.json'
                env.update(OPS028_BOUNDARY=boundary, OPS028_EVIDENCE=str(evidence))
                cleanup = False
                try:
                    run(compose + ['up', '-d', '--wait', '--wait-timeout', '120'])
                    for owner in ('api', 'notifications'):
                        run([sys.executable, 'scripts/migrate.py', '--owner', owner,
                             '--project', PROJECT], stdout=subprocess.DEVNULL)
                        password = run(compose + ['exec', '-T', 'postgres', 'sh', '-c',
                            f'printf %s "${owner.upper()}_RUNTIME_PASSWORD"'],
                            capture_output=True, text=True).stdout
                        if not password:
                            raise RuntimeError('missing disposable password')
                        key = 'DATABASE_URL' if owner == 'api' else 'NOTIFICATIONS_DATABASE_URL'
                        env[key] = (f'postgres://{owner}_runtime:{quote(password, safe="")}@'
                                    f'127.0.0.1:55448/{owner}?sslmode=disable&connect_timeout=5')
                    run(['go', 'test', '-race', './internal/tools/apimeasure',
                         '-run', '^TestNotificationFixture(Source)?$', '-count=1', '-v'],
                        stdout=(output / f'{name}.txt').open('w'))
                finally:
                    run(compose + ['down', '--volumes', '--remove-orphans'])
                    remaining = run(['docker', 'ps', '-aq', '--filter',
                                     f'label=com.docker.compose.project={PROJECT}'],
                                    capture_output=True, text=True).stdout.strip()
                    volumes = run(['docker', 'volume', 'ls', '-q', '--filter',
                                   f'label=com.docker.compose.project={PROJECT}'],
                                  capture_output=True, text=True).stdout.strip()
                    cleanup = not remaining and not volumes
                    (output / f'{name}-cleanup.json').write_text(json.dumps({
                        'project': PROJECT, 'containers_removed': not remaining,
                        'volumes_removed': not volumes}) + '\n')
                    if not cleanup:
                        raise RuntimeError('disposable cleanup incomplete')
                print(f'PASS: {name}, cleanup={cleanup}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit(f'fixture verification failed (exit {error.returncode})')
