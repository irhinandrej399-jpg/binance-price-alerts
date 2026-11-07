#!/usr/bin/env python3
"""Verify the API child harness on unique disposable PostgreSQL/Kafka projects."""
import argparse
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def run(command, env, timeout=180, **kwargs):
    # Each command owns a process group. Timeout/interruption must not leave a
    # Go test or Compose CLI running while its resources are being removed.
    with subprocess.Popen(command, cwd=ROOT, env=env, start_new_session=True,
                          **kwargs) as process:
        try:
            output, _ = process.communicate(timeout=timeout)
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
            raise
        if process.returncode:
            raise RuntimeError(f'check command failed (exit {process.returncode})')
        return output


def main(owner="api"):
    if owner not in ("api", "market_data", "matcher", "notifications"):
        raise RuntimeError("unsupported process environment owner")
    market = owner == "market_data"
    matcher = owner == "matcher"
    notifications = owner == "notifications"
    label = "notifications" if notifications else ("matcher" if matcher else ("market-data" if market else "API"))
    parser = argparse.ArgumentParser(description=f"Verify a {label} child on disposable PostgreSQL/Kafka projects.")
    parser.add_argument('--package', default='./internal/testutil/notificationsprocess' if notifications else ('./internal/testutil/matcherprocess' if matcher else ('./internal/testutil/marketprocess' if market else './internal/testutil/apiprocess')),
                        help='Go test package for this disposable owner environment')
    args = parser.parse_args()
    if not args.package.startswith('./') or '..' in Path(args.package).parts:
        parser.error('--package must be a repository-relative Go package')
    suffix = uuid.uuid4().hex[:16]
    pg_project = 'binance-process-pg-' + suffix
    kafka_project = 'binance-process-kafka-' + suffix
    prefix = 'process-' + suffix
    # Remove all inherited owner DSNs and app settings. Only the selected runtime
    # credential (plus parent-only API fixture for matcher/notifications) is supplied to tests.
    # Child env is explicitly constructed by apiprocess.Config, with no parent inheritance.
    env = {k: v for k, v in os.environ.items()
           if k in ('PATH', 'HOME', 'TMPDIR', 'DOCKER_HOST', 'DOCKER_CONTEXT',
                    'GOPATH', 'GOCACHE', 'GOMODCACHE')}
    env['GOTOOLCHAIN'] = 'local'
    env['POSTGRES_PORT'] = str(free_port())
    env['KAFKA_PORT'] = str(free_port())
    while env['KAFKA_PORT'] == env['POSTGRES_PORT']:
        env['KAFKA_PORT'] = str(free_port())
    pg = ['docker', 'compose', '-p', pg_project, '-f',
          str(ROOT / 'infra/postgres/compose.yaml')]
    kafka = ['docker', 'compose', '-p', kafka_project, '-f',
             str(ROOT / 'infra/kafka/compose.yaml')]
    registry = tempfile.TemporaryDirectory(prefix="binance-process-pids-")
    env["API_PROCESS_PID_DIR"] = registry.name
    cleanup_errors = []
    try:
        run(pg + ['up', '-d', '--wait', '--wait-timeout', '120'], env)
        for db_owner in (("api", owner) if matcher or notifications else (owner,)):
            run([sys.executable, str(ROOT / 'scripts/migrate.py'), '--owner', db_owner,
                 '--project', pg_project], env)
            credential_key = db_owner.upper() + "_RUNTIME_PASSWORD"
            password = run(pg + ['exec', '-T', 'postgres', 'sh', '-c',
                                 'printf %s "$' + credential_key + '"'], env,
                           stdout=subprocess.PIPE, text=True)
            if not password:
                raise RuntimeError('missing owner runtime credential')
            dsn_variable = 'DATABASE_URL' if db_owner == 'api' else db_owner.upper() + '_DATABASE_URL'
            env[dsn_variable] = (f'postgres://{db_owner}_runtime:{quote(password, safe="")}@'
                                f'127.0.0.1:{env["POSTGRES_PORT"]}/{db_owner}'
                                '?sslmode=disable&connect_timeout=5')
        run(kafka + ['up', '-d', '--wait', '--wait-timeout', '180'], env, timeout=210)
        with (ROOT / 'infra/kafka/init-topics.sh').open() as script:
            run(kafka + ['exec', '-T', 'kafka', 'timeout', '90', 'bash', '-s', '--', prefix],
                env, timeout=100, stdin=script)
        env['NOTIFICATIONS_PROCESS_INTEGRATION' if notifications else ('MATCHER_PROCESS_INTEGRATION' if matcher else ('MARKET_DATA_PROCESS_INTEGRATION' if market else 'API_PROCESS_INTEGRATION'))] = '1'
        env.update(POSTGRES_TEST='1',
                   KAFKA_TEST_BROKER='127.0.0.1:' + env['KAFKA_PORT'],
                   KAFKA_TEST_PREFIX=prefix)
        if market or matcher or notifications:
            run(['go', 'test', '-race', '-count=1', '-v', '-timeout', '30s',
                 './internal/testutil/apiprocess'], env, timeout=60)
        # Keep room for crash/startup and graceful/recovery/fault scenarios,
        # including Kafka membership expiry after SIGKILL.
        run(['go', 'test', '-race', '-count=1', '-v', '-timeout',
             '210s' if notifications else ('150s' if matcher else '90s'), args.package], env,
            timeout=240 if notifications else (180 if matcher else 120))
    finally:
        # Go's test timeout/panic does not execute defers. Kill any registered
        # child process groups before removing their disposable infrastructure.
        for marker in Path(registry.name).iterdir():
            try:
                os.killpg(int(marker.name), signal.SIGKILL)
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    try:
                        os.kill(int(marker.name), 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.05)
                else:
                    raise RuntimeError('registered child did not exit')
                print('Cleanup verified: registered child group', flush=True)
            except ProcessLookupError:
                pass
            except Exception:
                cleanup_errors.append('registered child group')
        registry.cleanup()
        # Try BOTH cleanups even when one fails. Verify resources by exact
        # Compose label, including stopped containers, networks and volumes.
        for compose, project in ((kafka, kafka_project), (pg, pg_project)):
            try:
                run(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '10'],
                    env, timeout=60)
                for kind, args in (('container', ['ps', '-aq']),
                                   ('volume', ['volume', 'ls', '-q']),
                                   ('network', ['network', 'ls', '-q'])):
                    remaining = run(['docker'] + args + ['--filter',
                                    'label=com.docker.compose.project=' + project],
                                    env, timeout=15, stdout=subprocess.PIPE, text=True)
                    if remaining.strip():
                        raise RuntimeError(f'{kind} cleanup incomplete for {project}')
                print(f'Cleanup verified: {project}', flush=True)
            except Exception:
                cleanup_errors.append(project)
        if cleanup_errors:
            raise RuntimeError('cleanup failed: ' + ', '.join(cleanup_errors))
    print(f'PASS {label}: selected process checks passed; resources removed')


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        main()
    except KeyboardInterrupt:
        sys.exit('API process check interrupted')
    except Exception as exc:
        # Never print raw commands, SQL errors, DSNs or captured output.
        sys.exit('API process check failed: ' + (str(exc) if isinstance(exc, RuntimeError)
                                               else type(exc).__name__))
