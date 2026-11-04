#!/usr/bin/env python3
"""Disposable PostgreSQL physical collector; no functional fixtures/test suite."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
from urllib.parse import quote
import uuid

from migrate import OWNERS, ROOT

SNAPSHOT = ROOT / 'infra/postgres/measure/snapshot.sql'


def save(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')


def lsn(value):
    hi, lo = value.split('/')
    return (int(hi, 16) << 32) + int(lo, 16)


def summarize(raw):
    # Physical relations are enumerated once, including TOAST + its indexes.
    groups = {}
    for db, snap in raw.items():
        sizes = {}
        for rel in snap['relations']:
            key = f"{rel['schema']}/{rel['kind']}/{rel['fork']}"
            sizes[key] = sizes.get(key, 0) + rel['bytes']
        groups[db] = {'relation_bytes': sizes, 'database_bytes': snap['database_bytes'],
                      'temp_bytes': snap['database_stats']['temp_bytes'],
                      'temp_files': snap['database_stats']['temp_files']}
    cluster = raw['api']  # Cluster-wide counters only once; never sum four DBs.
    return {'databases': groups, 'wal_insert_bytes': lsn(cluster['lsn']),
            'wal_segment_file_bytes': sum(w['size'] for w in cluster['wal_files']),
            'wal': cluster['wal'], 'io': cluster['io'],
            'checkpointer': cluster['checkpointer'], 'bgwriter': cluster['bgwriter']}


def delta(before, after):
    # All numeric counters and gauges are retained; nonnumeric fields stay in raw.
    if isinstance(after, dict):
        before = before if isinstance(before, dict) else {}
        return {k: delta(before.get(k, 0), v) for k, v in after.items()
                if (v is None or isinstance(v, (dict, int, float, list))) and not isinstance(v, bool)}
    if isinstance(after, list):
        # pg_stat_io ordering is unspecified; key by backend/object/context.
        def keyed(rows):
            return {'/'.join(str(r[k]) for k in ('backend_type', 'object', 'context')): r for r in rows}
        return delta(keyed(before if isinstance(before, list) else []), keyed(after))
    if after is None or before is None:
        return {'status': 'unsupported', 'qualification_blocked': True}
    return after - before


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new directory; never overwritten')
    parser.add_argument('--rows', type=int, default=1000)
    parser.add_argument('--command', nargs=argparse.REMAINDER, help='trusted adapter executable and args; runtime DSNs in env; default pgmeasure')
    parser.add_argument('--fail-after-up', action='store_true', help='verify error cleanup')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    project = 'binance-pg-measure-' + uuid.uuid4().hex[:12]
    report = {'format_version': 1, 'project': project, 'status': 'running',
              'rows': args.rows, 'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'source_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
              'commands': [], 'phases': {}}
    env = os.environ.copy()
    env['GOTOOLCHAIN'] = 'local'

    def run(command, **kwargs):
        # Never persist environment, container inspect, or credentials.
        report['commands'].append(command)
        result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, **kwargs)
        if result.returncode:
            # Diagnostics may include SQL values; persist only a safe error class.
            if command[:2] == ['docker', 'compose'] and command[-1] in ('120', '--remove-orphans'):
                print(result.stderr, file=sys.stderr)
            raise subprocess.CalledProcessError(result.returncode, command)
        return result.stdout

    with tempfile.TemporaryDirectory(prefix='pg-measure-') as tmp:
        tmp = Path(tmp)
        secret_file = tmp / 'secrets.env'
        secret_file.touch(mode=0o600)
        passwords = {owner: secrets.token_hex(24) for owner in OWNERS}
        secret_file.write_text('POSTGRES_PASSWORD=' + secrets.token_hex(24) + '\n' + ''.join(
            f'{owner.upper()}_RUNTIME_PASSWORD={passwords[owner]}\n{owner.upper()}_MIGRATION_PASSWORD={secrets.token_hex(24)}\n'
            for owner in OWNERS))
        override = tmp / 'override.yaml'
        override.write_text(f'''services:
  postgres:
    env_file: !override [{json.dumps(str(secret_file))}]
    ports: !override ["127.0.0.1::5432"]
    command: [postgres, -c, fsync=on, -c, synchronous_commit=on, -c, full_page_writes=on, -c, track_io_timing=on, -c, track_wal_io_timing=on]
''')
        report['override'] = override.read_text().replace(str(secret_file), '<ephemeral-secret-file>')
        compose = ['docker', 'compose', '-p', project, '-f', str(ROOT / 'infra/postgres/compose.yaml'), '-f', str(override)]

        def sql(database, query):
            return run(compose + ['exec', '-T', 'postgres', 'psql', '-XAt', '-U', 'postgres', '-d', database, '-v', 'ON_ERROR_STOP=1'], input=query)

        def snapshot(name):
            time.sleep(2)  # Stats lag >= 1s; fresh sessions avoid transaction caching.
            raw = {owner: json.loads(sql(owner, SNAPSHOT.read_text())) for owner in OWNERS}
            with gzip.open(args.output / f'{name}.json.gz', 'wt') as f:
                json.dump(raw, f, ensure_ascii=False)
            return raw

        try:
            run(compose + ['up', '-d', '--wait', '--wait-timeout', '120'])
            if args.fail_after_up:
                raise RuntimeError('injected failure after startup')
            container = run(compose + ['ps', '-q', 'postgres']).strip()
            image_id = run(['docker', 'inspect', '--format', '{{.Image}}', container]).strip()
            report['image'] = json.loads(run(['docker', 'image', 'inspect', '--format', '{{json .}}', image_id]))
            # Image metadata contains no runtime secrets, but retain only identity/platform.
            report['image'] = {k: report['image'][k] for k in ('Id', 'RepoDigests', 'Architecture', 'Os')}
            report['server_version'] = sql('api', 'SELECT version();').strip()
            report['migration_sha256'] = {}
            report['collector_sha256'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__).resolve(), SNAPSHOT, ROOT / 'internal/tools/pgmeasure/main.go', ROOT / 'infra/postgres/compose.yaml', ROOT / 'infra/postgres/init.sh', ROOT / 'scripts/migrate.py')}
            report['versions'] = {'go': run(['go', 'version']).strip(), 'docker': run(['docker', 'version', '--format', '{{.Server.Version}}']).strip()}
            report['resources'] = json.loads(run(['docker', 'inspect', '--format', '{{json .HostConfig}}', container]))
            report['resources'] = {k: report['resources'][k] for k in ('NanoCpus','Memory','ShmSize')}
            for owner in OWNERS:
                run([sys.executable, str(ROOT / 'scripts/migrate.py'), '--owner', owner, '--project', project])
                report['migration_sha256'][owner] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in sorted((ROOT / 'migrations' / owner).glob('*.sql'))}
                report.setdefault('migration_history', {})[owner] = json.loads(sql(owner,
                    "SELECT json_agg(h) FROM (SELECT version,name,checksum FROM migration.history ORDER BY version) h;"))
                schema = run(compose + ['exec', '-T', 'postgres', 'pg_dump', '-U', 'postgres', '-d', owner, '--schema-only'])
                with gzip.open(args.output / f'{owner}-schema.sql.gz', 'wt') as f:
                    f.write(schema)
            port = run(compose + ['port', 'postgres', '5432']).strip().rsplit(':', 1)[1]
            for owner in OWNERS:
                key = 'DATABASE_URL' if owner == 'api' else owner.upper() + '_DATABASE_URL'
                env[key] = f'postgres://{owner}_runtime:{quote(passwords[owner])}@127.0.0.1:{port}/{owner}?sslmode=disable&connect_timeout=5'
            binary = tmp / 'adapter'
            run(['go', 'build', '-o', str(binary), './internal/tools/pgmeasure'])
            report['initial_api_accounts'] = int(sql('api', 'SELECT count(*) FROM app.users;').strip())
            snapshot('warmup')  # First catalogue reads may set hint bits/generate WAL.
            for phase in ('empty', 'adapter'):
                sql('api', 'CHECKPOINT;')
                before = snapshot(phase + '-before')
                started = time.monotonic()
                operation = None
                if phase == 'adapter':
                    operation = run(args.command or [str(binary), '--rows', str(args.rows)]).strip()
                else:
                    time.sleep(0.1)
                elapsed = time.monotonic() - started
                after = snapshot(phase + '-after')
                sql('api', 'CHECKPOINT;')
                checkpoint = snapshot(phase + '-checkpoint')
                report['phases'][phase] = {'operation': operation, 'operation_seconds': elapsed,
                    'operation_delta': delta(summarize(before), summarize(after)),
                    'checkpoint_delta': delta(summarize(after), summarize(checkpoint)),
                    'inclusive_delta': delta(summarize(before), summarize(checkpoint))}
                # Reset stamps must match across the whole measurement window.
                for owner in OWNERS:
                    for field in ('wal', 'checkpointer', 'bgwriter', 'database_stats'):
                        if len({s[owner][field]['stats_reset'] for s in (before, after, checkpoint)}) != 1:
                            raise RuntimeError('statistics reset during measurement')
                for row in before['api']['io']:
                    key = (row['backend_type'], row['object'], row['context'])
                    for snap in (after, checkpoint):
                        matches = [r for r in snap['api']['io'] if (r['backend_type'], r['object'], r['context']) == key]
                        if len(matches) != 1 or matches[0]['stats_reset'] != row['stats_reset']:
                            raise RuntimeError('I/O statistics reset during measurement')
                for owner, snap in before.items():
                    if set(snap['tablespaces']) != {'pg_default', 'pg_global'}:
                        raise RuntimeError('custom tablespaces need additional temp-file collector')
                    if any(snap['settings'][k]['value'] != 'on' for k in ('fsync','synchronous_commit','full_page_writes','track_io_timing','track_wal_io_timing')):
                        raise RuntimeError('required durability/timing unavailable')
            report['final_api_accounts'] = int(sql('api', 'SELECT count(*) FROM app.users;').strip())
            if not args.command and (report['initial_api_accounts'] != 0 or report['final_api_accounts'] != args.rows):
                raise RuntimeError('adapter account count mismatch')
            report['unsupported'] = {
                'temp_peak_live_bytes': 'Snapshots cannot capture deleted temporary files; cumulative temp_bytes and temp I/O retained; peak qualification blocked.',
                'device_physical_bytes_and_fsync_latency': 'PostgreSQL counters describe OS calls, not device writes/cache flushes; device qualification blocked.'}
            report['metric_support'] = {'relation_forks_bytes': 'supported', 'wal_insert_bytes': 'supported', 'wal_segment_file_bytes': 'supported', 'temp_bytes_cumulative': 'supported', 'pg_stat_io': 'supported; null cells unsupported for backend/object/context', 'fsync_count_time': 'pg_stat_io fsyncs/fsync_time; null cells unsupported; interpret with wal_sync_method'}
            report['status'] = 'collected-not-qualified'
        except BaseException as error:
            report['status'] = 'failed'
            report['error'] = str(error) if isinstance(error, RuntimeError) else type(error).__name__
            raise
        finally:
            try:
                run(compose + ['down', '--volumes', '--remove-orphans'])
                remaining = {}
                for resource in ('container', 'volume', 'network'):
                    remaining[resource] = run(['docker', resource, 'ls', '-q', '--filter', f'label=com.docker.compose.project={project}']).split()
                report['cleanup'] = remaining
                if any(remaining.values()):
                    raise RuntimeError('disposable resources remain')
            except BaseException:
                report['status'] = 'failed'
                report['cleanup_error'] = 'cleanup failed; inspect resources with report project label'
                raise
            finally:
                # Temporary command paths are useful for provenance, secrets never are.
                save(args.output / 'report.json', report)
    print(f'Collected {args.output}; disposable resources removed')


if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt):
        sys.exit('PostgreSQL measurement failed; see safe report.json (no subprocess output logged)')
