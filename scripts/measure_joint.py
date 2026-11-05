#!/usr/bin/env python3
"""Coordinate existing disposable collectors around a trusted JSON-lines adapter."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import selectors
import re
import signal
import subprocess
import threading
import time

import measure_kafka as kafka
import measure_postgres as postgres

ROOT = postgres.ROOT


def hashes(paths):
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


def validate_manifest(m):
    assert m['format_version'] == 1
    assert isinstance(m['source_id'], str) and m['source_id']
    assert m['contract_versions'] and m['sources']
    assert all(type(v) is int and v > 0 for v in m['contract_versions'].values())
    for path in m['sources']:
        p = (ROOT / path).resolve()
        assert p.is_relative_to(ROOT) and p.is_file()
    phases = m['phases']
    ids = [p['phase_id'] for p in phases]
    assert ids and len(ids) == len(set(ids))
    for p in phases:
        assert re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', p['phase_id'])
        assert not p['phase_id'].endswith(('-before', '-after', '-checkpoint'))
        assert p['phase_id'] not in ('initial', 'baseline', 'warmup', 'setup', 'verification')
        assert p['unit'] and type(p['size']) is int and p['size'] >= 0
        assert type(p['max_batch']) is int and p['max_batch'] >= 1
        for link in p['links']:
            assert set(link) == {'event_id', 'trigger_id', 'attempt_id'}
            assert all(v is None or isinstance(v, str) for v in link.values())


class Session:
    def __init__(self, manifest, command, timeout, report):
        self.phases = [p['phase_id'] for p in manifest['phases']]
        self.command, self.timeout, self.report = command, timeout, report
        self.env = dict(os.environ, GOTOOLCHAIN='local')
        self.lock = threading.Lock()
        self.driver = None
        self.close_lock = threading.Lock()
        self.barrier = threading.Barrier(2)
        self.result = None

    def request(self, phase):
        started = time.time_ns()
        self.driver.stdin.write((json.dumps({'phase_id': phase}) + '\n').encode())
        self.driver.stdin.flush()
        deadline = time.monotonic() + self.timeout
        line = b''
        with selectors.DefaultSelector() as selector:
            selector.register(self.driver.stdout, selectors.EVENT_READ)
            while not line.endswith(b'\n'):
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise TimeoutError('adapter acknowledgement timeout')
                chunk = os.read(self.driver.stdout.fileno(), 65537 - len(line))
                if not chunk:
                    raise RuntimeError('adapter exited before acknowledgement')
                line += chunk
                if len(line) > 65536 or line.count(b'\n') > 1 or (b'\n' in line and not line.endswith(b'\n')):
                    raise RuntimeError('invalid adapter acknowledgement')
        ack = json.loads(line)
        if ack.get('phase_id') != phase or ack.get('status') != 'ok':
            raise RuntimeError('adapter phase failed')
        # Only explicit protocol fields are retained, never adapter diagnostics.
        self.result = {'phase_id': phase, 'status': 'ok', 'started_ns': started,
                       'finished_ns': time.time_ns()}
        self.report['acknowledgements'][phase] = self.result

    def setup(self, owner, env):
        with self.lock:
            keys = ['DATABASE_URL'] + [o.upper() + '_DATABASE_URL' for o in postgres.OWNERS if o != 'api'] if owner == 'postgres' else ['KAFKA_BROKERS', 'KAFKA_PREFIX']
            self.env.update({k: env[k] for k in keys})
        self.sync()
        if owner == 'postgres':
            self.driver = subprocess.Popen(self.command, cwd=ROOT, env=self.env,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                start_new_session=True)
            self.request('setup')
        self.sync()

    def sync(self):
        self.barrier.wait(timeout=600)

    def operation(self, phase):
        # Barrier action runs once after both before-snapshot boundaries.
        # Separate request barrier; both participants receive the same ack.
        self.op_barrier.wait(timeout=600)
        return self.result

    def verify(self):
        self.verify_barrier.wait(timeout=600)
        return self.result

    def close(self):
        with self.close_lock:
            if self.driver:
                if self.driver.poll() is None:
                    try:
                        os.killpg(self.driver.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    except PermissionError:
                        # macOS can reject killpg after the group has exited.
                        if self.driver.poll() is None:
                            raise
                try:
                    self.driver.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(self.driver.pid, signal.SIGKILL)
                    self.driver.wait(timeout=5)
                self.driver.stdin.close()
                self.driver.stdout.close()
                self.driver = None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--manifest', required=True, type=Path)
    p.add_argument('--timeout', default=60, type=float)
    p.add_argument('--observe-kafka-fsync', action='store_true')
    p.add_argument('--command', nargs=argparse.REMAINDER, required=True)
    args = p.parse_args()
    manifest = json.loads(args.manifest.read_text())
    validate_manifest(manifest)
    assert args.command and args.timeout > 0
    args.output.mkdir(parents=True, exist_ok=False)
    postgres.save(args.output / 'manifest.json', manifest)
    report = {'format_version': 1, 'source_id': manifest['source_id'], 'status': 'running',
              'qualified': False, 'started_ns': time.time_ns(), 'acknowledgements': {},
              'manifest_sha256': hashlib.sha256((args.output / 'manifest.json').read_bytes()).hexdigest(),
              'adapter_command': args.command,
              'source_sha256': hashes(set([ROOT / s for s in manifest['sources']] + list((ROOT / 'internal').rglob('*.go')) + [ROOT / 'go.mod', ROOT / 'go.sum'])),
              'schema_sha256': hashes(list((ROOT / 'migrations').glob('*/*.sql'))),
              'contract_sha256': hashes(list((ROOT / 'proto').rglob('*.proto')) + list((ROOT / 'internal/contracts').rglob('*.go'))),
              'collector_reports': {'postgres': 'postgres/report.json', 'kafka': 'kafka/report.json'},
              'qualification_blockers': ([] if args.observe_kafka_fsync else ['Kafka fsync unsupported']) + ['PostgreSQL temp peak/device IO unsupported', 'Control run is not cost qualification']}
    session = Session(manifest, args.command, args.timeout, report)
    phase_iter = iter(session.phases)
    session.op_barrier = threading.Barrier(2, action=lambda: session.request(next(phase_iter)))
    session.verify_barrier = threading.Barrier(2, action=lambda: session.request('verification'))

    def collect(module, owner):
        try:
            collector_args = ['--output', str(args.output / owner)]
            if owner == 'kafka' and args.observe_kafka_fsync:
                collector_args += ['--observe-fsync']
            module.main(collector_args, session=session)
        except BaseException:
            try:
                session.close()
            finally:
                session.barrier.abort()
                session.op_barrier.abort()
                session.verify_barrier.abort()
            raise

    def interrupted(signum, frame):
        session.barrier.abort()
        session.op_barrier.abort()
        session.verify_barrier.abort()
        session.close()
        raise InterruptedError('runner interrupted')

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(collect, module, owner) for module, owner in ((postgres, 'postgres'), (kafka, 'kafka'))]
            for f in futures:
                f.result()
        report['status'] = 'collected-not-qualified'
    except BaseException:
        report['status'] = 'failed'
        raise
    finally:
        try:
            session.close()
        finally:
            report['finished_ns'] = time.time_ns()
            postgres.save(args.output / 'report.json', report)


if __name__ == '__main__':
    main()
