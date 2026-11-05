#!/usr/bin/env python3
"""Collect disposable Kafka physical files. No application or load qualification."""
import argparse
import gzip
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def save(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')


def totals(files):
    result = {}
    for f in files:
        # Keep internal topics, metadata and every auxiliary file, not only .log.
        category = str(Path(f['path']).parent) + '/' + Path(f['path']).suffix
        row = result.setdefault(category, {'apparent_bytes': 0, 'allocated_bytes': 0})
        row['apparent_bytes'] += f['size']
        row['allocated_bytes'] += f['blocks'] * 512
    return result


def difference(before, after):
    return {k: {metric: after.get(k, {}).get(metric, 0) - before.get(k, {}).get(metric, 0)
                for metric in ('apparent_bytes', 'allocated_bytes')}
            for k in sorted(before.keys() | after.keys())}


def main(argv=None, session=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--fail-after-up', action='store_true')
    args = parser.parse_args(argv)
    args.output.mkdir(parents=True, exist_ok=False)
    project = 'binance-kafka-measure-' + uuid.uuid4().hex[:12]
    prefix = 'measure-' + uuid.uuid4().hex[:12]
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = dict(os.environ, KAFKA_PORT=str(port), GOTOOLCHAIN='local')
    compose = ['docker', 'compose', '-p', project, '-f', 'infra/kafka/compose.yaml']
    report = {'format_version': 1, 'project': project, 'prefix': prefix, 'port': port,
              'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'source_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
              'status': 'running', 'commands': [], 'phases': {},
              'fsync': {'count': None, 'time_ms': None, 'status': 'unsupported',
                        'qualification_blocked': True,
                        'reason': 'No broker-scoped syscall count/time observer in this profile; sync is not a counter.'},
              'boundary': 'Publish returned nil (EndTxn committed), wait 1s, container sync, live file stat; not an atomic broker snapshot.'}
    driver = None

    def run(cmd, **kwargs):
        report['commands'].append(cmd)
        result = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=True, timeout=240, **kwargs)
        if result.returncode:
            raise RuntimeError(f'command failed ({result.returncode}): {cmd}: {result.stderr[-2000:]}')
        return result.stdout

    def inside(*cmd, **kwargs):
        return run(compose + ['exec', '-T', 'kafka', *cmd], **kwargs)

    def operation(op=None):
        if op:
            report['commands'].append(['driver', op])
            driver.stdin.write(op + '\n')
            driver.stdin.flush()
        with selectors.DefaultSelector() as selector:
            selector.register(driver.stdout, selectors.EVENT_READ)
            if not selector.select(60):
                raise RuntimeError('driver response timeout')
        line = driver.stdout.readline()
        if not line:
            raise RuntimeError('driver exited: ' + driver.stderr.read()[-2000:])
        return json.loads(line)

    def snapshot(name):
        time.sleep(1)
        inside('sync')
        started = time.time_ns()
        raw = inside('bash', '-o', 'pipefail', '-c', "cd /var/lib/kafka/data; find . -type f -print0 | while IFS= read -r -d '' f; do stat -c '%n|%s|%b' \"$f\" || exit 1; done | sort")
        files = []
        for line in raw.splitlines():
            path, size, blocks = line.split('|')
            files.append({'path': path.removeprefix('./'), 'size': int(size), 'blocks': int(blocks)})
        if not files:
            raise RuntimeError('empty physical file inventory')
        snap = {'started_ns': started, 'finished_ns': time.time_ns(), 'files': files}
        with gzip.open(args.output / (name + '.json.gz'), 'wt') as out:
            json.dump(snap, out)
        summary = totals(files)
        report['phases'][name] = {'totals': summary}
        return summary

    def terminate(signum, frame):
        raise InterruptedError(f'signal {signum}')

    if not session:
        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGINT, terminate)
    try:
        with tempfile.TemporaryDirectory(prefix='kafka-measure-') as tmp:
            binary = str(Path(tmp) / 'driver')
            run(['go', 'build', '-o', binary, './internal/tools/kafkameasure'])
            report['compose'] = run(compose + ['config'])
            report['docker'] = run(['docker', 'info', '--format', '{{.ServerVersion}} {{.OSType}} {{.Architecture}} CPUs={{.NCPU}} RAM={{.MemTotal}} Driver={{.Driver}} OS={{.OperatingSystem}}'])
            run(compose + ['up', '-d', '--wait', '--wait-timeout', '180'])
            if args.fail_after_up:
                raise RuntimeError('injected failure after up')
            cid = run(compose + ['ps', '-q', 'kafka']).strip()
            report['image'] = json.loads(run(['docker', 'inspect', '--format', '{{json .Image}}', cid]))
            report['image_digests'] = json.loads(run(['docker', 'image', 'inspect', '--format', '{{json .RepoDigests}}', report['image']]))
            report['version'] = inside('/opt/kafka/bin/kafka-topics.sh', '--version').strip()
            report['filesystem'] = inside('bash', '-c', 'df -T /var/lib/kafka/data; cat /proc/mounts')
            snapshot('initial')
            inside('timeout', '90', 'bash', '-s', '--', prefix, input=(ROOT / 'infra/kafka/init-topics.sh').read_text())
            if session:
                session.setup('kafka', dict(env, KAFKA_BROKERS=f'127.0.0.1:{port}', KAFKA_PREFIX=prefix))
            else:
                report['producer_command'] = [binary, f'127.0.0.1:{port}', prefix]
                driver = subprocess.Popen(report['producer_command'], cwd=ROOT, env=env, text=True,
                                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                report['prepared'] = operation()
            report['topic_configs'] = inside('/opt/kafka/bin/kafka-configs.sh', '--bootstrap-server', 'kafka:9092', '--entity-type', 'topics', '--describe', '--all')
            report['broker_configs'] = inside('/opt/kafka/bin/kafka-configs.sh', '--bootstrap-server', 'kafka:9092', '--entity-type', 'brokers', '--entity-name', '1', '--describe', '--all')
            baseline = snapshot('baseline')
            previous = baseline
            if session:
                session.sync()
            for name, op in ([(p, p) for p in session.phases] if session else [('empty', 'empty'), ('first', 'publish'), ('repeat', 'publish')]):
                report['phases'][name] = {}  # operations persisted separately below
                before = snapshot(name + '-before') if session else previous
                ack = session.operation(op) if session else operation(op)
                current = snapshot(name)
                report['phases'][name].update(ack=ack, delta_previous=difference(before, current), delta_baseline=difference(baseline, current))
                if session:
                    report['phases'][name]['background_delta'] = difference(previous, before)
                previous = current
                if session:
                    session.sync()
            if session:
                report['verification'] = session.verify()
            else:
                report['read_committed'] = operation('read')
                driver.stdin.close()
                if driver.wait(timeout=10):
                    raise RuntimeError('driver failed at shutdown')
            report['status'] = 'complete'
    except BaseException as exc:
        report['status'] = 'failed'
        report['error'] = str(exc)
        raise
    finally:
        if driver and driver.poll() is None:
            driver.terminate()
            try:
                driver.wait(timeout=10)
            except subprocess.TimeoutExpired:
                driver.kill()
                driver.wait(timeout=10)
        try:
            try:
                report['broker_log'] = run(compose + ['logs', '--no-color', 'kafka'])
            except Exception as exc:
                report['broker_log_error'] = str(exc)
            run(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '10'])
            leftovers = {kind: run(['docker', kind, 'ls', *(['-a'] if kind == 'container' else []), '-q', '--filter', 'label=com.docker.compose.project=' + project]).strip()
                         for kind in ('container', 'volume', 'network')}
            report['cleanup'] = {'verified': not any(leftovers.values()), 'leftovers': leftovers}
            if not report['cleanup']['verified']:
                raise RuntimeError('cleanup left resources')
        except BaseException as exc:
            report['status'] = 'failed'
            report['cleanup_error'] = str(exc)
            raise
        finally:
            save(args.output / 'report.json', report)
    print(args.output / 'report.json')


if __name__ == '__main__':
    main()
