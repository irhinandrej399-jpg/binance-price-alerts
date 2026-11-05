"""Linux ptrace observer; syscall latency is not device flush latency."""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import time


def ns(value):
    return int(Decimal(value) * 1_000_000_000)


def aggregate(raw, windows):
    """Strictly parse strace -f -ttt -T output, including interleaved calls."""
    pending, calls = {}, []
    for line in raw.splitlines():
        m = re.fullmatch(r'(\d+)\s+(\d+\.\d+)\s+(.*)', line)
        if not m:
            raise ValueError('unrecognized trace line: ' + line)
        tid, stamp, body = int(m[1]), ns(m[2]), m[3]
        if body.startswith(('--- ', '+++ ')):
            continue
        if '<unfinished ...>' in body:
            if tid in pending or not re.match(r'(fsync|fdatasync)\(', body):
                raise ValueError('invalid unfinished call')
            pending[tid] = (stamp, body.replace('<unfinished ...>', ''))
            continue
        if body.startswith('<... '):
            resumed = re.fullmatch(r'<\.\.\. (fsync|fdatasync) resumed>(.*)', body)
            if not resumed or tid not in pending:
                raise ValueError('unpaired resumed call')
            stamp, first = pending.pop(tid)
            if not first.startswith(resumed[1] + '('):
                raise ValueError('resumed syscall mismatch')
            body = first + resumed[2]
        call = re.fullmatch(r'(fsync|fdatasync)\(.*\)\s+= (-?\d+).* <(\d+\.\d+)>', body)
        if not call:
            raise ValueError('incomplete/unknown syscall: ' + body)
        duration = ns(call[3])
        calls.append({'tid': tid, 'operation': call[1], 'started_ns': stamp,
                      'finished_ns': stamp + duration, 'duration_ns': duration,
                      'return_value': int(call[2])})
    if pending:
        raise ValueError('unfinished syscalls at detach')
    result = {}
    for name, w in windows.items():
        start, end = w['started_ns'], w['finished_ns']
        if not start < end:
            raise ValueError('invalid window')
        crossing = [c for c in calls if c['started_ns'] < end and c['finished_ns'] > start
                    and not (start <= c['started_ns'] and c['finished_ns'] <= end)]
        if crossing:
            raise ValueError('syscall crosses window boundary: ' + name)
        selected = [c for c in calls if start <= c['started_ns'] and c['finished_ns'] <= end]
        known = {int(t) for t in w['broker_tasks_before']} | {int(t) for t in w['broker_tasks_after']}
        if any(c['tid'] not in known for c in selected):
            raise ValueError('syscall from task without broker thread membership evidence')
        result[name] = dict(w, count=len(selected),
            time_ms=sum(c['duration_ns'] for c in selected) / 1_000_000,
            operations={op: {'count': sum(c['operation'] == op for c in selected),
                'duration_ns': sum(c['duration_ns'] for c in selected if c['operation'] == op)}
                for op in ('fsync', 'fdatasync')},
            failed_count=sum(c['return_value'] < 0 for c in selected))
    return result


class Observer:
    def __init__(self, run, cid, project, output, fault=None):
        self.run, self.cid, self.output = run, cid, output
        self.name = project + '-fsync'
        self.fault, self.created = fault, False
        self.windows = {}
        self.report = {'status': 'unknown', 'count': None, 'time_ms': None,
            'qualification_blocked': True, 'lost_events': None,
            'method': 'strace -f -ttt -T; ptrace synchronous syscall entry/exit',
            'time_semantics': 'sum of elapsed syscall durations in ms, threads may overlap; includes tracing/scheduling overhead; not device flush',
            'raw': 'fsync.trace', 'windows': self.windows}
        root = Path(__file__).resolve().parents[1]
        self.report['source_sha256'] = {p: hashlib.sha256((root / p).read_bytes()).hexdigest()
            for p in ('scripts/kafka_fsync.py', 'scripts/measure_kafka.py',
                      'infra/kafka/measure/observer.Dockerfile')}

    def exec(self, script):
        return self.run(['docker', 'exec', self.name, 'sh', '-c', script])

    def identity(self):
        # Namespace PID 1 must be the Kafka JVM, never a host PID guess.
        return self.run(['docker', 'exec', self.cid, 'sh', '-c',
            'cat /proc/1/stat; readlink /proc/1/exe; tr "\\000" " " </proc/1/cmdline'])

    def start(self):
        identity = self.identity()
        if 'kafka.Kafka' not in identity or '/java' not in identity:
            raise RuntimeError('broker is not namespace PID 1 JVM')
        self.report['broker_identity_before'] = identity
        self.report['kernel'] = self.run(['docker', 'exec', self.cid, 'uname', '-a']).strip()
        self.report['observer_image'] = json.loads(self.run(['docker', 'image', 'inspect',
            '--format', '{{json .Id}}', 'binance-fsync-observer:ops029']))
        args = ['docker', 'run', '-d', '--name', self.name,
            '--label', 'com.docker.compose.project=' + self.name,
            '--pid=container:' + self.cid, '--network', 'none', '--cap-drop', 'ALL']
        if self.fault != 'unavailable':
            args += ['--cap-add', 'SYS_PTRACE', '--cap-add', 'KILL', '--security-opt', 'seccomp=unconfined']
        args += ['--entrypoint', 'sh', 'binance-fsync-observer:ops029', '-c',
            'mkdir /trace; strace -V >/trace/version; '
            'strace -f -ttt -T -e trace=fsync,fdatasync -o /trace/raw -p 1 '
            '2>/trace/diagnostics & tracer=$!; echo "$tracer" >/trace/pid; '
            'wait "$tracer"; echo "$?" >/trace/exit; '
            'while :; do sleep 1; done']
        self.created = True
        self.run(args)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            state = self.exec('if test -f /trace/exit; then cat /trace/diagnostics; exit 1; fi; '
                'for t in /proc/1/task/*; do awk \'/^Pid:|^TracerPid:/ {print $2}\' "$t/status"; done')
            pairs = list(zip(state.splitlines()[::2], state.splitlines()[1::2]))
            if pairs and all(int(tracer) > 0 for _, tracer in pairs):
                self.report['initial_traced_tasks'] = pairs
                self.report['version'] = self.exec('cat /trace/version')
                # Both syscall filters must work. These separate-process calls must
                # not enter the broker trace. The control is outside phase windows.
                self.exec('strace -f -ttt -T -e trace=fsync,fdatasync -o /trace/control '
                    'sh -c \'echo probe >/trace/probe; sync /trace/probe; sync -d /trace/probe\'')
                control = self.exec('cat /trace/control')
                (self.output / 'fsync-control.trace').write_text(control)
                if 'fsync(' not in control or 'fdatasync(' not in control:
                    raise RuntimeError('both syscall controls required')
                return
            time.sleep(.1)
        raise RuntimeError('observer did not attach all broker threads')

    def clock(self):
        self.exec('test ! -f /trace/exit')
        return ns(self.exec('date +%s.%N').strip())

    def begin(self, name):
        tasks = self.tasks()
        self.windows[name] = {'broker_tasks_before': tasks, 'started_ns': self.clock()}

    def end(self, name):
        self.windows[name]['finished_ns'] = self.clock()
        self.windows[name]['broker_tasks_after'] = self.tasks()

    def tasks(self):
        state = self.exec('for t in /proc/1/task/*; do '
            'awk \'/^Pid:|^TracerPid:/ {print $2}\' "$t/status"; done').splitlines()
        pairs = list(zip(state[::2], state[1::2]))
        tracer = self.exec('cat /trace/pid').strip()
        if not pairs or any(t != tracer for _, t in pairs):
            raise RuntimeError('broker thread not traced')
        return [tid for tid, _ in pairs]

    def finish(self):
        try:
            if not self.created:
                return
            (self.output / 'fsync-windows.json').write_text(json.dumps(self.windows, indent=2) + '\n')
            self.exec('if test ! -f /trace/exit; then kill -INT "$(cat /trace/pid)"; fi; '
                'for i in $(seq 1 100); do test -f /trace/exit && exit 0; sleep .1; done; exit 1')
            raw = self.exec('cat /trace/raw')
            (self.output / 'fsync.trace').write_text(raw)
            diagnostics = self.exec('cat /trace/diagnostics')
            (self.output / 'fsync-diagnostics.txt').write_text(diagnostics)
            self.report['tracer_exit'] = self.exec('cat /trace/exit').strip()
            self.report['broker_identity_after'] = self.identity()
            # stat fields after the starttime vary normally; compare PID starttime/exe/cmdline.
            before = self.report['broker_identity_before'].splitlines()
            after = self.report['broker_identity_after'].splitlines()
            if before[0].split()[21] != after[0].split()[21] or before[1:] != after[1:]:
                raise RuntimeError('broker identity changed')
            if self.report['tracer_exit'] not in ('0', '130') or not all(re.fullmatch(
                    r'strace: Process \d+ (attached(?: with \d+ threads)?|detached)', line)
                    for line in diagnostics.splitlines()):
                raise RuntimeError('observer unavailable or trace interrupted')
            if not self.windows or not self.report.get('initial_traced_tasks'):
                raise RuntimeError('no complete observation window')
            control = (self.output / 'fsync-control.trace').read_text()
            control_tids = {line.split()[0] for line in control.splitlines()}
            if any(line.split()[0] in control_tids for line in raw.splitlines()):
                raise RuntimeError('non-broker control leaked into broker trace')
            self.report['windows'] = aggregate(raw, self.windows)
            self.report['raw_sha256'] = {name: hashlib.sha256((self.output / name).read_bytes()).hexdigest()
                for name in ('fsync.trace', 'fsync-windows.json', 'fsync-control.trace', 'fsync-diagnostics.txt')}
            self.report.update(status='measured', qualification_blocked=False, lost_events=0,
                count=sum(w['count'] for w in self.report['windows'].values()),
                time_ms=sum(w['time_ms'] for w in self.report['windows'].values()))
        except BaseException as exc:
            self.report['reason'] = str(exc)
            raise
        finally:
            if self.created:
                self.run(['docker', 'rm', '-f', self.name])
                self.created = False
                remaining = self.run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + self.name]).strip()
                self.report['cleanup_verified'] = not remaining
                if remaining:
                    raise RuntimeError('observer container remains')


def verify_observer(directory, report):
    o = report['fsync']
    assert o['status'] == 'measured' and o['qualification_blocked'] is False
    assert o['lost_events'] == 0 and o['cleanup_verified'] is True
    assert o['tracer_exit'] in ('0', '130')
    assert o['kernel'] and o['observer_image'] and o['version'] and o['source_sha256']
    assert o['initial_traced_tasks'] and all(int(t[1]) > 0 for t in o['initial_traced_tasks'])
    for name in ('fsync.trace', 'fsync-windows.json', 'fsync-control.trace', 'fsync-diagnostics.txt'):
        assert hashlib.sha256((directory / name).read_bytes()).hexdigest() == o['raw_sha256'][name], name
    diagnostics = (directory / 'fsync-diagnostics.txt').read_text()
    assert diagnostics and all(re.fullmatch(r'strace: Process \d+ (attached(?: with \d+ threads)?|detached)', l)
        for l in diagnostics.splitlines())
    before, after = [o[k].splitlines() for k in ('broker_identity_before', 'broker_identity_after')]
    assert before[0].split()[21] == after[0].split()[21] and before[1:] == after[1:]
    assert 'kafka.Kafka' in before[-1] and '/java' in before[1]
    raw = (directory / 'fsync.trace').read_text()
    windows = json.loads((directory / 'fsync-windows.json').read_text())
    assert set(windows) == {k for k, v in report['phases'].items() if 'ack' in v}
    recomputed = aggregate(raw, windows)
    assert recomputed == o['windows'], 'raw aggregate mismatch'
    assert sum(w['count'] for w in recomputed.values()) == o['count']
    assert sum(w['time_ms'] for w in recomputed.values()) == o['time_ms']
    control = (directory / 'fsync-control.trace').read_text()
    assert 'fsync(' in control and 'fdatasync(' in control
    control_tids = {line.split()[0] for line in control.splitlines()}
    assert not any(line.split()[0] in control_tids for line in raw.splitlines())
    # Parse both controls as actual completed calls, not substring-only evidence.
    control_window = {'control': {'started_ns': 1, 'finished_ns': 10**20,
        'broker_tasks_before': list(control_tids), 'broker_tasks_after': list(control_tids)}}
    c = aggregate(control, control_window)['control']
    assert c['failed_count'] == 0
    assert all(c['operations'][op]['count'] > 0 for op in ('fsync', 'fdatasync'))
    last = 0
    for name, w in recomputed.items():
        assert last < w['started_ns'] < w['finished_ns']
        last = w['finished_ns']
        import gzip
        with gzip.open(directory / (name + '.json.gz'), 'rt') as f:
            snap = json.load(f)
        assert w['started_ns'] < snap['started_ns'] <= snap['finished_ns'] < w['finished_ns']
