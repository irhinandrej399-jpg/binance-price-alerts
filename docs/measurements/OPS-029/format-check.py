#!/usr/bin/env python3
"""Damage actual collected evidence; every copy must fail verification."""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts'))
from verify_kafka_measurement import verify

source = Path(__file__).parent / 'run-b'
results = {}
for case in ('count', 'time', 'loss', 'unknown', 'cleanup', 'missing_end',
             'garbage', 'unfinished', 'crossing', 'foreign', 'delta'):
    with tempfile.TemporaryDirectory(prefix='ops029-format-') as tmp:
        d = Path(tmp) / 'copy'; shutil.copytree(source, d)
        r = json.loads((d / 'report.json').read_text()); o = r['fsync']
        if case == 'count': o['count'] += 1
        if case == 'time': o['time_ms'] += 1
        if case == 'loss': o['lost_events'] = 1
        if case == 'unknown': o['status'] = 'unknown'
        if case == 'cleanup': o['cleanup_verified'] = False
        if case == 'missing_end':
            name = 'fsync-windows.json'
            w = json.loads((d / name).read_text()); del w['empty']['finished_ns']
            (d / name).write_text(json.dumps(w))
            o['raw_sha256'][name] = hashlib.sha256((d / name).read_bytes()).hexdigest()
        if case in ('garbage', 'unfinished', 'crossing', 'foreign'):
            trace = (d / 'fsync.trace').read_text()
            if case == 'garbage': trace += 'garbage\n'
            if case == 'unfinished': trace += '1 1793877600.000000 fsync(5 <unfinished ...>\n'
            if case == 'foreign': trace += (d / 'fsync-control.trace').read_text()
            if case == 'crossing':
                w = o['windows']['empty']; stamp = w['started_ns'] - 500_000
                tid = w['broker_tasks_before'][0]
                trace += f'{tid} {stamp // 10**9}.{stamp % 10**9:09d} fsync(5) = 0 <0.001000>\n'
            (d / 'fsync.trace').write_text(trace)
            o['raw_sha256']['fsync.trace'] = hashlib.sha256((d / 'fsync.trace').read_bytes()).hexdigest()
        if case == 'delta':
            row = next(iter(r['phases']['first']['delta_previous'].values()))
            row['apparent_bytes'] += 1
        (d / 'report.json').write_text(json.dumps(r))
        try:
            verify(d)
        except (AssertionError, ValueError, KeyError):
            results[case] = 'rejected'
        else:
            raise AssertionError('damaged evidence accepted: ' + case)
print(json.dumps(results, indent=2))
