"""Verify OPS-037 preflight observations, not owner completeness or lifetime."""
import gzip
import hashlib
import json
from pathlib import Path
import runpy
import zlib

ROOT = Path(__file__).resolve().parents[3]
DIRECTORY = Path(__file__).resolve().parent / 'preflight'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check(directory=DIRECTORY):
    report = json.loads((directory / 'report.json').read_text())
    for name, expected in report['sha256'].items():
        require(hashlib.sha256((directory / name).read_bytes()).hexdigest() == expected,
                'hash mismatch: ' + name)
    require(all(not value for value in report['cleanup'].values()), 'resources remain')
    require(report['captures']['references']['returncode'] == 0, 'FS control failed')
    require(report['exec_baseline_returncode'] == 0, 'nonleader exec baseline failed')
    require(report['captures']['exec']['status'] == 'timeout', 'expected timeout not observed')
    require(report['qualification_blocked'] and
            all(report[key] is None for key in ('temp_peak_live_bytes', 'lower_bytes', 'upper_bytes')),
            'false qualification')

    helper = [json.loads(line) for line in (directory / 'references.stdout').read_text().splitlines()]
    by_tag = {row['tag']: row for row in helper}
    initial = by_tag['before-rename']
    identity = initial['dev'], initial['ino']
    for tag in ('outside-root-open', 'outside-root-grown', 'outside-root-unlinked', 'before-mapping-only'):
        row = by_tag[tag]
        require((row['dev'], row['ino']) == identity, 'independent inode changed')
        require(row['mode'] == initial['mode'] and row['fd'] == initial['fd'], 'independent FD changed')
    require(initial['size'] == 4096 and by_tag['outside-root-grown']['size'] == 8192,
            'growth not observed independently')
    require(by_tag['outside-root-grown']['nlink'] == 1 and
            by_tag['outside-root-unlinked']['nlink'] == 0 and
            by_tag['before-mapping-only']['nlink'] == 0, 'unlink not observed independently')
    require(by_tag['mapping-only']['value'] == 42 and helper[-1]['tag'] == 'complete',
            'mapping-only control did not complete')

    verifier = runpy.run_path(str(ROOT / 'infra/postgres/measure/ancillary/verify.py'))
    rows = verifier['load'](directory / 'references.jsonl.gz')
    replay = verifier['replay'](rows)
    snapshots = [row for row in rows if row['kind'] == 'snapshot']
    files = [f for row in snapshots for f in row['files'] or []
             if (f['dev'], f['ino']) == identity]
    require(any(f['size'] == 4096 for f in files), 'pre-rename inode absent')
    require(not any(f['size'] == 8192 for f in files), 'original path filtering no longer reproduced')
    require(all('maps' not in row and 'census' not in row for row in snapshots),
            'this diagnostic expects original inventory schema')
    # Bind the mapping-only observation to successful mmap/close/munmap syscall order.
    pending, completed = {}, []
    for row in rows:
        if row['kind'] == 'enter':
            pending[row['pid']] = row
        elif row['kind'] == 'leave':
            entry = pending.pop(row['pid'])
            if row['ret'] >= 0:
                completed.append((entry, row))
    mm_entry, mm_return = next((e, r) for e, r in completed
                               if e['nr'] == 222 and e['pid'] == initial['pid']
                               and e['args'][4] == initial['fd'] and e['args'][1] == 8192)
    close = next(r for e, r in completed if e['nr'] == 57 and e['pid'] == initial['pid']
                 and e['args'][0] == initial['fd'] and e['seq'] > mm_return['seq'])
    unmap = next(e for e, r in completed if e['nr'] == 215 and e['pid'] == initial['pid']
                 and e['args'][0] == mm_return['ret'] and e['seq'] > close['seq'])
    interval = [row for row in snapshots if close['seq'] < row['seq'] < unmap['seq']]
    require(interval and not any((f['dev'], f['ino']) == identity
                                for row in interval for f in row['files'] or []),
            'mapping-only inventory loss not reproduced')

    exec_row = json.loads((directory / 'exec.stdout').read_text())
    baseline = json.loads((directory / 'exec-baseline.stdout').read_text())
    require(exec_row['tag'] == baseline['tag'] == 'nonleader-exec' and
            exec_row['tid'] != exec_row['pid'] and baseline['tid'] != baseline['pid'],
            'exec control ran on leader')
    process_bytes = gzip.decompress((directory / 'exec-processes.txt.gz').read_bytes())
    require(hashlib.sha256(process_bytes).hexdigest() ==
            report['uncompressed_sha256']['exec-processes.txt'], 'procfs bytes changed')
    process_blocks = process_bytes.decode().split('/proc/')
    require(any('Name:\ttrue\n' in block and 'State:\tt (tracing stop)\n' in block
                and f"Pid:\t{exec_row['pid']}\n" in block for block in process_blocks),
            'exec transition not witnessed independently')
    stream = zlib.decompressobj(31)
    partial = stream.decompress((directory / 'exec.partial.jsonl.gz').read_bytes())
    partial_rows = [json.loads(line) for line in partial.splitlines()]
    require(not stream.eof and partial_rows[0]['kind'] == 'header' and
            all(row['kind'] != 'footer' for row in partial_rows), 'unexpected complete exec raw')
    try:
        verifier['load'](directory / 'exec.partial.jsonl.gz')
    except (EOFError, gzip.BadGzipFile):
        pass
    else:
        raise ValueError('incomplete gzip accepted as a complete artifact')
    return {'reference_snapshots': len(snapshots), 'mapping_only_snapshots': len(interval),
            'inode': list(identity), 'independent_size_after_rename': 8192,
            'snapshot_size_after_rename': None, 'ancillary_replay': replay,
            'exec_baseline_returncode': 0, 'exec_capture': 'timeout',
            'exec_partial_rows': len(partial_rows),
            'timeout_root_cause': 'not established by partial raw',
            'temp_peak_live_bytes': None, 'lower_bytes': None, 'upper_bytes': None,
            'qualification_blocked': True}


if __name__ == '__main__':
    print(json.dumps(check(), indent=2))
