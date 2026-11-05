#!/usr/bin/env python3
"""Check paired clean-volume experiments and failed observer controls."""
import argparse
import json
from pathlib import Path
from verify_kafka_measurement import verify


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directories', type=Path, nargs='+')
    p.add_argument('--failed', type=Path, nargs='*', default=[])
    args = p.parse_args()
    projects, prefixes = set(), set()
    for path in args.directories:
        print(path, verify(path))
        r = json.loads((path / 'report.json').read_text())
        assert r['fsync']['status'] == 'measured'
        assert r['project'] not in projects and r['prefix'] not in prefixes
        projects.add(r['project']); prefixes.add(r['prefix'])
    for path in args.failed:
        r = json.loads((path / 'report.json').read_text())
        assert r['status'] == 'failed' and r['error']
        assert 'cleanup_error' not in r, 'observer failure must not mask successful cleanup'
        assert r['cleanup']['verified'] and not any(r['cleanup']['leftovers'].values())
        o = r['fsync']
        assert o['status'] == 'unknown' and o['qualification_blocked'] is True
        assert o['count'] is None and o['time_ms'] is None and o['lost_events'] is None
        assert o['cleanup_verified'] is True and o['reason']
        assert o['tracer_exit'] in ('1', '137')
        if o['tracer_exit'] == '1':
            assert 'Operation not permitted' in (path / 'fsync-diagnostics.txt').read_text()
            assert not o['windows']
        else:
            assert 'finished_ns' not in o['windows']['empty']
        print(path, 'unknown observer and both cleanups verified')


if __name__ == '__main__':
    main()
