#!/usr/bin/env python3
"""Verify saved OPS-019 control experiments against their raw PostgreSQL data."""
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def verify(directory):
    results = []
    projects = set()
    hashes = []
    for name, rows in (('run-1000-a', 1000), ('run-1000-b', 1000), ('run-2000', 2000)):
        folder = directory / name
        report = json.loads((folder / 'report.json').read_text())
        assert report['status'] == 'collected-not-qualified', name
        assert report['cleanup'] == {'container': [], 'volume': [], 'network': []}
        assert report['initial_api_accounts'] == 0 and report['final_api_accounts'] == rows
        assert report['project'] not in projects
        projects.add(report['project'])
        hashes.append(report['collector_sha256'])
        result = {'run': name, 'rows': rows}
        for phase in ('empty', 'adapter'):
            snapshots = [json.load(gzip.open(folder / f'{phase}-{step}.json.gz', 'rt'))
                         for step in ('before', 'after', 'checkpoint')]
            a, _, b = snapshots
            for snapshot in snapshots:
                assert set(snapshot) == {'api', 'market_data', 'matcher', 'notifications'}
                for database, raw in snapshot.items():
                    assert raw['database'] == database
                    assert len({(r['oid'], r['fork']) for r in raw['relations']}) == len(raw['relations'])
                    for setting in ('fsync','synchronous_commit','full_page_writes','track_io_timing','track_wal_io_timing'):
                        assert raw['settings'][setting]['value'] == 'on'
                    for kind in ('wal','checkpointer','bgwriter','database_stats'):
                        assert raw[kind]['stats_reset'] == a[database][kind]['stats_reset']
            delta = report['phases'][phase]['inclusive_delta']
            # Independently reconstruct expected physical relation growth from raw tuples.
            for database in a:
                expected = sum(r['bytes'] for r in b[database]['relations']) - sum(r['bytes'] for r in a[database]['relations'])
                assert sum(delta['databases'][database]['relation_bytes'].values()) == expected
            def lsn(s):
                h, l = s.split('/')
                return int(h,16)*4294967296 + int(l,16)
            assert delta['wal_insert_bytes'] == lsn(b['api']['lsn']) - lsn(a['api']['lsn'])
            assert delta['wal_segment_file_bytes'] == sum(w['size'] for w in b['api']['wal_files']) - sum(w['size'] for w in a['api']['wal_files'])
            io = delta['io']['client backend/wal/normal']
            if phase == 'adapter':
                operation = json.loads(report['phases'][phase]['operation'])
                assert operation['rows'] == rows and operation['role'] == 'api_runtime'
                assert io['fsyncs'] >= rows
            result[phase] = {'wal_insert_bytes': delta['wal_insert_bytes'],
                'api_relation_bytes': sum(delta['databases']['api']['relation_bytes'].values()),
                'api_database_bytes': delta['databases']['api']['database_bytes'],
                'wal_segment_file_bytes': delta['wal_segment_file_bytes'],
                'client_wal_fsyncs': io['fsyncs'], 'client_wal_fsync_ms': io['fsync_time']}
        assert result['adapter']['wal_insert_bytes'] > 100 * result['empty']['wal_insert_bytes']
        results.append(result)
    assert hashes[0] == hashes[1] == hashes[2], 'collector changed between controls'
    for path, digest in hashes[0].items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == digest, path
    failure = json.loads((directory / 'failure' / 'report.json').read_text())
    assert failure['status'] == 'failed' and failure['error'] == 'injected failure after startup'
    assert failure['cleanup'] == {'container': [], 'volume': [], 'network': []}
    assert failure['project'] not in projects
    # Input-size signal must exceed the repeat's observed noise. No timing SLO claimed.
    a,b,c = [r['adapter']['wal_insert_bytes'] for r in results]
    assert c - max(a,b) > abs(a-b)
    assert results[2]['adapter']['api_relation_bytes'] > max(r['adapter']['api_relation_bytes'] for r in results[:2])
    return {'status': 'verified-controls-not-qualified', 'experiments': results,
            'repeat_wal_spread_bytes': abs(a-b), 'failure_cleanup': 'verified'}


if __name__ == '__main__':
    directory = Path(sys.argv[1])
    summary = verify(directory)
    (directory / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))
