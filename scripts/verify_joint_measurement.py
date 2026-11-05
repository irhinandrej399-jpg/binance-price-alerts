#!/usr/bin/env python3
"""Validate joint protocol, required metrics and both original raw collectors."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path

import measure_joint as joint
import measure_postgres as pg
import measure_kafka as kafka


def read(path):
    return json.loads(path.read_text())


def raw(path):
    with gzip.open(path, 'rt') as f:
        return json.load(f)


def numeric(v):
    assert type(v) in (int, float), 'missing/unknown numeric metric'


def verify(path):
    report = read(path / 'report.json')
    manifest = read(path / 'manifest.json')
    joint.validate_manifest(manifest)
    assert report['format_version'] == 1 and report['source_id'] == manifest['source_id']
    assert report['manifest_sha256'] == hashlib.sha256((path / 'manifest.json').read_bytes()).hexdigest()
    assert report['source_sha256'] and report['schema_sha256'] and report['contract_sha256']
    assert report['status'] == 'collected-not-qualified'
    assert report['qualified'] is False and report['qualification_blockers']
    assert report['started_ns'] < report['finished_ns']
    ids = [p['phase_id'] for p in manifest['phases']]
    acks = report['acknowledgements']
    assert set(acks) == set(['setup', *ids, 'verification'])
    last = report['started_ns']
    for phase in ['setup', *ids, 'verification']:
        a = acks[phase]
        assert a['phase_id'] == phase and a['status'] == 'ok'
        assert last <= a['started_ns'] <= a['finished_ns']
        last = a['finished_ns']
    p = read(path / report['collector_reports']['postgres'])
    k = read(path / report['collector_reports']['kafka'])
    assert p['status'] == 'collected-not-qualified' and k['status'] == 'complete'
    assert not any(p['cleanup'].values()) and k['cleanup']['verified']
    assert p['image']['Id'] and k['image_digests']
    assert set(p['migration_history']) == set(pg.OWNERS)
    assert p['unsupported'] and k['fsync']['status'] == 'unsupported'
    assert set(p['phases']) == set(ids)
    pg_previous = pg.summarize(raw(path / 'postgres/baseline.json.gz'))
    previous = kafka.totals(raw(path / 'kafka/baseline.json.gz')['files'])
    baseline = previous
    for phase in ids:
        ack = acks[phase]
        assert p['phases'][phase]['operation'] == k['phases'][phase]['ack'] == ack
        snaps = [raw(path / f'postgres/{phase}-{s}.json.gz') for s in ('before', 'after', 'checkpoint')]
        for snap in snaps:
            assert set(snap) == set(pg.OWNERS)
            for owner, db in snap.items():
                numeric(db['database_bytes'])
                numeric(db['database_stats']['temp_bytes'])
                numeric(db['database_stats']['temp_files'])
                assert db['relations']
                for rel in db['relations']:
                    numeric(rel['bytes'])
                for setting in ('fsync', 'synchronous_commit', 'full_page_writes'):
                    assert db['settings'][setting]['value'] == 'on'
        b, a, c = [pg.summarize(s) for s in snaps]
        assert p['phases'][phase]['background_delta'] == pg.delta(pg_previous, b)
        pg_previous = c
        for key, before, after in [('operation_delta', b, a), ('checkpoint_delta', a, c), ('inclusive_delta', b, c)]:
            assert p['phases'][phase][key] == pg.delta(before, after)
            numeric(p['phases'][phase][key]['wal_insert_bytes'])
        snap = raw(path / f'kafka/{phase}.json.gz')
        assert snap['files'] and snap['started_ns'] >= ack['finished_ns']
        for f in snap['files']:
            numeric(f['size']); numeric(f['blocks'])
        current = kafka.totals(snap['files'])
        entry = k['phases'][phase]
        assert entry['totals'] == current
        before = kafka.totals(raw(path / f'kafka/{phase}-before.json.gz')['files'])
        assert entry['background_delta'] == kafka.difference(previous, before)
        assert entry['delta_previous'] == kafka.difference(before, current)
        assert entry['delta_baseline'] == kafka.difference(baseline, current)
        previous = current
    assert set(k['phases']) == set(['initial', 'baseline', *ids, *(i + '-before' for i in ids)])
    assert p['verification'] == k['verification'] == acks['verification']
    if manifest['source_id'] == 'OPS-023-control-v1':
        assert p['initial_api_accounts'] == 0 and p['final_api_accounts'] == 10
        # Cluster WAL includes background work; compare owner relations instead.
        for phase, growing in (('empty', False), ('write', True)):
            relations = p['phases'][phase]['operation_delta']['databases']['api']['relation_bytes']
            app_growth = sum(v for key, v in relations.items() if key.startswith('app/'))
            assert (app_growth > 0) if growing else (app_growth == 0)
        assert p['phases']['write']['operation_delta']['wal_insert_bytes'] > 0
        assert any(v['apparent_bytes'] > 0 for name, v in k['phases']['write']['delta_previous'].items() if 'subscription-changes' in name and name.endswith('/.log'))
    return {'path': str(path), 'postgres_project': p['project'], 'kafka_project': k['project'], 'prefix': k['prefix']}


def verify_failure(path):
    report = read(path / 'report.json')
    assert report['status'] == 'failed' and report['qualified'] is False
    assert set(report['acknowledgements']) == {'setup', 'empty'}
    p = read(path / report['collector_reports']['postgres'])
    k = read(path / report['collector_reports']['kafka'])
    assert p['status'] == k['status'] == 'failed'
    assert not any(p['cleanup'].values()) and k['cleanup']['verified']
    assert not any(k['cleanup']['leftovers'].values())
    print(str(path), 'expected write failure; both cleanups verified')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths', type=Path, nargs='*')
    parser.add_argument('--failed', type=Path, nargs='*', default=[])
    args = parser.parse_args()
    assert args.paths or args.failed
    for path in args.failed:
        verify_failure(path)
    results = [verify(p) for p in args.paths]
    for key in ('postgres_project', 'kafka_project', 'prefix'):
        assert len({r[key] for r in results}) == len(results), 'repeat must use fresh resources'
    print(json.dumps(results, indent=2))
