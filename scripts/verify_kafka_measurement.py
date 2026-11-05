#!/usr/bin/env python3
"""Verify raw Kafka snapshots and recompute every reported byte delta."""
import argparse
import gzip
import json
from pathlib import Path
from measure_kafka import difference, totals
from kafka_fsync import verify_observer


def verify(directory):
    report = json.loads((directory / 'report.json').read_text())
    assert report['cleanup']['verified'], 'cleanup unverified'
    assert not any(report['cleanup']['leftovers'].values()), 'resources left'
    if report['status'] == 'failed':
        assert report['error'] == 'injected failure after up', report.get('error')
        return 'expected failure; cleanup verified'
    assert report['status'] == 'complete'
    if report['fsync']['status'] == 'measured':
        verify_observer(directory, report)
    else:
        assert report['fsync']['status'] == 'unsupported'
        assert report['fsync']['qualification_blocked'] is True
        assert report['fsync']['count'] is None and report['fsync']['time_ms'] is None
    assert report['version'] and report['image_digests'] and report['broker_configs'] and report['topic_configs']
    assert report['project'].startswith('binance-kafka-measure-')
    assert 'KAFKA_LOG_FLUSH_INTERVAL_MESSAGES: "1"' in report['compose']
    baseline = previous = None
    last_end = 0
    for name in ('initial', 'baseline', 'empty', 'first', 'repeat'):
        with gzip.open(directory / (name + '.json.gz'), 'rt') as inp:
            snap = json.load(inp)
        assert last_end < snap['started_ns'] <= snap['finished_ns']
        last_end = snap['finished_ns']
        files = snap['files']
        assert len({f['path'] for f in files}) == len(files)
        assert all(f['size'] >= 0 and f['blocks'] >= 0 for f in files)
        total = totals(files)
        assert total == report['phases'][name]['totals'], name
        if name == 'initial':
            continue
        paths = [f['path'] for f in files]
        for marker in ('__transaction_state-', '__consumer_offsets-', '__cluster_metadata-', '.index', '.timeindex', '.log'):
            assert any(marker in p for p in paths), (name, marker)
        if name == 'baseline':
            baseline = previous = total
            continue
        phase = report['phases'][name]
        assert difference(previous, total) == phase['delta_previous'], name
        assert difference(baseline, total) == phase['delta_baseline'], name
        assert phase['ack']['committed_records'] == {'empty': 1, 'first': 2, 'repeat': 3}[name]
        if name != 'empty':
            topic = report['prefix'] + '-subscription-changes-0'
            growth = sum(v['apparent_bytes'] for k, v in phase['delta_previous'].items() if topic in k and k.endswith('/.log'))
            assert growth > 0, (name, 'no physical log growth')
        previous = total
    read = report['read_committed']
    assert read['isolation'] == 'read_committed'
    rows = read['records']
    assert len(rows) == 3
    assert rows[0]['value'] == 'prepare'
    assert rows[1]['value'] == rows[2]['value'] == report['prepared']['payload']
    assert json.loads(rows[1]['value'])['event_id'] == 'ops020-probe-1'
    assert rows[1]['offset'] < rows[2]['offset']
    assert all(r['key'] == 'probe' and r['partition'] == 0 for r in rows)
    return 'raw files/deltas/internal topics/read_committed/cleanup verified'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', type=Path, nargs='+')
    for directory in parser.parse_args().directories:
        print(directory, verify(directory))
