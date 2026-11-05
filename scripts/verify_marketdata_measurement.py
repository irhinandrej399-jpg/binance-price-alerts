#!/usr/bin/env python3
"""Validate OPS-022 real storage/producer/relay evidence and original raw deltas."""
import argparse
import hashlib
import json
from pathlib import Path
import verify_joint_measurement as joint


def verify(path):
    resources = joint.verify(path)
    m, e = [joint.read(path / n) for n in ('manifest.json', 'identities.json')]
    assert e['format_version'] == 1
    assert e['source_id'] == m['source_id'] == 'OPS-022-market-data-v1'
    assert e['sql_role'] == 'market_data_runtime'
    assert e['max_batch'] == m['limits']['relay_max_batch'] == 1
    assert m['qualification']['qualified'] is False and m['qualification']['blockers'] and m['qualification']['uncovered']
    assert m['limits']['retained_history_bound'] is m['limits']['total_publish_attempts_bound'] is None
    phases = {p['phase_id']: p['result'] for p in e['phases']}
    assert list(phases) == [p['phase_id'] for p in m['phases']]
    for p in m['phases']:
        assert phases[p['phase_id']]['operations'] == p['size']
    assert phases['begin']['generation'] == 1 and phases['begin_again']['generation'] == 2
    assert phases['stale_generation']['stale'] is True and phases['status_sticky']['gap'] is True
    for n in ('repeat', 'old', 'reject_4097'):
        assert phases[n]['accepted'] is False
    assert phases['reject_4097']['bytes'] == 4097
    assert phases['relay_tail'] == {'operations': 2, 'max_batch': 1}
    assert phases['claim_empty']['empty'] is True
    assert phases['publish_lost']['commit'] == 'unknown-to-client-broker-committed'
    assert phases['publish_lost']['broker_endtxn_code'] == 0
    assert phases['publish_first']['commit'] == phases['publish_retry']['commit'] == 'confirmed'
    claim, reclaim = phases['claim_lost'], phases['reclaim']
    assert claim['event_id'] == reclaim['event_id'] == 'ops022-price-2' and reclaim['token'] > claim['token']
    rows = e['outbox']
    assert len(rows) == 4
    bodies = {}
    for i, (r, phase) in enumerate(zip(rows, ('accept_small', 'accept_4095_gap', 'accept_4096', 'accept_generation2')), 1):
        assert r['event_id'] == phases[phase]['event_id'] == f'ops022-price-{i}'
        assert r['published'] is True and r['position'] == i and r['pair'] == 'BTCUSDT'
        assert r['claim_token'] == (2 if i == 2 else 1)
        b = bytes.fromhex(r['body_hex'])
        assert len(b) == phases[phase]['bytes']
        if i > 1:
            assert len(b) == (4095 if i == 2 else 4096)
        v = json.loads(b)
        assert v['event_id'] == r['event_id'] and v['ordering_key'] == 'BTCUSDT'
        q = v['payload']
        assert q['source_id'] == ['10','12','13','14'][i-1]
        assert q['revision']['generation'] == (2 if i == 4 else 1)
        assert q['revision']['sequence'] == (1 if i == 4 else i)
        assert q['gap_detected'] is (i > 1)
        bodies[r['event_id']] = r['body_hex']
    records = e['read_committed']
    assert len(records) == 5
    assert [r['event_id'] for r in records] == ['ops022-price-1','ops022-price-2','ops022-price-2','ops022-price-3','ops022-price-4']
    for i, r in enumerate(records):
        assert r['body_hex'] == bodies[r['event_id']] and r['key'] == 'BTCUSDT'
        assert r['partition'] == records[0]['partition'] and r['offset'] >= 0
        assert i == 0 or r['offset'] > records[i-1]['offset']
    q = e['latest']
    assert q['source_id'] == '14' and q['revision']['generation'] == 2 and q['revision']['sequence'] == 1
    assert q['gap_detected'] is True and q['stream_state'] == 'live'
    kf = joint.read(path / 'kafka/report.json')
    assert kf['fsync']['count'] is None and kf['fsync']['time_ms'] is None
    assert kf['fsync']['qualification_blocked'] is True
    for phase in ('publish_first','publish_lost','publish_retry','relay_tail'):
        assert sum(v['apparent_bytes'] for key,v in kf['phases'][phase]['delta_previous'].items() if '-prices-' in key and key.endswith('/.log')) > 0
    pg = joint.read(path / 'postgres/report.json')
    assert pg['initial_api_accounts'] == pg['final_api_accounts'] == 0
    for phase in ('accept_small','accept_4095_gap','accept_4096','accept_generation2','claim_first','mark_first','claim_lost','reclaim','mark_retry','relay_tail'):
        assert pg['phases'][phase]['operation_delta']['wal_insert_bytes'] > 0
    (path/'resolved-manifest.json').write_text(json.dumps({'format_version':1, 'source_id':e['source_id'],
        'evidence_sha256':hashlib.sha256((path/'identities.json').read_bytes()).hexdigest(),
        'outbox': rows, 'read_committed':records, 'qualification':m['qualification']},indent=2)+'\n')
    return resources


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('paths',type=Path,nargs='+')
    results = [verify(path) for path in p.parse_args().paths]
    for key in ('postgres_project','kafka_project','prefix'):
        assert len({r[key] for r in results}) == len(results)
    print(json.dumps(results,indent=2))
