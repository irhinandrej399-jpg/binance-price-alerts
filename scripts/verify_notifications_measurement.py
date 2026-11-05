#!/usr/bin/env python3
"""Verify OPS-026 source identities, durable obligations and raw measurements."""
import argparse
import hashlib
import json
from pathlib import Path
import verify_joint_measurement as joint

ROOT = Path(__file__).resolve().parents[1]


def verify(path):
    resources = joint.verify(path)
    m, e = [joint.read(path / name) for name in ('manifest.json', 'identities.json')]
    src_path = ROOT / m['source']['path']
    assert hashlib.sha256(src_path.read_bytes()).hexdigest() == m['source']['sha256'] == e['source_sha256']
    src = joint.read(src_path)
    assert e['format_version'] == 1
    assert e['source_id'] == m['source_id'] == 'OPS-026-notifications-v1'
    assert e['sql_role'] == 'notifications_runtime'
    assert e['boundary'] == m['boundary'] in ('reports', 'handler')
    assert m['qualification']['qualified'] is False
    assert m['qualification']['uncovered'] and m['qualification']['blockers']
    assert m['limits']['retained_attempts_per_workflow'] is None
    phases = {p['phase_id']: p['result'] for p in e['phases']}
    assert list(phases) == [p['phase_id'] for p in m['phases']]
    for p in m['phases']:
        assert phases[p['phase_id']]['operations'] == p['size']
    records = e['records']
    originals = src['read_committed']['delivery-requests']
    assert len(records) == len(originals) == 5
    for i, (r, original) in enumerate(zip(records, originals)):
        assert r['source'] == original and r['body_hex'] == original['body_hex'] and r['key'] == original['key']
        assert r['topic'] == records[0]['topic'] and r['partition'] == 0
        assert r['offset'] >= 0 and (i == 0 or r['offset'] > records[i-1]['offset'])
    assert records[1]['body_hex'] == records[2]['body_hex']
    assert e['checkpoint'] == records[-1]['offset'] + 1
    assert phases['observe_intake'] == {'operations': 1, 'jobs': 4, 'next_offset': e['checkpoint']}
    assert len(phases['discover']['work']) == 4
    assert len(e['items']) == 4
    attempts = set()
    for i, item in enumerate(e['items']):
        identity = src['identities'][i]
        assert item['identity'] == identity
        original = next(r for r in originals if r['key'] == identity['subscription_id'])
        assert item['event_id'] == original['event_id']
        assert item['body_hex'] == original['body_hex']
        assert item['body_sha256'] == hashlib.sha256(bytes.fromhex(item['body_hex'])).hexdigest()
        admitted = next(a for a in src['admissions'] if a['attempt_id'] == identity['attempt_id'])
        response = bytes.fromhex(admitted['response'][2:])
        assert item['source_response_sha256'] == hashlib.sha256(response).hexdigest()
        assert item['source_permit'] == json.loads(response)['Attempt']
        assert item['api_state'] == ['delivered', 'pending_delivery', 'pending_delivery', 'delivery_blocked'][i]
        assert item['job_state'] == ['succeeded', 'retryable', 'uncertain', 'blocked'][i]
        attempt = item['current_attempt_id']
        assert attempt not in attempts
        attempts.add(attempt)
        if m['boundary'] == 'reports':
            assert attempt == identity['attempt_id'] and item['current_request_id'] == identity['request_id']
            assert item['response_hex'] == response.hex()
            assert phases[f'report_loss_{i}']['lost_reply'] is True
            assert phases[f'report_replay_{i}']['acked'] == 1
            assert phases[f'report_empty_{i}']['acked'] == 0
            if i == 1:
                assert phases['outcome_1']['plan'] == phases['outcome_repeat_1']['plan']
        else:
            assert attempt != identity['attempt_id'] and item['current_request_id'] != identity['request_id']
            fresh = json.loads(bytes.fromhex(item['response_hex']))['Attempt']
            assert fresh['ID'] == attempt and fresh['ChatID'] == 24001 and fresh['BindingVersion'] == 1
            assert fresh['ExpiresAt'] > fresh['Quote']['trade_time']
            assert phases[f'handle_{i}']['pending_write'] is True
            assert phases[f'handle_{i}']['http_calls'] == phases[f'handle_retry_{i}']['http_calls'] == i+1
    counts = e['counts']
    assert counts['delivery_jobs'] == counts['delivery_attempts'] == 4
    for kind in ('success', 'uncertain', 'retryable', 'blocked'):
        assert counts[kind+'_pending'] == 0
    for table in ('delivery_successes', 'delivery_uncertainties', 'delivery_retryables', 'delivery_blocks'):
        assert counts[table] == 1
    assert e['api_results'] == 4 and e['api_slots'] == 3
    if m['boundary'] == 'reports':
        assert e['http_calls'] == 0 and e['api_journal'] == 4
        assert counts['delivery_claims'] == 5 and counts['delivery_block_parks'] == 1
        assert e['items'][3]['fence'] == 2
    else:
        assert e['http_calls'] == 4 and e['api_journal'] == 8
        assert counts['delivery_claims'] == 4 and counts['delivery_block_parks'] == 0
    pg = joint.read(path / 'postgres/report.json')
    assert pg['initial_api_accounts'] == 0 and pg['final_api_accounts'] == 1
    for name in ('intake_first', 'intake_second', 'intake_physical_duplicate', 'intake_tail'):
        assert pg['phases'][name]['operation_delta']['wal_insert_bytes'] > 0
    resolved = {'format_version': 1, 'boundary': m['boundary'], 'source': m['source'],
                'evidence_sha256': hashlib.sha256((path/'identities.json').read_bytes()).hexdigest(),
                'records': records, 'items': e['items'], 'coverage': m['qualification']}
    (path / 'resolved-manifest.json').write_text(json.dumps(resolved, indent=2)+'\n')
    return resources


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths', type=Path, nargs='+')
    args = parser.parse_args()
    results = [verify(p) for p in args.paths]
    for key in ('postgres_project', 'kafka_project', 'prefix'):
        assert len({r[key] for r in results}) == len(results)
    print(json.dumps(results, indent=2))
