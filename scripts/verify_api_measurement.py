#!/usr/bin/env python3
"""Validate OPS-024 raw, physical duplicates, transition bindings and resolved manifest."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

import verify_joint_measurement as joint


def verify(path):
    resources = joint.verify(path)
    report = joint.read(path / 'report.json')
    manifest = joint.read(path / 'manifest.json')
    evidence = joint.read(path / 'identities.json')
    assert manifest['source_id'] == evidence['source_id'] == 'OPS-024-api-v1'
    assert evidence['format_version'] == 1 and evidence['sql_role'] == 'api_runtime'
    assert manifest['qualification']['qualified'] is False
    assert manifest['limits']['journal_append_max_batch'] is None
    assert manifest['limits']['subscription_relay_max_batch'] is None
    assert len(manifest['qualification']['blockers']) >= 6
    phases = {p['phase_id']: p['result'] for p in evidence['phases']}
    assert list(phases) == [p['phase_id'] for p in manifest['phases']]
    ids = evidence['identities']
    assert len(ids) == 5 and len({i['subscription_id'] for i in ids}) == 5
    assert len({i['trigger_id'] for i in ids[:4]}) == 4
    assert len({i['attempt_id'] for i in ids[:4]}) == 4
    assert all(i['owner_id'] == ids[0]['owner_id'] for i in ids)
    assert ids[4]['trigger_id'] == ids[4]['attempt_id'] == ''
    journal = evidence['journal']
    outbox = evidence['outbox']
    assert len(journal) == evidence['relay_cursor'] == 19
    assert len(outbox) == evidence['delivery_published'] == 4
    assert [j['position'] for j in journal] == list(range(1, 20))
    assert all(j['transaction_end'] == j['position'] for j in journal)
    assert phases['empty']['operations'] == 0
    assert len(phases['create']) == 5 and len(phases['prepare']) == 4
    assert phases['workflow'] == phases['workflow_replay']
    assert phases['cancel'] == phases['cancel_replay']
    assert phases['admission'] == phases['admission_replay']
    admission_quotes = [v['attempt']['quote'] for v in phases['admission']]
    assert all(q == admission_quotes[0] for q in admission_quotes)
    assert admission_quotes[0]['revision']['sequence'] == 3
    assert admission_quotes[0]['source_id'] == 'OPS-024-api-v1'
    assert admission_quotes[0]['price'] == '101'
    assert phases['admission_wait']['outcome'] == 'wait_condition'
    assert [p['state'] for p in phases['results_replay']] == ['delivered','pending_delivery','pending_delivery','delivery_blocked']
    for i, name in enumerate(('succeeded', 'retryable', 'uncertain', 'blocked')):
        assert phases[name] == phases['results_replay'][i]
    for name in ('subscription', 'delivery'):
        assert phases[name+'_first']['worked'] is True
        assert phases[name+'_lost'] == {'worked': False, 'commit': 'unknown-to-client-broker-committed', 'broker_endtxn_code': 0}
        assert phases[name+'_retry']['worked'] is True
    assert phases['subscription_drain']['published'] == 17
    assert phases['delivery_drain']['published'] == 2
    # Link actual source bytes and durable version/position to the measured phase.
    phase_for_position = {**{i:'create' for i in range(1,6)}, **{i:'workflow' for i in range(6,10)}, 10:'cancel',11:'admission_wait',**{i:'admission' for i in range(12,16)},16:'succeeded',17:'retryable',18:'uncertain',19:'blocked'}
    bindings = []
    known = {i['subscription_id']: i for i in ids}
    for j in journal:
        env = json.loads(bytes.fromhex(j['body_hex']))
        change = env['payload']
        identity = known[env['ordering_key']]
        assert env['event_id'] == j['event_id']
        assert change['position'] == change['transaction_end'] == j['position']
        assert change['subscription_id'] == identity['subscription_id']
        bindings.append({'phase_id': phase_for_position[j['position']], 'event_id': j['event_id'],
                         'subscription_id': identity['subscription_id'], 'trigger_id': identity['trigger_id'] or None,
                         'attempt_id': identity['attempt_id'] or None, 'position': j['position'],
                         'wire_bytes': len(bytes.fromhex(j['body_hex'])), 'body_sha256': hashlib.sha256(bytes.fromhex(j['body_hex'])).hexdigest()})
    for topic, originals in [('subscription-changes', journal), ('delivery-requests', outbox)]:
        expected = {r['event_id']: r['body_hex'] for r in originals}
        records = evidence['read_committed'][topic]
        assert len(records) == len(originals)+1
        counts = Counter(r['event_id'] for r in records)
        assert set(counts) == set(expected)
        assert sorted(counts.values()) == [1]*(len(originals)-1)+[2]
        assert len({(r['partition'],r['offset']) for r in records}) == len(records)
        assert all(r['body_hex'] == expected[r['event_id']] for r in records)
        assert all(r['key'] in known for r in records)
        if topic == 'subscription-changes':
            assert all(r['partition'] == 0 for r in records)
            assert [r['event_id'] for r in records] == [journal[0]['event_id'],journal[1]['event_id'],*[j['event_id'] for j in journal[1:]]]
        else:
            assert {w['trigger_id'] for w in outbox} == {i['trigger_id'] for i in ids[:4]}
            for w in outbox:
                env = json.loads(bytes.fromhex(w['body_hex']))
                assert env['payload']['trigger_id'] == w['trigger_id']
                candidate = env['payload']['candidate_quote']
                assert candidate['revision']['sequence'] == 1
                assert candidate['price'] == '101'
                assert candidate['trade_time'] == admission_quotes[0]['trade_time']
                assert env['event_id'] == w['event_id']
                matching = [r for r in records if r['event_id'] == w['event_id']]
                assert len({r['partition'] for r in matching}) == 1
    pg = joint.read(path / 'postgres/report.json')
    assert pg['initial_api_accounts'] == 0 and pg['final_api_accounts'] == 1
    assert sum(pg['phases']['create']['operation_delta']['databases']['api']['relation_bytes'].values()) > 0
    resolved = {'format_version':1,'source_id':evidence['source_id'], 'template_sha256':report['manifest_sha256'],
                'evidence_sha256':hashlib.sha256((path/'identities.json').read_bytes()).hexdigest(),
                'coverage':manifest['qualification'],'identities':ids,'journal_bindings':bindings,
                'outbox':outbox,'continuations':{'OPS-025':'not-measured','OPS-026':'not-measured','OPS-022':'not-measured'}}
    (path/'resolved-manifest.json').write_text(json.dumps(resolved,indent=2)+'\n')
    return resources


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('paths', type=Path, nargs='+')
    a = p.parse_args()
    results = [verify(path) for path in a.paths]
    for key in ('postgres_project','kafka_project','prefix'):
        assert len({r[key] for r in results}) == len(results), 'repeat needs clean resources'
    print(json.dumps(results, indent=2))
