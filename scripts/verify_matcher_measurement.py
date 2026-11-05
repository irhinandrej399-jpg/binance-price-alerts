#!/usr/bin/env python3
"""Verify OPS-025 provenance, raw collectors, durable after-images and repeats."""
import argparse
import hashlib
import json
from pathlib import Path
import verify_joint_measurement as joint

ROOT = Path(__file__).resolve().parents[1]

def verify(path):
    resources = joint.verify(path)
    m = joint.read(path / 'manifest.json')
    e = joint.read(path / 'identities.json')
    src_path = ROOT / m['source']['path']
    assert hashlib.sha256(src_path.read_bytes()).hexdigest() == m['source']['sha256'] == e['source_sha256']
    src = joint.read(src_path)
    assert src['source_id'] == 'OPS-024-api-v1'
    assert e['source_id'] == m['source_id'] == 'OPS-025-matcher-v1'
    assert e['sql_role'] == 'matcher_runtime'
    assert e['format_version'] == 1
    assert m['qualification']['qualified'] is False
    assert m['qualification']['uncovered'] and m['qualification']['blockers']
    assert m['limits']['pricework_max_candidates'] is None
    assert [p['phase_id'] for p in e['phases']] == [p['phase_id'] for p in m['phases']]
    phases = {p['phase_id']:p['result'] for p in e['phases']}
    for phase in m['phases']:
        if phase['unit'] == 'adapter_call':
            assert phases[phase['phase_id']]['operations'] == phase['size']
    first, duplicate = phases['observe_first'], phases['observe_duplicate']
    assert first['checkpoint']['C'] == duplicate['checkpoint']['C'] == 2
    assert first['images'] == duplicate['images'] and len(first['images']) == 2
    assert duplicate['checkpoint']['NextOffset'] > first['checkpoint']['NextOffset']
    records = e['records']
    assert len(records) == 20
    originals = src['read_committed']['subscription-changes']
    for i, r in enumerate(records):
        assert r['source'] == originals[i]
        assert r['body_hex'] == originals[i]['body_hex']
        assert r['partition'] == 0
        assert r['topic'] == records[0]['topic']
        if i:
            assert r['offset'] > records[i-1]['offset']
    assert records[1]['body_hex'] == records[2]['body_hex']
    assert records[1]['source']['event_id'] == records[2]['source']['event_id']
    assert e['checkpoint'] == {'C':19, 'NextOffset':records[-1]['offset']+1}
    latest = {}
    for j in src['journal']:
        c = json.loads(bytes.fromhex(j['body_hex']))['payload']
        latest[c['subscription_id']] = c
    assert len(e['images']) == len(latest) == 5
    for image in e['images']:
        assert image['Snapshot'] is None
        c = image['Change']
        assert c == latest[c['subscription_id']]
    w = e['work']
    assert w['State'] == 'completed' and w['Completed'] == 4
    assert w['Input']['C'] == 5 and w['Input']['Position'] is None
    assert w['Input']['Quote'] == json.loads(bytes.fromhex(src['outbox'][0]['body_hex']))['payload']['candidate_quote']
    ids = {i['subscription_id']:i for i in src['identities'][:4]}
    assert len(w['Entries']) == 4
    for entry in w['Entries']:
        candidate = entry['Candidate']
        identity = ids[candidate['SubscriptionID']]
        assert candidate['OwnerID'] == identity['owner_id'] and candidate['Version'] == 1
        assert entry['Result'] == {'Outcome':'workflow','TriggerID':identity['trigger_id'],'WorkflowVersion':1}
    assert e['grant']['Token'] == 2 and e['grant_active'] is False
    assert e['generation']['Complete'] and e['generation']['C'] == e['generation']['Count'] == 0
    pg = joint.read(path / 'postgres/report.json')
    assert pg['initial_api_accounts'] == pg['final_api_accounts'] == 0
    for name in ('assign','initialize','replay_first','live_first','work_accept','work_results','work_complete'):
        delta = pg['phases'][name]['operation_delta']['databases']['matcher']
        assert pg['phases'][name]['operation_delta']['wal_insert_bytes'] > 0
        assert delta['relation_bytes']
    bindings = []
    names = ['live_sql_duplicate','live_first','live_physical_duplicate', *(['live_creates']*3), *(['live_tail']*14)]
    for name,r in zip(names,records):
        env = json.loads(bytes.fromhex(r['body_hex']))
        change = env['payload']
        bindings.append({'phase_id':name,'source_event_id':env['event_id'],
                         'subscription_id':change['subscription_id'], 'subscription_version':change['subscription_version'],
                         'trigger_id':change.get('workflow',{}).get('trigger_id'),
                         'position':change['position'],'source_offset':r['source']['offset'],
                         'offset':r['offset'],'wire_bytes':len(bytes.fromhex(r['body_hex']))})
    resolved = {'journal_bindings':bindings,'format_version':1,'source':m['source'], 'evidence_sha256':hashlib.sha256((path/'identities.json').read_bytes()).hexdigest(), 'coverage':m['qualification'], 'records':records,'identities':src['identities'],'checkpoint':e['checkpoint']}
    (path/'resolved-manifest.json').write_text(json.dumps(resolved,indent=2)+'\n')
    return resources

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths',type=Path,nargs='+')
    args = parser.parse_args()
    results = [verify(p) for p in args.paths]
    for key in ('postgres_project','kafka_project','prefix'):
        assert len({r[key] for r in results}) == len(results)
    print(json.dumps(results,indent=2))
