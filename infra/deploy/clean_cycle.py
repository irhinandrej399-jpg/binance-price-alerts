#!/usr/bin/env python3
"""One bounded cleaner cycle for the reviewed one-replica deployment inventory."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

from deploy import Deployment, maintenance, private
from observe import sql


REGISTRIES = tuple([f'prices-p{p}' for p in range(16)] + ['changes-p0'] +
                   [f'delivery-p{p}' for p in range(16)])


def plans(d):
    """Operator census, plus prospective IDs for exactly one fixed 16-partition replica.

    No SQL secrets reach the cleaner container. No registry is initialized here.
    The hashes mirror replica's documented deployment identity, checked in smoke.
    """
    inv = json.loads((d.data / 'inventory/sources.json').read_text())
    runtime, sources = inv['Runtime'], inv['Sources']
    if inv['Version'] != 1 or runtime['MATCHER_REPLICA_ID'] != 'one':
        raise ValueError('unsupported inventory')
    census = sql(d, 'matcher', """
SELECT json_build_object(
 'projections',(SELECT json_agg(projection_id) FROM
   (SELECT projection_id FROM app.matcher_projection_grants UNION SELECT projection_id FROM app.matcher_checkpoints) p),
 'prices',(SELECT json_agg(json_build_object('id',scope,'partition',partition,'cluster',source_cluster,
   'tid',encode(source_topic_id,'hex'),'topic',topic)) FROM app.matcher_price_sources),
 'assignments',(SELECT json_agg(json_build_object('id',scope,
   'identity',convert_from(grant_body,'UTF8')::jsonb->'Identity')) FROM app.matcher_assignments),
 'legacy',EXISTS(SELECT FROM app.matcher_price_checkpoints c WHERE NOT EXISTS
   (SELECT FROM app.matcher_price_sources s WHERE s.scope=c.scope AND s.topic=c.topic AND s.partition=c.partition)));
""")
    if census['legacy']:
        raise ValueError('unbound historical consumer')

    def digest(value):
        return hashlib.sha256(json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()

    def consumer(kind, identity, endpoint='matcher', generation=None):
        name_key = identity if generation is None else identity + '\0' + generation
        result = dict(name=kind + '-' + hashlib.sha256(name_key.encode()).hexdigest(), kind=kind, id=identity, endpoint=endpoint)
        if generation is not None:
            result['generation'] = generation
        return result

    environment, group, topic = runtime['MATCHER_ENVIRONMENT'], runtime['MATCHER_GROUP'], sources['prices']['Topic']
    projection = 'replica:' + digest(dict(Environment=environment, Group=group, Topic=topic, Replica='one', Parts=list(range(16))))
    projections = sorted(set((census['projections'] or []) + [projection]))
    result = {'changes-p0': dict(source=sources['subscription-changes'], partition=0,
              consumers=[consumer('projection', v) for v in projections])}
    for p in range(16):
        scope = 'partition:' + digest(dict(Environment=environment, Group=group, Topic=topic, Partition=p))
        ids = {scope}
        for row in census.get('assignments') or []:
            identity = row['identity']
            if identity['Topic'] != topic or identity['Partition'] not in range(16):
                raise ValueError('historical assignment mismatch')
            if identity['Partition'] == p:
                # Include an assigned consumer even before it has accepted a
                # source-bound SQL event. Its unknown proof must block cleanup.
                ids.add(row['id'])
        for row in census['prices'] or []:
            source = sources['prices']
            if row['cluster'] != source['ClusterID'] or row['tid'] != bytes(source['TopicID']).hex() or row['topic'] != source['Topic']:
                raise ValueError('historical source mismatch')
            if row['partition'] not in range(16):
                raise ValueError('historical partition mismatch')
            if row['partition'] == p:
                ids.add(row['id'])
        result[f'prices-p{p}'] = dict(source=sources['prices'], partition=p, consumers=[consumer('prices', v) for v in sorted(ids)])
        binding = inv['Binding']
        result[f'delivery-p{p}'] = dict(source=sources['delivery-requests'], partition=p, consumers=[
            consumer('notifications', binding['ConsumerID'], 'notifications', binding['Generation'])])
    return result


def review(d, expected):
    for name, plan in expected.items():
        state = json.loads((d.data / 'cleaner' / name / 'registry.json').read_text())
        if state['source'] != plan['source'] or state['partition'] != plan['partition']:
            raise ValueError('source mismatch')
        # Explicit retirement is represented by a retained exact tombstone.
        enrolled = [{k: v for k, v in c.items() if k != 'retired'} for c in state['consumers']]
        if any(c not in enrolled for c in plan['consumers']):
            raise ValueError('unenrolled consumer')


def cycle(d):
    # Same operator lock as start/update/backup. Engine also owns each durable
    # registry lock. Init/register/retire are separate, explicit operator steps.
    with maintenance(d.data):
        if not (d.data / 'cleaner/enabled').is_file():
            raise ValueError('cleaner not enabled after inventory review')
        if any(not (d.data / 'cleaner' / name / 'registry.json').is_file() for name in REGISTRIES):
            raise ValueError('incomplete registry set')
        review(d, plans(d))
        # One container, individual CLI deadlines/offset bounds; any unknown or
        # refusal makes the whole cycle fail and stops subsequent deletions.
        script = 'set -e; for name in "$@"; do /app/service -operation clean -state-dir "/state/$name" -config /inputs/clients.json -max-offsets 1000 -timeout 15s; done'
        output = d.dc('run', '--rm', '--no-deps', '--entrypoint', '/bin/bash', 'kafka-cleaner',
                      '-ec', script, 'clean-cycle', *REGISTRIES, timeout=540)
        print(output, end='', flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env', type=Path, required=True)
    p.add_argument('--draft', action='store_true', help='write input inventories only; never init or enable')
    args = p.parse_args()
    try:
        d = Deployment(args.env)
        if args.draft:
            with maintenance(d.data):
                expected = plans(d)
                if any((d.data / 'config/cleaner' / (name + '.json')).exists() for name in expected):
                    raise ValueError('draft already exists; preserve reviewed input')
                for name, plan in expected.items():
                    path = d.data / 'config/cleaner' / (name + '.json')
                    private(path, json.dumps(plan) + '\n')
            print('Drafted 33 inventories; review all consumers before explicit init/register/enable')
        else:
            cycle(d)
    except Exception:
        print('WARNING cleaner_cycle_failed; preserve registry/floor; review inventory and proofs', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
