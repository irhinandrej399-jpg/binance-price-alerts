"""Build evidence-backed coverage and byte tables for OPS-022/018 handoff."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def read(p):
    return json.loads(p.read_text())


def main():
    summary = {}
    for name in ('reports-a', 'reports-b', 'handler-a', 'handler-b'):
        run = HERE / name
        pg, kafka = [read(run / owner / 'report.json') for owner in ('postgres', 'kafka')]
        e = read(run / 'identities.json')
        phases = {}
        for phase, values in pg['phases'].items():
            result = {}
            for window in ('background_delta', 'operation_delta', 'checkpoint_delta', 'inclusive_delta'):
                d = values[window]
                result[window] = {
                    'cluster_wal_bytes': d['wal_insert_bytes'],
                    'app_relation_bytes_by_owner': {
                        owner: sum(v for key, v in x['relation_bytes'].items() if key.startswith('app/'))
                        for owner, x in d['databases'].items()},
                }
            result['kafka_operation_delta'] = kafka['phases'][phase]['delta_previous']
            phases[phase] = result
        summary[name] = {'report_sha256': hashlib.sha256((run/'report.json').read_bytes()).hexdigest(),
                         'boundary': e['boundary'], 'phases': phases, 'qualified': False,
                         'unsupported': {'postgres': pg['unsupported'], 'kafka_fsync': kafka['fsync']}}
    (HERE/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    coverage = []

    def add(label, run, phases, gaps):
        path = HERE / run
        manifest = read(path / 'manifest.json')
        assert set(phases) <= {p['phase_id'] for p in manifest['phases']}
        files = [f'{run}/postgres/{p}-{suffix}.json.gz' for p in phases for suffix in ('before','after','checkpoint')]
        files += [f'{run}/kafka/{p}.json.gz' for p in phases]
        assert all((HERE / f).is_file() for f in files)
        coverage.append({'class': label, 'run': run, 'report': run+'/report.json', 'phases': phases,
                         'raw': files, 'blocking_gaps': gaps})

    api, matcher = '../OPS-024/run-a2', '../OPS-025/run-a3'
    add('API create/slot/journal',api,['create'],['maximum input/batch/TOAST','registration/auth not measured'])
    add('API first workflow/outbox/replay',api,['workflow','workflow_replay'],['maximum payloads','wait/blocked/reactivation variants'])
    add('API cancel/replay',api,['cancel','cancel_replay'],['all concurrent/late variants'])
    add('API prepare/admission/replay',api,['prepare','admission_wait','admission','admission_replay'],['CAS/limiter waits see handler fixture; full variants not measured'])
    add('API outcomes/replay',api,['succeeded','retryable','uncertain','blocked','results_replay'],['historical/takeover/conflict/rollback variants'])
    for kind in ('subscription','delivery'):
        add('API '+kind+' relay/physical duplicate',api,[kind+'_'+x for x in ('first','lost','retry','drain')],['maximum batch/failure windows and finite total repeats'])
    add('matcher snapshot/replay',matcher,['snapshot_load','snapshot_repeat','initialize','replay_first','replay_repeat'],['nonempty snapshot rows/pages','finite total snapshot bound'])
    add('matcher projection/checkpoint',matcher,['live_first','live_physical_duplicate','live_tail','live_old_version'],['group coordinator/price Prefix/OffsetCommit','maximal after-images/batches'])
    add('matcher fencing',matcher,['assign','assign_repeat','takeover','stale_write','revoke'],['full live coordinator/runtime flow'])
    add('matcher pricework/job',matcher,['work_accept','work_results','work_complete'],['new canonical workflow RPC','superseded/restart/rollback','finite candidate bound'])
    add('notifications intake/checkpoint', 'reports-a',['checkpoint','intake_first','intake_repeat','intake_second','intake_physical_duplicate','intake_tail'],['group Step/rebalance/OffsetCommit','maximal bytes/batches'])
    add('notifications claims/attempts', 'reports-a',[f'claim_register_{i}' for i in range(4)],['contention/expiry/crash/takeover cost'])
    add('notifications outcomes/pending', 'reports-a',[f'{op}_{i}' for i in range(4) for op in ('outcome','outcome_repeat')],['exhausted retry/historical outcomes/late success/rollback'])
    add('notifications reporter/ack', 'reports-a',[f'{op}_{i}' for i in range(4) for op in ('report_loss','report_replay','report_empty')],['failed local ack/concurrent reporters','unbounded total reports'])
    add('notifications recovery/worker service transitions','reports-a',['discover','recovery_schedule','recovery_repeat','recovery_claim','recovery_park','worker_sweep'],['changed channel/condition false','full worker at load','unbounded checks/scans/history'])
    add('notifications handler/prepare/both limiters/new admission/HTTP','handler-a',[f'{op}_{i}' for i in range(4) for op in ('handle','handle_retry')],['limiter/condition wait paths','retry exhaustion','maximal messages/TOAST','full worker/load'])
    (HERE/'coverage.json').write_text(json.dumps({'format_version':1,'qualified':False,'source_id':'OPS-024-api-v1','classes':coverage,
        'global_blockers':['PostgreSQL temp peak/device IO unsupported','Kafka fsync unsupported','finite input/batch/retention bounds incomplete','write variants/maximal data uncovered','market-data source remains OPS-022','DEC-008 full load fixture absent'],
        'note':'Physical duplicates are transport records, not new logical obligations. Different fixture boundaries are not one historical SQL snapshot. Costs are full windows; no empty subtraction or double sum of inclusive.'},indent=2)+'\n')
    print('summary.json and coverage.json: all raw references exist')


if __name__ == '__main__':
    main()
