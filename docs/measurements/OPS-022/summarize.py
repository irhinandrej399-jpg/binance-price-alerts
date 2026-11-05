"""Join prior API continuation coverage with the independent market-data source.

All paths are repository-relative. This is an inventory of evidence and gaps,
not a cost table: no subtraction, normalization, safety factor or quotas.
"""
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT/'scripts'))
import verify_joint_measurement as joint

COMPONENTS = {
    'sql_heap': ('postgres','relations; schema=app, kind=r, forks main/fsm/vm/init', 'observed-limited'),
    'sql_index': ('postgres','relations; kind=i, app indexes and TOAST indexes separately', 'observed-limited'),
    'sql_toast': ('postgres','relations; kind=t plus its indexes via toast_oid/OID; no total_relation_size addition', 'observed-limited'),
    'wal': ('postgres','operation_delta.wal_insert_bytes; wal_segment_file_bytes separately; cluster-wide once', 'observed-limited'),
    'temp': ('postgres','database_stats.temp_bytes/temp_files; io object=temp relation; live temp files', 'partial-unsupported'),
    'postgres_fsync': ('postgres','io[].fsyncs/fsync_time plus checkpointer; null cells unsupported; OS calls, not device', 'partial-unsupported'),
    'kafka_log': ('kafka','files[].size/blocks; .log groups (business/internal/metadata); allocated blocks * 512', 'observed-limited'),
    'kafka_index': ('kafka','files .index/.timeindex/.txnindex; apparent and allocated separately', 'observed-limited'),
    'kafka_segment': ('kafka','full file inventory and segment names; preallocated baseline retained; roll upper bound unmeasured', 'observed-limited'),
    'kafka_transaction': ('kafka','__transaction_state plus business transaction markers/.txnindex; no independent marker-byte attribution', 'observed-limited'),
    'kafka_fsync': ('kafka','report.fsync; count/time unsupported, no zero substitution', 'unsupported'),
}


def read(p):
    return json.loads(p.read_text())


def repo_path(p):
    return str(p.resolve().relative_to(ROOT))


def build():
    old_path = ROOT/'docs/measurements/OPS-026/coverage.json'
    old = read(old_path)
    classes = []
    for item in old['classes']:
        c = dict(item)
        for key in ('run','report'):
            c[key] = repo_path(old_path.parent/item[key])
        c['raw'] = [repo_path(old_path.parent/f) for f in item['raw']]
        classes.append(c)
    def add(label, phases, gaps):
        run = HERE/'run-a'
        prefix = repo_path(run)
        raw = [f'{prefix}/postgres/{p}-{s}.json.gz' for p in phases for s in ('before','after','checkpoint')]
        raw += [f'{prefix}/kafka/{p}{s}.json.gz' for p in phases for s in ('-before','')]
        classes.append({'class':label,'run':prefix,'report':prefix+'/report.json','phases':phases,'raw':raw,'blocking_gaps':gaps})
    add('market-data generation/status/gap',['begin','status_live','begin_again','stale_generation','status_gap','status_sticky'],['large generation counters/many pairs/contention','all status/error variants'])
    add('market-data accept/SQL outbox/size boundary',['accept_small','accept_4095_gap','accept_4096','accept_generation2','reject_4097'],['all field shapes/max price and pair lengths','correlation padding compressible; worst-case TOAST unmeasured','SQL lost commit/rollback/crash variants'])
    add('market-data repeat/old source version',['repeat','old'],['all duplicate/conflict/contention variants'])
    add('market-data claim/mark/reclaim',['claim_first','mark_first','claim_lost','reclaim','mark_retry','claim_empty'],['contention/large backlog scan','stale mark/claim overflow/fencing variants'])
    add('market-data first publish/lost reply/physical duplicate',['publish_first','publish_lost','publish_retry'],['segment roll/incompressible and all maximum field shapes','finite total repeat bound absent'])
    add('market-data production serial relay/max batch',['relay_tail'],['full runtime/load/restart/takeover variants','longer outbox retained history unbounded'])
    # A source price is not an API workflow and not an automatic new obligation.
    # Its mandatory downstream path has its own input bounds, currently unmeasured.
    classes.append({'class':'market-data price downstream matcher continuation','run':None,'report':None,'phases':[],'raw':[],
        'blocking_gaps':['No measured market-data price Prefix -> candidates -> canonical API workflow -> notifications boundary; API fixture above covers only its own source identities.']})
    classes.append({'class':'future ledger service writes and DEC-008 baseline fixture','run':None,'report':None,'phases':[],'raw':[],
        'blocking_gaps':['Ledger schema absent; conservative bound and eventual recheck required in OPS-018/015/016.',
                         'Accounts/auth/catalog and initial 1M/boundary 1.5M subscriptions/retained history not measured at full DEC-008 profile.']})
    for c in classes:
        c['components'] = {}
        for name,(collector,selector,status) in COMPONENTS.items():
            refs = [p for p in c['raw'] if f'/{collector}/' in p]
            reports = [] if c['run'] is None else [c['run']+f'/{collector}/report.json']
            c['components'][name] = {'status':status if refs else 'unmeasured','raw':refs,'reports':reports,'selector':selector,
                'blocks_upper_bound':True}
        validate_class(c)
    coverage = {'format_version':1,'qualified':False,'classes':classes,
        'source_boundaries':{'api':'OPS-024/run-a2 -> OPS-025/run-a3 -> OPS-026 reports/handler separate datasets',
                             'market_data':'OPS-022 independently generated source ID/bytes; no API trigger/attempt IDs'},
        'prior_coverage':{'path':repo_path(old_path),'sha256':hashlib.sha256(old_path.read_bytes()).hexdigest()},
        'global_blockers':[x for x in old['global_blockers'] if x!='market-data source remains OPS-022']+[
            'market-data measured source does not cover downstream price Prefix/candidate/workflow cost',
            'all component observations are bounded experiments, not qualified maxima; unknown remains blocking'],
        'components':list(COMPONENTS)}
    summary = {}
    runs = [ROOT/c['run'] for c in classes if c['run']]
    runs += [HERE/'run-b', ROOT/'docs/measurements/OPS-024/run-b2', ROOT/'docs/measurements/OPS-025/run-b3', ROOT/'docs/measurements/OPS-026/reports-b', ROOT/'docs/measurements/OPS-026/handler-b']
    for run in dict.fromkeys(runs):
        report = read(run/'report.json')
        pg,k = [read(run/x/'report.json') for x in ('postgres','kafka')]
        phases = {}
        for p,values in pg['phases'].items():
            windows = {}
            for w in ('background_delta','operation_delta','checkpoint_delta','inclusive_delta'):
                d = values[w]
                windows[w] = {'cluster_wal_bytes':d['wal_insert_bytes'],'cluster_wal_segment_file_bytes':d['wal_segment_file_bytes'],
                    'databases':d['databases']}
            phases[p] = {'postgres':windows,'kafka_operation':k['phases'][p]['delta_previous'],
                         'kafka_background':k['phases'][p]['background_delta']}
        summary[repo_path(run)] = {'report_sha256':hashlib.sha256((run/'report.json').read_bytes()).hexdigest(),
            'source_id':report['source_id'],'qualified':False,'phases':phases,
            'unsupported':{'postgres':pg['unsupported'],'kafka_fsync':k['fsync']}}
    validate(coverage, summary)
    (HERE/'coverage.json').write_text(json.dumps(coverage,indent=2)+'\n')
    (HERE/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(f'{len(classes)} classes, {len(summary)} runs; component references and unknown blockers verified')


def validate_class(c):
    assert c['blocking_gaps']
    if c['run'] is None:
        assert c['report'] is None and not c['phases'] and not c['raw']
    else:
        m=read(ROOT/c['run']/'manifest.json')
        assert set(c['phases']) <= {p['phase_id'] for p in m['phases']}
        assert (ROOT/c['report']).is_file()
        assert c['raw'] and all((ROOT/f).is_file() for f in c['raw'])
        for phase in c['phases']:
            expected = [c['run']+f'/postgres/{phase}-{suffix}.json.gz' for suffix in ('before','after','checkpoint')]
            expected += [c['run']+f'/kafka/{phase}.json.gz']
            assert set(expected) <= set(c['raw'])
    assert set(c['components']) == set(COMPONENTS)
    for component,info in c['components'].items():
        status = COMPONENTS[component][2] if c['run'] else 'unmeasured'
        assert info['status'] == status and info['blocks_upper_bound'] is True
        collector = COMPONENTS[component][0]
        assert info['raw'] == [p for p in c['raw'] if f'/{collector}/' in p]
        assert info['reports'] == ([] if c['run'] is None else [c['run']+f'/{collector}/report.json'])
        assert info['selector'] == COMPONENTS[component][1]
        assert all((ROOT/p).is_file() for p in info['raw']+info['reports'])
        if c['run']:
            assert info['raw'] and info['reports']
        else:
            assert not info['raw'] and not info['reports']


def validate(coverage, summary):
    assert coverage['qualified'] is False and coverage['global_blockers']
    assert coverage['components'] == list(COMPONENTS)
    assert len({c['class'] for c in coverage['classes']}) == len(coverage['classes']) == 25
    for c in coverage['classes']:
        validate_class(c)
    for run,values in summary.items():
        joint.verify(ROOT/run)
        pg,k = [read(ROOT/run/x/'report.json') for x in ('postgres','kafka')]
        assert values['qualified'] is False
        assert values['report_sha256'] == hashlib.sha256((ROOT/run/'report.json').read_bytes()).hexdigest()
        assert values['unsupported']['postgres'] == pg['unsupported']
        assert values['unsupported']['kafka_fsync'] == k['fsync']
        assert k['fsync']['status'] == 'unsupported'
        assert k['fsync']['count'] is None and k['fsync']['time_ms'] is None
        assert k['fsync']['qualification_blocked'] is True
        assert set(values['phases']) == set(pg['phases'])
        for p,v in values['phases'].items():
            for w,d in v['postgres'].items():
                joint.numeric(d['cluster_wal_bytes'])
                assert d['cluster_wal_bytes'] == pg['phases'][p][w]['wal_insert_bytes']
                assert d['cluster_wal_segment_file_bytes'] == pg['phases'][p][w]['wal_segment_file_bytes']
                assert d['databases'] == pg['phases'][p][w]['databases']
            assert v['kafka_operation'] == k['phases'][p]['delta_previous']
            assert v['kafka_background'] == k['phases'][p]['background_delta']


if __name__ == '__main__':
    build()
