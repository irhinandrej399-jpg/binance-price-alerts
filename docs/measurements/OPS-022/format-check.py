"""Corrupt disposable copies and verify unknown never becomes zero or qualified."""
import copy
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'scripts'))
import verify_marketdata_measurement as verifier
spec = importlib.util.spec_from_file_location('ops022summary',HERE/'summarize.py')
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


def main():
    cases = {
        'unknown_wal':('postgres/report.json',lambda x:x['phases']['empty']['operation_delta'].__setitem__('wal_insert_bytes',None)),
        'missing_phase':('identities.json',lambda x:x['phases'].pop()),
        'changed_body':('identities.json',lambda x:x['outbox'][0].__setitem__('body_hex','00')),
        'same_duplicate_offset':('identities.json',lambda x:x['read_committed'][2].__setitem__('offset',x['read_committed'][1]['offset'])),
        'missing_sql_mark':('identities.json',lambda x:x['outbox'][1].__setitem__('published',False)),
        'bad_boundary_size':('identities.json',lambda x:next(p for p in x['phases'] if p['phase_id']=='reject_4097')['result'].__setitem__('bytes',4096)),
        'kafka_fsync_zero':('kafka/report.json',lambda x:x.__setitem__('fsync',{'status':'supported','count':0,'time':0})),
        'unsupported_fsync_count_zero':('kafka/report.json',lambda x:x['fsync'].__setitem__('count',0)),
        'claim_token_reused':('identities.json',lambda x:x['outbox'][1].__setitem__('claim_token',1)),
    }
    results={}
    with tempfile.TemporaryDirectory(prefix='ops022-format-') as tmp:
        dest=Path(tmp)/'run'
        shutil.copytree(HERE/'run-a',dest)
        for name,(file,mutate) in cases.items():
            p=dest/file; original=p.read_bytes(); data=json.loads(original); mutate(data); p.write_text(json.dumps(data))
            try:
                verifier.verify(dest)
            except (AssertionError,KeyError,ValueError,TypeError):
                results[name]='rejected'
            else:
                raise AssertionError('corruption accepted: '+name)
            finally:
                p.write_bytes(original)
    c=json.loads((HERE/'coverage.json').read_text())
    s=json.loads((HERE/'summary.json').read_text())
    for name,mutate in {
        'summary_unknown_as_zero':lambda c,s: next(iter(s.values()))['unsupported'].__setitem__('postgres',{}),
        'summary_qualified':lambda c,s:c.__setitem__('qualified',True),
        'unmeasured_component_as_zero':lambda c,s:c['classes'][-1]['components']['sql_heap'].__setitem__('status','zero'),
        'missing_component':lambda c,s:c['classes'][0]['components'].pop('sql_toast'),
        'unsupported_fsync_as_observed':lambda c,s:c['classes'][0]['components']['kafka_fsync'].__setitem__('status','observed-limited'),
    }.items():
        cc,ss=copy.deepcopy(c),copy.deepcopy(s); mutate(cc,ss)
        try:
            summary.validate(cc,ss)
        except (AssertionError,KeyError,ValueError,TypeError):
            results[name]='rejected'
        else:
            raise AssertionError('corruption accepted: '+name)
    (HERE/'format-check.json').write_text(json.dumps(results,indent=2)+'\n')
    print(json.dumps(results,indent=2))


if __name__=='__main__':
    main()
