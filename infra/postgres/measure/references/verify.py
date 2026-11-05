"""OPS-040 full reference artifact verifier; blocked evidence only."""
import argparse
import copy
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile

import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
from artifacts import digest, postgres_rows

BASE = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location('lifecycle', BASE/'lifecycle/verify.py')
v = importlib.util.module_from_spec(spec); spec.loader.exec_module(v)
a, inv, require = v.a, v.inv, v.require
TAGS = ['receive-opened','receive-queue-only','receive-delivered','receive-discharged','receive-reused','drop-opened','drop-queue-only','drop-discharged','drop-reused','mapping-opened','mapping-only','mapping-unmapped','mapping-reused']

def points(directory, rows, replay):
    ps = [json.loads(l) for l in (directory/'filesystem.txt').read_text().splitlines()]
    require([p['tag'] for p in ps] == TAGS, 'reference stages missing')
    gates = [r for r in rows if r['kind']=='enter' and r['nr']==155 and r['pid']==ps[0]['tid']]
    require(len(gates)==len(ps), 'reference checkpoints missing')
    result = {}
    for i,(p,g) in enumerate(zip(ps,gates)):
        cp = rows[g['seq']]
        require(cp['kind']=='inventory' and cp['phase']=='before', 'reference synchronization')
        group,start = v.identity(p['status'],p['stat'],p['tid'])
        require(group==p['pid'] and inv.census(cp['census_before'])[p['tid']]==(group,(p['tid'],start)), 'helper identity/census')
        owner = next(o for o in cp['owners'] if o['tid']==p['tid'])
        s = p['source'];tag=p['tag']
        base=ps[0 if i<5 else 5 if i<9 else 9]['source']
        require(s['size']==(4096 if tag.endswith('reused') else 8192), 'source size')
        require(s['nlink']==(1 if tag.endswith(('opened','reused')) else 0), 'source nlink')
        if not tag.endswith('reused'):
            require(all(s[k]==base[k] for k in ('dev','ino','mode','size')), 'source identity changed')
        require(len(p['sockets'])==2 and p['sockets'][0]['fd']!=p['sockets'][1]['fd'], 'independent socket witnesses')
        matches = lambda e: e.get('stat') and inv.same(e['stat'],s)
        fds = [e for e in owner['fd_entries'] if matches(e)]
        for sock in p['sockets']:
            require(any(e.get('stat') and inv.same(e['stat'],sock) for e in owner['fd_entries']), 'socket identity missing')
        mappings = [l for l in owner['maps']['text'].splitlines() if len(l.split())>=5 and int(l.split()[4])==s['ino'] and '/tmp/work/reused' in l]
        independent = [l for l in p['maps'].splitlines() if len(l.split())>=5 and int(l.split()[4])==s['ino'] and '/tmp/work/reused' in l]
        require(mappings==independent and bool(mappings)==p['mapped'], 'mapping witness mismatch')
        for line in mappings:
            major,minor=(int(x,16) for x in line.split()[3].split(':'))
            require(((minor&255)|(major<<8)|((minor&~255)<<12))==s['dev'], 'mapping device identity')
        if tag.endswith('-opened') or tag.endswith('-reused') or tag=='receive-delivered':
            require(fds and any(e['path'].endswith('/'+str(s['fd'])) for e in fds), 'fstat/FD mismatch')
        else:
            # All inventoried owners, not just the helper, must show no FD to this inode.
            require(not any(matches(e) for o in cp['owners'] for e in o['fd_entries']), 'queue/mapping-only still has FD')
        if tag.endswith('queue-only'):
            require(p['queued_bytes']==1 and s['nlink']==0 and not mappings, 'queue-only interval missing')
            require(not any(matches(e) for e in cp['paths']), 'queue-only still has path')
            require(not any('/tmp/work/reused' in e['target'] for e in p['fds']), 'independent remaining source FD')
            lo=gates[i-1]['seq'];hi=g['seq']
            sends=[(e,x) for e,x in replay['returns'] if e['pid']==p['tid'] and e['nr']==211 and lo<e['seq']<hi and x['ret']==1]
            require(len(sends)==1, 'successful queue send missing')
            a.decode(sends[0][0],sends[0][0],False)
            # decode returns summaries; independently recorded SCM_RIGHTS FD stats are bound below.
            rawfds=[f for item in sends[0][0]['messages']['items'] for f in item['rights']]
            require(len(rawfds)==1 and all(rawfds[0]['stat'][k]==s[k] for k in ('dev','ino','size','mode','nlink')), 'queued right identity')
            closes=[(e,x) for e,x in replay['returns'] if e['pid']==p['tid'] and e['nr']==57 and e['args'][0]==s['fd'] and lo<e['seq']<hi and x['ret']==0]
            require(closes and closes[0][0]['seq']>sends[0][1]['seq'], 'sender close missing')
        else: require(p['queued_bytes']==0, 'queue not discharged')
        if tag in ('receive-delivered','drop-discharged'):
            lo=gates[i-1]['seq']
            rs=[(e,x) for e,x in replay['returns'] if e['pid']==p['tid'] and e['nr']==212 and lo<e['seq']<g['seq'] and x['ret']==1]
            require(len(rs)==1, 'queue receive/drop return')
            e,x=rs[0];item=x['messages']['items'][0]
            header=bytes.fromhex(item['header']['hex'])
            import struct
            flags=struct.unpack_from('<I',header,48)[0]
            require(bool(flags&8)==(tag=='drop-discharged'), 'drop MSG_CTRUNC witness')
            if tag=='receive-delivered':
                require(len(item['rights'])==1 and inv.same(item['rights'][0]['stat'],s), 'delivered identity')
            else: require(item['rights']==[], 'drop delivered FD')
        if tag.endswith('reused'):
            original=ps[0 if tag.startswith('receive') else 5 if tag.startswith('drop') else 9]['source']
            require(s['fd']==original['fd'] and s['size']==4096, 'name/FD reuse not exercised')
            result[tag]={'inode_reused':s['ino']==original['ino'] and s['dev']==original['dev']}
    require('socket queue reference history pending OPS-034' in rows[-1]['issues'], 'unsupported queue not blocked')
    return result

def verify(directory):
    report=json.loads((directory/'report.json').read_text())
    require(report['status']=='ancillary-captured-not-qualified' and report['mode']=='ops040-references-v1', 'run status')
    require(set(report['cleanup'])=={'container','volume','network'} and all(not x for x in report['cleanup'].values()), 'cleanup')
    require(report['qualification_blocked'] and all(report[k] is None for k in ('temp_peak_live_bytes','lower_bytes','upper_bytes')), 'false bounds')
    required={'full-lifecycle-verifier.py.txt','full-verifier.py.txt','artifact-transport.py.txt','full-inventory-verifier.py.txt','dependencies.json','filesystem.jsonl.gz','filesystem.txt','postgres.jsonl.gz','pg-witness.json','parallel-plan.txt','ordinary-processes.txt','after-parallel-processes.txt','main_linux.go.txt','message_linux.go.txt','inventory_linux.go.txt','lifecycle_linux.go.txt','control-main_linux.go.txt','control-references_linux.go.txt','gate-source.py.txt','verifier-source.py.txt','inventory-verifier.py.txt','lifecycle-verifier.py.txt','references-verifier.py.txt'}
    require(required<=report['sha256'].keys() and len(report['binary_sha256'])==64 and len(report['control_binary_sha256'])==64,'mandatory artifact/source/binary hashes')
    require(set(report['archives'])=={'postgres.jsonl.gz'}, 'PG archive absent')
    require(set(report['archives']['postgres.jsonl.gz']['parts'])<=report['sha256'].keys(), 'unhashed archive chunk')
    for name,expected in report['sha256'].items():
        if name not in report['archives']:require(digest(directory/name)==expected,'hash '+name)
    rows=a.load(directory/'filesystem.jsonl.gz');r=v.replay(rows);refs=points(directory,rows,r)
    with postgres_rows(directory,report) as pg:
        pr=v.replay(pg)
        env=next(x for x in pg if x['kind']=='inventory_environment')
        require(not env['pg_witness']['error'] and json.loads(env['pg_witness']['text'])==json.loads((directory/'pg-witness.json').read_text()), 'PG independent input binding')
        require(json.loads(env['pg_witness']['text'])==dict(io_method=report['settings']['io_method'],settings=report['settings'],tablespaces=report['tablespaces']), 'PG report/input binding')
        require(report['settings']['io_method']=='sync' and report['settings']['fsync']=='on' and report['capture_exit']==0 and '2000' in report['ordinary_result'] and report['row_count']==2000, 'PG run/settings')
        spaces=report['tablespaces'];require({t['spcname'] for t in spaces}=={'pg_default','pg_global','ops040_custom'},'tablespace controls')
        require(next(t for t in spaces if t['spcname']=='ops040_custom')['location']=='/tmp/work/custom','custom location')
        plan_text=(directory/'parallel-plan.txt').read_text()
        plan=json.loads(plan_text[plan_text.index('['):])
        nodes=[]
        def walk(n):
            nodes.append(n)
            for x in n.get('Plans',[]):walk(x)
        walk(plan[0]['Plan']);require(any(n.get('Workers Launched',0)>0 for n in nodes),'no actual parallel worker')
        root=next(x['pid'] for x in pg if x['kind']=='root')
        backends=set();custom=default=False
        for cp in pg:
            if cp['kind']!='inventory':continue
            for o in cp['owners']:
                if o['traced'] and o['pid']!=root:backends.add(o['pid'])
                for e in o['fd_entries']:
                    custom |= '/tmp/work/custom/' in e['link']['text']
                    default |= '/base/pgsql_tmp/' in e['link']['text']
        require(custom and default, 'default/custom temp FD missing')
        require(len(backends)>=3,'backend/worker inventories absent')
        for x in (r,pr):x.pop('returns');x['birth_count']=len(x.pop('births'))
    deps=json.loads((directory/'dependencies.json').read_text())
    require(set(deps)=={'OPS-038','OPS-039'}, 'dependency evidence absent')
    for ticket,run_name in (('OPS-038','run-c'),('OPS-039','run-f')):
        d=deps[ticket]; expected=Path('docs/measurements')/ticket/run_name
        require(d['directory']==str(expected), 'unexpected dependency location')
        require(hashlib.sha256((expected/'report.json').read_bytes()).hexdigest()==d['report_sha256'], 'dependency preservation')
        (inv.verify if ticket=='OPS-038' else v.verify)(expected)
    return dict(references=refs,filesystem=r,postgres=pr,backend_pids=sorted(backends),qualification_blocked=True)

def negative_checks(directory):
    checks=[]
    def reject(name,mutate):
        with tempfile.TemporaryDirectory(prefix='ops040-tamper-') as tmp:
            dest=Path(tmp)/'data';shutil.copytree(directory,dest)
            rows=a.load(dest/'filesystem.jsonl.gz');mutate(rows)
            for i,r in enumerate(rows,1):r['seq']=i
            path=dest/'filesystem.jsonl.gz'
            with gzip.open(path,'wt',compresslevel=1) as f:
                for r in rows:f.write(json.dumps(r)+'\n')
            report=json.loads((dest/'report.json').read_text());report['sha256'][path.name]=hashlib.sha256(path.read_bytes()).hexdigest();(dest/'report.json').write_text(json.dumps(report))
            try:verify(dest)
            except (ValueError,KeyError,TypeError,IndexError,StopIteration):checks.append(name)
            else:raise ValueError('tamper accepted '+name)
    def cp(rows,index):
        root=next(r['pid'] for r in rows if r['kind']=='root')
        gate=[r for r in rows if r['kind']=='enter' and r['nr']==155 and r['pid']==root][index]
        return rows[gate['seq']]
    reject('missing-queue-only',lambda rs:rs.remove(cp(rs,1)))
    reject('missing-mapping-only',lambda rs:rs.remove(cp(rs,10)))
    reject('queue-census-omission',lambda rs:cp(rs,1)['census_before']['processes'].pop())
    reject('queue-owner-omission',lambda rs:cp(rs,1)['owners'].pop())
    reject('mapping-omission',lambda rs:next(o for o in cp(rs,10)['owners'] if o['tid']==cp(rs,10)['pid'])['maps'].__setitem__('text',''))
    reject('isolation-omission',lambda rs:next(r for r in rs if r['kind']=='inventory_environment').pop('isolation'))
    reject('lifecycle-omission',lambda rs:next(r for r in rs if r['kind']=='enter').pop('lifecycle'))
    reject('queue-send-omission',lambda rs:rs.remove(next(r for r in rs if r['kind']=='enter' and r['nr']==211)))
    def omit_mapping(rs):
        o=next(o for o in cp(rs,10)['owners'] if o['tid']==cp(rs,10)['pid'])
        lines=[l for l in o['maps']['text'].splitlines() if '/tmp/work/reused' not in l]
        o['maps']['text']='\n'.join(lines)+'\n'
        o['map_entries']=[e for e in o['map_entries'] if '/tmp/work/reused' not in e['line']]
    reject('consistent-mapping-removal',omit_mapping)
    reject('missing-queued-right-stat',lambda rs:next(r for r in rs if r['kind']=='enter' and r['nr']==211)['messages']['items'][0]['rights'][0].pop('stat'))
    reject('substituted-queued-inode',lambda rs:next(r for r in rs if r['kind']=='enter' and r['nr']==211)['messages']['items'][0]['rights'][0]['stat'].__setitem__('ino',1))
    reject('missing-paths',lambda rs:cp(rs,1).pop('paths'))
    reject('missing-namespaces',lambda rs:cp(rs,1)['owners'][0].pop('namespaces'))
    reject('missing-unsupported-queue',lambda rs:rs.remove(next(r for r in rs if r['kind']=='unsupported' and r['reason']=='socket queue reference history pending OPS-034')))
    reject('false-completeness',lambda rs:cp(rs,1)['consistency'].__setitem__('complete',True))
    return checks

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);p.add_argument('--record',action='store_true');args=p.parse_args()
    result={'result':verify(args.directory),'negative_checks':negative_checks(args.directory)}
    if args.record:(args.directory/'checks.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
