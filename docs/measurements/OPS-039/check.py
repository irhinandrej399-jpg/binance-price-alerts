"""Verify original-timeout diagnosis, exact repaired replay, and final controls."""
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import zlib

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('lifecycle',ROOT/'infra/postgres/measure/lifecycle/verify.py')
v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
HERE=Path(__file__).parent

def preserved(name):
    directory=HERE/name;report=json.loads((directory/'report.json').read_text())
    for path,digest in report['sha256'].items():
        v.require(hashlib.sha256((directory/path).read_bytes()).hexdigest()==digest,'hash mismatch '+name+'/'+path)
    v.require(all(not x for x in report['cleanup'].values()),'cleanup '+name)
    return directory,report

red,r=preserved('red')
v.require(r['captures']['exec']['status']=='timeout' and r['captures']['exec']['timeout_seconds']==8 and r['exec_baseline_returncode']==0,'original regression not reproduced')
raw=(red/'exec.partial.jsonl.gz').read_bytes();decoder=zlib.decompressobj(31)
rows=[json.loads(l) for l in decoder.decompress(raw).decode().splitlines()]
v.require(not decoder.eof and all(x['kind']!='footer' for x in rows),'unexpected complete timeout raw')
point=json.loads((red/'exec.stdout').read_text())
event=next(x for x in rows if x['kind']=='event' and x['event']==4)
v.require(event['pid']==point['pid'] and event['message']==point['tid']!=point['pid'],'timeout exec binding')
last=rows[-1];stop=next(x for x in reversed(rows) if x['kind']=='wait_stop')
v.require(last['kind']=='queued' and last['pid']==point['pid'] and stop['active']==point['tid'] and stop['pid']==point['pid'],'stale gate regression missing')
v.require(stop['pending'][str(point['tid'])]['nr']==221 and stop['pending'][str(point['tid'])]['entered'],'orphan exec pending missing')
v.require('Name:\ttrue\n' in stop['identity']['text'] and 'State:\tt (tracing stop)' in stop['identity']['text'],'independent stopped true identity')
proc_bytes=gzip.decompress((red/'exec-processes.txt.gz').read_bytes())
v.require(hashlib.sha256(proc_bytes).hexdigest()==r['procfs_uncompressed_sha256'],'lossless procfs hash')
proc=proc_bytes.decode()
v.require('Name:\ttrue\n' in proc and 'State:\tt (tracing stop)' in proc,'independent timeout procfs')

exact,r=preserved('exact-exec')
v.require(r['captures']['exec']['returncode']==0 and r['exec_baseline_returncode']==0,'exact baseline/repair failed')
rows=v.a.load(exact/'exec.jsonl.gz');result=v.replay(rows)
p=json.loads((exact/'exec.stdout').read_text());t=next(x for x in rows if x['kind']=='exec_transition')
v.require(t['former']==p['tid']!=p['pid']==t['pid'] and result['transitions']==1,'exact repaired former TID')
v.require(any(x['kind']=='wait_stop' and x['pid']==p['pid'] and 'Name:\ttrue\n' in x['identity']['text'] for x in rows),'exact repaired true procfs missing')
final=v.verify(HERE/'run-f')
checks=v.negative_checks(HERE/'run-f')
print(json.dumps({'diagnosis':'former TID retains gate; new leader openat queues behind vanished former TID','exact_exec':{'returncode':0,'seconds':r['captures']['exec']['seconds'],'transitions':result['transitions']},'final':final,'negative_checks':checks},indent=2))
