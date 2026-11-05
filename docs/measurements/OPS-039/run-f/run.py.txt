"""Disposable OPS-039 filesystem control. No PG or working volumes."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import uuid

parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('directory',type=Path);args=parser.parse_args()
out=args.directory;out.mkdir(parents=True,exist_ok=False)
image='postgres:18.6-bookworm@sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc'
project='binance-ops039-'+uuid.uuid4().hex[:10];label='com.docker.compose.project='+project
report={'project':project,'image':image,'commands':[],'status':'failed','temp_peak_live_bytes':None,'lower_bytes':None,'upper_bytes':None,'qualification_blocked':True}
def run(cmd,check=True,timeout=60):
 report['commands'].append(cmd)
 p=subprocess.run(cmd,capture_output=True,text=True,timeout=timeout)
 if check and p.returncode: raise RuntimeError((cmd,p.returncode,p.stderr))
 return p
try:
 for source,target in [('ancillary/main_linux.go','main_linux.go.txt'),('ancillary/message_linux.go','message_linux.go.txt'),('ancillary/inventory_linux.go','inventory_linux.go.txt'),('lifecycle/control/main_linux.go','control-main_linux.go.txt'),('lifecycle/control/clone_linux_arm64.s','clone_linux_arm64.s.txt'),('ancillary/lifecycle_linux.go','lifecycle_linux.go.txt'),('lifecycle/run.py','run.py.txt'),('lifecycle/verify.py','verify.py.txt'),('inventory/verify.py','inventory-verify.py.txt'),('ancillary/verify.py','ancillary-verify.py.txt')]:
  shutil.copyfile(Path('infra/postgres/measure')/source,out/target)
 env=['env','GOCACHE=/tmp/binance-ops038-gocache','GOMODCACHE=/tmp/binance-ops039-modcache','GOOS=linux','GOARCH=arm64','CGO_ENABLED=0','GOTOOLCHAIN=local']
 for binary,source in [('capture','./infra/postgres/measure/ancillary'),('control','./infra/postgres/measure/lifecycle/control')]:
  run(env+['go','build','-o',str(out/binary),source])
 report['binary_sha256']={n:hashlib.sha256((out/n).read_bytes()).hexdigest() for n in ('capture','control')}
 run(['docker','run','-d','--name',project,'--label',label,'--network','none','--cpus','1','--memory','512m','--cap-add','SYS_PTRACE','--security-opt','seccomp=unconfined','--tmpfs','/probe:rw,size=16m','--entrypoint','sleep',image,'infinity'])
 report['kernel']=run(['docker','exec',project,'uname','-a']).stdout.strip()
 for n in ('capture','control'): run(['docker','cp',str(out/n),project+':/tmp/'+n])
 settings=['-e','OPS038_INVENTORY=1','-e','OPS039_LIFECYCLE=1','-e','OPS039_EARLY_CHILD=1','-e','OPS038_PATH_SCOPE=/probe','-e','OPS038_READ_PROBE=/proc/2147483647/status','-e','OPS038_PG_WITNESS=/probe/pg-witness-not-provided']
 for mode in ('references','exec'):
  p=run(['docker','exec',*settings,project,'/tmp/capture','/probe/work','/tmp/'+mode+'.gz','--','/tmp/control',mode],False,timeout=60)
  (out/(mode+'.stdout')).write_text(p.stdout);(out/(mode+'.stderr')).write_text(p.stderr)
  report[mode+'_returncode']=p.returncode
  run(['docker','cp',project+':/tmp/'+mode+'.gz',str(out/(mode+'.jsonl.gz'))])
  if p.returncode: raise RuntimeError('capture failed '+mode)
 report['status']='captured-not-qualified'
finally:
 run(['docker','rm','-f',project],False)
 report['cleanup']={kind:run(['docker',kind,'ls',*(['-a'] if kind=='container' else []),'-q','--filter','label='+label]).stdout.splitlines() for kind in ('container','volume','network')}
 # Preserve source and binary hashes, keep binaries out of the repository.
 for n in ('capture','control'):
  if (out/n).exists(): (out/n).unlink()
 report['sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir() if p.is_file() and p.name!='report.json'}
 (out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'status':report['status'],'directory':str(out),'cleanup':report['cleanup']}))
