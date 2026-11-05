"""Isolated filesystem semantics control, NOT a PostgreSQL peak observer."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parent
out = ROOT / 'lifecycle'
out.mkdir(exist_ok=False)
project = 'binance-ops030-lifecycle-' + uuid.uuid4().hex[:10]
image = 'binance-fsync-observer:ops029'
label = 'com.docker.compose.project=' + project
report = {'status': 'running', 'project': project, 'commands': [],
          'temp_peak_live_bytes': None, 'qualification_blocked': True}

def run(args, check=True, **kwargs):
    report['commands'].append(args)
    r = subprocess.run(args, capture_output=True, text=True, timeout=60, **kwargs)
    if check and r.returncode:
        raise RuntimeError((r.returncode, r.stderr))
    return r.stdout

helper = r'''set -eu
mkdir -p /tmp/lifecycle/base/pgsql_tmp
cd /tmp/lifecycle
f=base/pgsql_tmp/pgsql_tmp1.0
exec 3<>"$f"
dd if=/dev/zero bs=4096 count=4 >&3 2>/dev/null
printf 'grown '; stat -Lc '%d %i %s %h' "$f"
ln "$f" "$f.alias"
printf 'alias '; stat -Lc '%d %i %s %h' "$f.alias"
rm "$f"
printf 'linked_alias '; stat -Lc '%d %i %s %h' "$f.alias"
rm "$f.alias"
printf 'unlinked_open '; stat -Lc '%d %i %s %h' "/proc/$$/fd/3"
truncate -s 4096 "/proc/$$/fd/3"
printf 'shrunken '; stat -Lc '%d %i %s %h' "/proc/$$/fd/3"
truncate -s 12288 "/proc/$$/fd/3"
printf 'regrown '; stat -Lc '%d %i %s %h' "/proc/$$/fd/3"
exec 3>&-
test -z "$(ls -A base/pgsql_tmp)"
printf 'closed_empty\n'
'''
try:
    report['image_id'] = run(['docker','image','inspect','--format','{{.Id}}',image]).strip()
    run(['docker','run','-d','--name',project,'--label',label,'--network','none',
         '--cap-drop','ALL','--cap-add','SYS_PTRACE','--security-opt','seccomp=unconfined',
         '--entrypoint','sh',image,'-c','while :; do sleep 1; done'])
    run(['docker','exec','-i',project,'sh','-c','cat >/tmp/helper.sh'],input=helper)
    report['kernel'] = run(['docker','exec',project,'uname','-a']).strip()
    report['strace_version'] = run(['docker','exec',project,'strace','-V']).strip()
    controls = run(['docker','exec',project,'strace','-f','-yy','-ttt','-T','-s','64',
        '-e','trace=%file,%desc,%process,%memory','-o','/tmp/raw','sh','/tmp/helper.sh'])
    (out/'control.txt').write_text(controls)
    raw = run(['docker','exec',project,'cat','/tmp/raw'])
    with gzip.open(out/'syscalls.trace.gz','wt') as f:
        f.write(raw)
    rows = [line.split() for line in controls.splitlines()]
    assert [r[0] for r in rows] == ['grown','alias','linked_alias','unlinked_open','shrunken','regrown','closed_empty']
    assert len({tuple(r[1:3]) for r in rows[:-1]}) == 1, 'inode identity changed'
    assert [int(r[3]) for r in rows[:-1]] == [16384,16384,16384,16384,4096,12288]
    assert [int(r[4]) for r in rows[:-1]] == [1,2,1,0,0,0]
    report['control_assertions'] = 'PASS: same dev/inode, link counts, shrink/regrow while unlinked, final empty'
    report['known_sequential_helper_peak_apparent_bytes'] = 16384
    report['status'] = 'filesystem-control-passed-not-pg-observer'
finally:
    run(['docker','rm','-f',project],check=False)
    report['cleanup'] = {kind: run(['docker',kind,'ls',*(['-a'] if kind=='container' else []),'-q','--filter','label='+label]).splitlines() for kind in ('container','network','volume')}
    report['sha256'] = {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),out/'control.txt',out/'syscalls.trace.gz'] if p.exists()}
    (out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
print(report['status'])
