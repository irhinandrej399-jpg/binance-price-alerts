"""Verify frozen research evidence. Does NOT qualify a peak observer."""
import argparse
import collections
import gzip
import hashlib
import json
from pathlib import Path
import runpy

root = Path(__file__).resolve().parent
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--record', action='store_true')
args = p.parse_args()
b = runpy.run_path(str(root/'reconstruction-source.py'))
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
pre = json.loads((root/'preflight/report.json').read_text())
for path, digest in pre['source_sha256'].items():
    assert sha(root/path) == digest
for path, digest in pre['raw_sha256'].items():
    assert sha(root/'preflight'/path) == digest
raw = gzip.open(root/'preflight/syscalls.trace.gz','rt').read()
cs = b['calls'](raw)
result = b['reconstruct'](cs)
saved = json.load(gzip.open(root/'reconstruction/conditional.json.gz','rt'))
assert result == saved
held = sum(f['size'] for f in pre['held']['files'])
assert result['conditional_lower_bytes'] == result['conditional_upper_bytes'] == held == 34792064
assert len(result['files']) == 4
assert result['status'] == 'unknown' and result['temp_peak_live_bytes'] is None
assert result['qualification_blocked'] is True
assert all(i['reason'] == 'shared writable mapping requires identity proof' for i in result['issues'])
assert pre['before']['files'] == pre['after']['files'] == []
assert pre['after']['stats']['temp_bytes']-pre['before']['stats']['temp_bytes'] == held
checks = {'replay_matches_saved': True, 'conditional_bounds_match_independent_held': True,
          'peak_remains_unknown': True}
# Mutate actual raw without inventing an artificial successful infrastructure run.
unlink = next(c for c in cs if c['op']=='unlinkat' and c['ret']==0)
changed = b['reconstruct']([c for c in cs if c is not unlink])
assert any(i['reason']=='work lifetimes incomplete' for i in changed['issues'])
checks['missing_unlink_rejected'] = True
lines = raw.splitlines()
first_resume = next(i for i,l in enumerate(lines) if '<... pwrite64 resumed>' in l)
try:
    b['calls']('\n'.join(lines[:first_resume] + lines[first_resume+1:]))
except ValueError:
    checks['missing_resume_rejected'] = True
else:
    raise AssertionError('missing resume accepted')
killed = raw + '\n999 1793892598.000000 +++ killed by SIGKILL +++\n'
try:
    b['calls'](killed)
except ValueError:
    checks['killed_process_rejected'] = True
else:
    raise AssertionError('killed process accepted')
control = json.loads((root/'lifecycle/report.json').read_text())
assert control['status'] == 'filesystem-control-passed-not-pg-observer'
assert control['temp_peak_live_bytes'] is None and control['qualification_blocked']
assert all(not values for values in control['cleanup'].values())
for path,digest in control['sha256'].items():
    assert sha(root/path)==digest
rows = [l.split() for l in (root/'lifecycle/control.txt').read_text().splitlines()]
assert len({tuple(r[1:3]) for r in rows[:-1]}) == 1
assert [int(r[3]) for r in rows[:-1]] == [16384,16384,16384,16384,4096,12288]
assert [int(r[4]) for r in rows[:-1]] == [1,2,1,0,0,0]
life = b['reconstruct'](b['calls'](gzip.open(root/'lifecycle/syscalls.trace.gz','rt').read()))
assert life['status']=='unknown' and life['qualification_blocked']
reasons = collections.Counter(i['reason'] for i in life['issues'])
assert reasons['unsupported operation ftruncate'] == 2
assert reasons['unsupported operation linkat'] == 1
assert reasons['unsupported operation dup3'] > 0
checks['real_alias_unlink_open_shrink_regrow_evidence'] = True
checks['prototype_rejects_lifecycle_control'] = True
inputs = ['preflight/report.json','preflight/syscalls.trace.gz','preflight-source.py',
          'reconstruction-source.py','reconstruction/conditional.json.gz',
          'lifecycle-probe.py','lifecycle/report.json','lifecycle/control.txt',
          'lifecycle/syscalls.trace.gz','verify-research.py']
report = {'status':'research-checks-passed-not-observer-qualification',
          'qualification_blocked':True, 'checks':checks,
          'conditional_bounds_bytes':[held,held],
          'lifecycle_rejection_reasons':dict(sorted(reasons.items())),
          'sha256':{path:sha(root/path) for path in inputs}}
target = root/'reconstruction/checks.json'
if args.record:
    target.write_text(json.dumps(report,indent=2)+'\n')
else:
    assert json.loads(target.read_text())==report
print(report['status'],len(checks),'checks')
