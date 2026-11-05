import copy
import json
import sys
import tempfile
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'scripts'))
import verify_matcher_measurement as verifier
root=Path('docs/measurements/OPS-025/run-a3').resolve()

def change_evidence(name, edit):
    return name,'identities.json',edit
checks=[
 ('missing_phase','report.json',lambda v:v['acknowledgements'].pop('live_first')),
 ('unknown_numeric_metric','postgres/report.json',lambda v:v['phases']['work_accept']['operation_delta'].__setitem__('wal_insert_bytes',None)),
 change_evidence('same_physical_offset',lambda v:v['records'][2].__setitem__('offset',v['records'][1]['offset'])),
 change_evidence('mixed_source_hash',lambda v:v.__setitem__('source_sha256','0'*64)),
 change_evidence('changed_after_image',lambda v:v['images'][0]['Change'].__setitem__('subscription_version',999)),
 change_evidence('false_snapshot_complete',lambda v:v['generation'].__setitem__('Complete',False)),
 change_evidence('changed_canonical_trigger',lambda v:v['work']['Entries'][0]['Result'].__setitem__('TriggerID','other')),
 change_evidence('changed_duplicate_diagnostic',lambda v:next(p for p in v['phases'] if p['phase_id']=='observe_duplicate')['result']['images'].clear()),
]
results={}
for name,filename,edit in checks:
 with tempfile.TemporaryDirectory(prefix='ops025-format-') as tmp:
  target=Path(tmp)
  for p in root.iterdir(): (target/p.name).symlink_to(p,target_is_directory=p.is_dir())
  parts=Path(filename).parts
  parent=target
  for part in parts[:-1]:
   link=parent/part
   if link.is_symlink():
    source=link.resolve();link.unlink();link.mkdir()
    for p in source.iterdir(): (link/p.name).symlink_to(p,target_is_directory=p.is_dir())
   parent=link
  file=target/filename
  data=json.loads(file.read_text());edit(data);file.unlink();file.write_text(json.dumps(data))
  try:verifier.verify(target)
  except (AssertionError,KeyError,TypeError,ValueError):results[name]='rejected'
  else:raise AssertionError(name+' accepted')
Path('docs/measurements/OPS-025/format-check.json').write_text(json.dumps(results,indent=2)+'\n')
print(json.dumps(results,indent=2))
