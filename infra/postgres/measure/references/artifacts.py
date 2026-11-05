"""Lossless chunk transport and disk-backed JSONL view for large PG evidence."""
import argparse
from contextlib import contextmanager
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

CHUNK = 32 * 1024 * 1024
ROOT = Path(__file__).resolve().parents[4]

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()

def seal(directory,report):
    """Append transport/verification metadata; never replace capture source/raw hashes."""
    original=directory/'postgres.jsonl.gz'
    if original.exists():
        expected=report.get('sha256',{}).get(original.name,digest(original))
        if digest(original)!=expected:raise ValueError('raw changed before packing')
        parts=[]
        with original.open('rb') as f:
            for i,block in enumerate(iter(lambda:f.read(CHUNK),b'')):
                name=original.name+f'.part-{i:03d}';(directory/name).write_bytes(block);parts.append(name)
        report['archives']={original.name:dict(encoding='concatenated-bytes',parts=parts,size=original.stat().st_size)}
        report.setdefault('sha256',{})[original.name]=expected
        original.unlink()
    dependencies={}
    for ticket,run_name in (('OPS-038','run-c'),('OPS-039','run-f')):
        rel=Path('docs/measurements')/ticket/run_name
        dependencies[ticket]=dict(directory=str(rel),report_sha256=digest(ROOT/rel/'report.json'))
    (directory/'dependencies.json').write_text(json.dumps(dependencies,indent=2)+'\n')
    for source,target in [('verify.py','full-verifier.py.txt'),('artifacts.py','artifact-transport.py.txt')]:
        shutil.copyfile(Path(__file__).parent/source,directory/target)
    # A post-capture verifier refinement has its own snapshot; the captured
    # inventory-verifier.py.txt is left byte-for-byte unchanged.
    shutil.copyfile(Path(__file__).parents[1]/'inventory/verify.py',directory/'full-inventory-verifier.py.txt')
    shutil.copyfile(Path(__file__).parents[1]/'lifecycle/verify.py',directory/'full-lifecycle-verifier.py.txt')
    report['post_capture_verification']='dependencies plus lossless PG chunks and full-* verifier snapshots; original capture sources preserved'
    report['sha256'].update({p.name:digest(p) for p in directory.iterdir() if p.is_file() and p.name not in ('report.json','checks.json')})

class DiskRows:
    """Indexed temporary JSONL; random lookup does not move an active iterator."""
    def __init__(self,raw):
        self.file=tempfile.TemporaryFile();self.offsets=[]
        with gzip.open(raw,'rb') as src:
            for line in src:
                self.offsets.append(self.file.tell());self.file.write(line)
        self.file.flush()
    def __len__(self):return len(self.offsets)
    def __getitem__(self,index):
        self.file.seek(self.offsets[index]);return json.loads(self.file.readline())
    def __iter__(self):
        # __getitem__ seeks before every read, allowing nested random lookups.
        for i in range(len(self)):yield self[i]
    def close(self):self.file.close()

@contextmanager
def postgres_rows(directory,report):
    with tempfile.TemporaryFile() as raw:
        info=report['archives']['postgres.jsonl.gz']
        if info['encoding']!='concatenated-bytes' or not info['parts']:raise ValueError('archive transport')
        if info['parts']!=[f'postgres.jsonl.gz.part-{i:03d}' for i in range(len(info['parts']))]:raise ValueError('archive chunk sequence')
        h=hashlib.sha256()
        for name in info['parts']:
            with (directory/name).open('rb') as src:
                for block in iter(lambda:src.read(1024*1024),b''):raw.write(block);h.update(block)
        if raw.tell()!=info['size'] or h.hexdigest()!=report['sha256']['postgres.jsonl.gz']:raise ValueError('raw archive size/hash')
        raw.seek(0);rows=DiskRows(raw)
        try:yield rows
        finally:rows.close()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);args=p.parse_args()
    report=json.loads((args.directory/'report.json').read_text());seal(args.directory,report)
    (args.directory/'report.json').write_text(json.dumps(report,indent=2)+'\n')
