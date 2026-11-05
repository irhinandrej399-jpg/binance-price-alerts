"""Replay OPS-033 ancillary evidence. This never qualifies an inode peak."""
import argparse
from collections import Counter
import copy
import gzip
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import shutil

CALLS = {211, 212, 243, 269}
RECV = {212, 243}
MULTI = {243, 269}
VERSION = 'linux-arm64-ancillary-v1'


def require(value, reason):
    if not value:
        raise ValueError(reason)


def mem(m, address, length):
    require(set(m) == {'address', 'length', 'hex', 'error'}, 'memory fields missing')
    require(m['address'] == address and m['length'] == length, 'memory address/length mismatch')
    b = bytes.fromhex(m['hex'])
    require(len(b) <= length and (m['error'] or len(b) == length), 'short memory without error')
    return None if m['error'] else b


def header(m, address, stride):
    b = mem(m['header'], address, stride)
    if b is None:
        return None
    return {'ptr': struct.unpack_from('<Q', b, 32)[0],
            'length': struct.unpack_from('<Q', b, 40)[0],
            'flags': struct.unpack_from('<I', b, 48)[0],
            'msg_len': struct.unpack_from('<I', b, 56)[0] if stride == 64 else None}


def decode(row, entry, output):
    nr, args = entry['nr'], entry['args']
    data = row['messages']  # Mandatory even for errors and zero completions.
    require(data['version'] == VERSION, 'ancillary version')
    ret = row.get('ret', 0)
    count = args[2] if nr in MULTI else 1
    if output:
        count = 0 if ret < 0 else (ret if nr in MULTI else 1)
        require(ret < 0 or nr not in MULTI or ret <= args[2], 'too many completions')
    if count > 1024 or (count > 0 and args[1]+(count-1)*(64 if nr in MULTI else 56) >= 1 << 64):
        require(data['error'] and not data['items'], 'unbounded vector silently omitted')
        return [], ['vector incomplete']
    require(not data['error'] and len(data['items']) == count, 'missing message entries')
    stride = 64 if nr in MULTI else 56
    rights, issues = [], []
    for i, m in enumerate(data['items']):
        require(set(m) == {'header', 'control', 'rights', 'error'}, 'message fields missing')
        h = header(m, args[1] + i * stride, stride)
        if h is None:
            require(m['control'] is None and not m['rights'], 'decoded unreadable header')
            issues.append('header unreadable')
            continue
        if output and nr in RECV:
            before = header(entry['messages']['items'][i], args[1] + i * stride, stride)
            require(before is not None, 'successful receive missing input capacity')
            require(h['ptr'] == before['ptr'] and h['length'] <= before['length'], 'receive exceeds input capacity')
            if h['flags'] & 8:
                issues.append('MSG_CTRUNC: discarded references unresolved')
        if (nr in RECV) != output:
            require(m['control'] is None and not m['rights'] and not m['error'], 'stale control decoded')
            continue
        require(m['control'] is not None, 'missing mandatory control bytes')
        b = mem(m['control'], h['ptr'], h['length'])
        if b is None:
            require(m['error'] == 'control unreadable' and not m['rights'], 'unreadable control silently accepted')
            issues.append(m['error'])
            continue
        fds, expected_error = [], ''
        off = 0
        while off + 16 <= len(b):
            length, level, typ = struct.unpack_from('<QII', b, off)
            if length < 16 or length > len(b)-off:
                expected_error = 'invalid cmsghdr length'
                break
            if (level, typ) == (1, 1):
                if (length-16) % 4:
                    expected_error = 'invalid rights length'
                    break
                fds.extend(struct.unpack_from('<i', b, j)[0] for j in range(off+16, off+length, 4))
            else:
                expected_error = 'ancillary type has unresolved reference semantics'
            off += (length+7) & ~7
        require(m['error'] == expected_error, 'ancillary decode status mismatch')
        if expected_error:
            issues.append(expected_error)
        require([r['fd'] for r in m['rights']] == fds, 'missing/substituted FD evidence')
        for r in m['rights']:
            require(set(r) == {'fd', 'stat', 'error'}, 'FD evidence fields missing')
            if r['error']:
                require(r['stat'] is None, 'stat/error conflict')
                issues.append('FD stat unreadable')
                continue
            s = r['stat']
            require(s is not None and s.get('fd', 0) == r['fd'] and s['pid'] == row['pid'], 'FD stat binding')
            require(all(k in s for k in ('dev', 'ino', 'size', 'nlink', 'mode', 'path')), 'missing stat fields')
            require(s['dev'] >= 0 and s['ino'] > 0 and s['size'] >= 0 and s['nlink'] >= 0, 'invalid stat')
            rights.append(s)
    return rights, issues


def load(path):
    with gzip.open(path, 'rt') as f:
        return [json.loads(line) for line in f]


def replay(rows):
    require(rows[0]['kind'] == 'header' and rows[0]['schema'] == 'ops033-ptrace-ancillary-v1'
            and rows[0]['arch'] == 'arm64', 'missing OPS-033 arm64 schema')
    require(rows[-1]['kind'] == 'footer', 'incomplete capture')
    pending, alive, journal, counts, evidence, issues = {}, set(), set(), Counter(), [], []
    clock, active = -1, None
    root = None
    for seq, row in enumerate(rows, 1):
        require(row['seq'] == seq and row['ns'] >= clock, 'sequence/clock')
        clock = row['ns']
        kind, pid = row['kind'], row.get('pid')
        if kind == 'root':
            require(root is None, 'duplicate root')
            root = pid
            alive.add(pid)
        elif kind == 'event':
            require(pid in alive, 'missing parent')
            if row['event'] in (1, 2, 3):
                require(row['message'] not in alive, 'duplicate child')
                alive.add(row['message'])
        elif kind == 'enter':
            require(pid in alive and pid not in pending, 'unmatched entry')
            pending[pid] = row
            if row['nr'] in CALLS:
                rs, bad = decode(row, row, False)
                counts[str(row['nr']) + '-entry'] += 1
                issues.extend(bad)
                evidence.extend(dict(s, seq=seq, phase='input', nr=row['nr']) for s in rs)
            else:
                require('messages' not in row, 'unexpected message evidence')
        elif kind == 'leave':
            entry = pending.pop(pid, None)
            require(entry is not None and entry['nr'] == row['nr'], 'unmatched return')
            if row['nr'] in CALLS:
                rs, bad = decode(row, entry, True)
                counts[str(row['nr']) + '-return'] += 1
                counts['restart' if row['ret'] in (-512,-513,-514,-516) else 'error' if row['ret'] < 0 else 'success'] += 1
                issues.extend(bad)
                evidence.extend(dict(s, seq=seq, phase='output', nr=row['nr']) for s in rs)
        elif kind == 'exit':
            require(pid in alive, 'unregistered exit')
            alive.remove(pid)
            entry = pending.pop(pid, None)
            if entry and entry['nr'] in CALLS:
                issues.append('message syscall did not return')
        elif kind == 'dispatch':
            require(active is None and pid in pending, 'overlapping dispatch')
            active = pid
        elif kind == 'release':
            require(active == pid and pid not in pending, 'unmatched release')
            active = None
        elif kind == 'unsupported':
            journal.add(row['reason'])
    f = rows[-1]
    require(not alive and not pending and active is None and root is not None, 'incomplete lifecycle')
    require(f['status'] == 'unknown' and f['temp_peak_live_bytes'] is None and f['qualification_blocked'], 'false qualification')
    require(set(f['issues']) == journal, 'issue journal mismatch')
    require('complete syscall classification and monotonic-per-operation proof pending' in journal, 'missing classifier obligation')
    if counts:
        require('socket queue reference history pending OPS-034' in journal, 'missing queue obligation')
    return {'counts': dict(counts), 'issues': sorted(set(issues)), 'rights': evidence,
            'temp_peak_live_bytes': None, 'lower_bytes': None, 'upper_bytes': None, 'qualification_blocked': True}


def verify(directory):
    report = json.loads((directory/'report.json').read_text())
    require(report['status'] == 'ancillary-captured-not-qualified', 'run failed')
    require(all(not v for v in report['cleanup'].values()), 'resources remain')
    require(report['temp_peak_live_bytes'] is None and report['qualification_blocked'], 'false report qualification')
    required = {'filesystem.jsonl.gz', 'postgres.jsonl.gz', 'filesystem.txt', 'parallel-plan.txt',
                'main_linux.go.txt', 'message_linux.go.txt', 'control-main_linux.go.txt', 'gate-source.py.txt'}
    require(required <= report['sha256'].keys(), 'missing required hashes')
    for name, digest in report['sha256'].items():
        require(hashlib.sha256((directory/name).read_bytes()).hexdigest() == digest, 'hash mismatch '+name)
    require(report['source_sha256'] == report['sha256']['main_linux.go.txt'], 'source hash mismatch')
    require(report['settings']['io_method'] == 'sync' and all(report['settings'][k]=='on' for k in
            ('fsync','synchronous_commit','full_page_writes')), 'PG profile')
    require(all(not t['location'] for t in report['tablespaces']), 'custom tablespace')
    results = {name: replay(load(directory/(name+'.jsonl.gz'))) for name in ('filesystem','postgres')}
    control = [json.loads(line) for line in (directory/'filesystem.txt').read_text().splitlines()]
    require(control[0]['abi'] == [56,64,16] and control[-1]['closed'], 'ABI/closure')
    sources = control[0]['source']
    cross = int(control[0].get('cross_process', False))
    require([s['size'] for s in sources] == [4096,8192] and [s['nlink'] for s in sources] == [1,0], 'independent source control')
    def identity(s): return tuple(s[k] for k in ('dev','ino','size','nlink','mode'))
    require(all(identity(r) in {identity(s) for s in sources} for r in results['filesystem']['rights']), 'independent sender metadata mismatch')
    for s in sources:
        require(all(any(identity(r)==identity(s) and r['phase']==phase for r in results['filesystem']['rights'])
                    for phase in ('input','output')), 'source/receiver identity absent')
    require(Counter((identity(r),r['fd']) for r in results['filesystem']['rights'] if r['phase']=='input') == Counter({(identity(sources[0]),sources[0]['fd']):3, (identity(sources[1]),sources[1]['fd']):7+cross}), 'missing/substituted sender evidence')
    # Every independent received FD must occur in the raw output evidence.
    received = [r for c in control[1:-1] for m in c['messages'] for r in m['rights']]
    require(Counter((identity(s),s['fd']) for s in received) == Counter((identity(s),s['fd']) for s in results['filesystem']['rights'] if s['phase']=='output'), 'independent receive metadata mismatch')
    fs_rows = load(directory/'filesystem.jsonl.gz')
    capacities = Counter()
    outcomes = Counter()
    for row in fs_rows:
        if row['kind']=='enter' and row['nr'] in RECV:
            stride = 64 if row['nr'] in MULTI else 56
            hs = [header(m,row['args'][1]+i*stride,stride) for i,m in enumerate(row['messages']['items'])]
            require(all(h is not None for h in hs), 'receiver input header missing')
            capacities[(row['nr'],tuple(h['length'] for h in hs))] += 1
        if row['kind']=='leave' and row['nr'] in RECV and row['ret'] >= 0:
            stride = 64 if row['nr'] in MULTI else 56
            vals=[]
            for m in row['messages']['items']:
                b=bytes.fromhex(m['header']['hex'])
                vals.append((struct.unpack_from('<Q',b,40)[0],struct.unpack_from('<I',b,48)[0],
                             struct.unpack_from('<I',b,56)[0] if stride==64 else 0))
            outcomes[(row['nr'],row['ret'],tuple(vals))] += 1
    require(capacities == Counter({(212,(64,)):6+cross,(212,(20,)):1,(212,(0,)):1,(243,(64,64)):3}), 'independent receive capacities mismatch')
    expected_outcomes=Counter((c['nr'],c['ret'],tuple((m['controllen'],m['flags'],m['length']) for m in c['messages']))
                             for c in control[1:-1] if c['nr'] in RECV and not c['errno'])
    require(outcomes==expected_outcomes, 'independent receive output mismatch')
    if cross:
        require(next(c for c in control[1:-1] if c['tag']=='cross-process' and c['nr']==212)['pid'] != control[0]['pid'], 'receiver not independent process')
    by_tag = {(c['tag'],c['nr']): c for c in control[1:-1]}
    for tag in ('ordinary','unlink','multiple','no-control','truncated','zero-capacity'):
        require(by_tag[tag,211]['errno']==by_tag[tag,212]['errno']==0, 'round trip failed '+tag)
    require(len(by_tag['multiple',212]['messages'][0]['rights'])==2, 'multiple FD control')
    require(by_tag['truncated',212]['messages'][0]['flags'] & 8 and len(by_tag['truncated',212]['messages'][0]['rights'])==1, 'MSG_CTRUNC control')
    require(by_tag['zero-capacity',212]['messages'][0]['flags'] & 8, 'zero capacity control')
    require(by_tag['partial-send',269]['ret']==by_tag['partial-recv',243]['ret']==1, 'partial mmsg control')
    require(by_tag['full-send',269]['ret']==by_tag['full-recv',243]['ret']==2, 'full mmsg control')
    require(all(by_tag[t,n]['errno'] for t,n in [('bad-socket',211),('bad-header',211),('bad-right',211),('empty-recv',243)]), 'failed syscall controls')
    require(results['filesystem']['counts'].get('restart',0)>0, 'real syscall restart absent')
    require('header unreadable' in results['filesystem']['issues'], 'memory failure control absent')
    require(results['postgres']['counts'].get('212-entry',0)>0 and not results['postgres']['issues'], 'PG message decode incomplete')
    pg = load(directory/'postgres.jsonl.gz')
    files = [f for r in pg if r['kind']=='snapshot' for f in r['files'] or []]
    require(any('.fileset/' in f['path'] for f in files) and any('.fileset' not in f['path'] for f in files), 'ordinary/shared spill absent')
    plan = (directory/'parallel-plan.txt').read_text()
    require('"Workers Launched": 2' in plan, 'parallel workers absent')
    return results


def negative_checks(directory):
    checks=[]
    # Invoke the full verifier after rewriting both raw and the saved SHA-256.
    # The untouched independent helper output must still bind FD metadata.
    original=load(directory/'filesystem.jsonl.gz')
    def reject(name, change):
        with tempfile.TemporaryDirectory(prefix='ops033-replay-') as tmp:
            dest=Path(tmp)/'data';shutil.copytree(directory,dest)
            rows=copy.deepcopy(original);change(rows)
            for i,r in enumerate(rows,1):r['seq']=i
            raw=dest/'filesystem.jsonl.gz'
            with gzip.open(raw,'wt') as f:
                for r in rows:f.write(json.dumps(r)+'\n')
            report=json.loads((dest/'report.json').read_text());report['sha256'][raw.name]=hashlib.sha256(raw.read_bytes()).hexdigest()
            (dest/'report.json').write_text(json.dumps(report))
            try:verify(dest)
            except (ValueError,KeyError,IndexError,TypeError): checks.append(name)
            else:raise ValueError('tampered evidence accepted: '+name)
    def message(rows,phase='enter'):
        return next(r for r in rows if r['kind']==phase and r['nr'] in CALLS and r['messages']['items'])
    reject('missing-message-object-rehashed', lambda rows: message(rows).pop('messages'))
    reject('missing-header-rehashed',lambda rows:message(rows)['messages']['items'][0].pop('header'))
    def control(rows):
        return next(m for r in rows if r['kind']=='enter' and r['nr']==211 for m in r['messages']['items'] if m['rights'])
    reject('missing-control-bytes-rehashed',lambda rows:control(rows).pop('control'))
    reject('missing-right-stat-rehashed',lambda rows:control(rows)['rights'][0].pop('stat'))
    reject('substituted-inode-rehashed',lambda rows:control(rows)['rights'][0]['stat'].__setitem__('ino',1))
    def truncated(rows):
        return next(m for r in rows if r['kind']=='leave' and r['nr']==212 for m in r['messages']['items'] if bytes.fromhex(m['header']['hex'])[48] & 8)
    def clear_truncation(rows):
        m=truncated(rows);b=bytearray.fromhex(m['header']['hex']);struct.pack_into('<I',b,48,0);m['header']['hex']=b.hex()
    reject('removed-MSG_CTRUNC-rehashed',clear_truncation)
    def change_capacity(rows):
        m=next(r for r in rows if r['kind']=='enter' and r['nr']==212)['messages']['items'][0]
        b=bytearray.fromhex(m['header']['hex']);struct.pack_into('<Q',b,40,128);m['header']['hex']=b.hex()
    reject('substituted-input-capacity-rehashed',change_capacity)
    reject('missing-output-rehashed',lambda rows:message(rows,'leave')['messages'].__setitem__('items',[]))
    reject('missing-message-entry-rehashed',lambda rows:rows.remove(message(rows)))
    reject('false-peak-rehashed',lambda rows:rows[-1].__setitem__('temp_peak_live_bytes',0))
    return checks


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);p.add_argument('--record',action='store_true');args=p.parse_args()
    result={'results':verify(args.directory),'negative_checks':negative_checks(args.directory)}
    if args.record:(args.directory/'checks.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
