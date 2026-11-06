"""Independent evidence audit for OPS-042; no lifetime reducer imports."""
import gzip
import hashlib
import json
from pathlib import Path
import stat
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / 'docs/measurements/OPS-041/run-c'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def key(s):
    return f"{s['dev']}:{s['ino']}"


def work(path):
    return '/base/pgsql_tmp/' in path or '/pgsql_tmp/' in path and path.startswith('/tmp/work/custom/')


def audit(rows, pg=False):
    entries, pending, births, seen, gates = {}, {}, [], {}, []
    next_gate = None
    for r in rows:
        kind = r['kind']
        if kind == 'enter':
            entries[r['pid']] = r
            if r['nr'] == 155:
                next_gate = r['seq']
        elif kind == 'leave':
            e = entries.pop(r['pid'], None)
            if e and e['nr'] in (56, 437) and r['ret'] >= 0:
                flags = e['args'][2] if e['nr'] == 56 else None
                if e['nr'] == 437:
                    how = e.get('lifecycle', {}).get('open_how', {})
                    if not how.get('error', True):
                        b = bytes.fromhex(how['hex'])
                        if len(b) >= 8:
                            flags = int.from_bytes(b[:8], 'little')
                if flags is not None and flags & 192 == 192:
                    pending[r['seq'] + 1] = (e, r)
        elif kind == 'inventory':
            local = []
            for owner in r['owners']:
                for fd in owner['fd_entries']:
                    s = fd.get('stat')
                    if not s or not stat.S_ISREG(s['mode']):
                        continue
                    local.append((owner, fd, s))
                    path = fd['link']['text']
                    if pg and work(path):
                        item = seen.setdefault(key(s), {'paths': set(), 'owners': set(), 'witnesses': {}})
                        # stat starttime field 22, comm can contain spaces.
                        # Independent census is the source for owner starttime.
                        threads = [t for p in r['census_before']['processes'] for t in p['threads'] if t['tid'] == owner['tid']]
                        assert len(threads) == 1
                        start = int(threads[0]['stat']['text'].rsplit(')', 1)[1].split()[19])
                        ident = (owner['tid'], start)
                        item['paths'].add(path); item['owners'].add(ident)
                        item['witnesses'].setdefault(str(owner['tid']), {'seq': r['seq'], 'starttime': start, 'path': path})
            if r['seq'] in pending:
                e, x = pending.pop(r['seq'])
                found = [s for o, f, s in local if o['tid'] == e['pid'] and f['path'].endswith('/'+str(x['ret'])) and (not pg or work(f['link']['text']))]
                if len(found) == 1:
                    births.append({'identity': key(found[0]), 'entry_seq': e['seq'], 'return_seq': x['seq'], 'stat_seq': r['seq']})
            if next_gate is not None:
                gates.append((next_gate, r))
                next_gate = None
    return seen, births, gates


def main():
    report = json.loads((SOURCE/'report.json').read_text())
    names = ['filesystem.txt', 'filesystem.jsonl.gz'] + report['archives']['postgres.jsonl.gz']['parts']
    for name in names:
        assert digest(SOURCE/name) == report['sha256'][name], name
    with tempfile.TemporaryFile() as packed:
        h = hashlib.sha256()
        for name in report['archives']['postgres.jsonl.gz']['parts']:
            b = (SOURCE/name).read_bytes(); packed.write(b); h.update(b)
        assert h.hexdigest() == report['sha256']['postgres.jsonl.gz']
        packed.seek(0)
        with gzip.open(packed, 'rt') as stream:
            pg, births, _ = audit((json.loads(l) for l in stream), pg=True)
    assert len(pg) == 17 and not births, (len(pg), births)
    special = pg['65025:208045']
    assert special['owners'] == {(195, 3996605), (234, 4001156)}
    assert {Path(p).name for p in special['paths']} == {'pgsql_tmp195.0', 'pgsql_tmp234.1'}
    with gzip.open(SOURCE/'filesystem.jsonl.gz', 'rt') as stream:
        _, fs_births, gates = audit(json.loads(l) for l in stream)
    points = [json.loads(l) for l in (SOURCE/'filesystem.txt').read_text().splitlines()]
    assert len(gates) == len(points) == 13 and len(fs_births) == 6
    stages = []
    for p, (gate, cp) in zip(points, gates):
        s = p['source']; identity = key(s)
        fd_views = sum(key(f['stat']) == identity for o in cp['owners'] for f in o['fd_entries'] if f.get('stat'))
        path_views = sum(key(f['stat']) == identity for f in cp['paths'] if f.get('stat'))
        mapping = [l for l in p['maps'].splitlines() if len(l.split()) >= 6 and int(l.split()[4]) == s['ino'] and '/tmp/work/reused' in l]
        assert bool(mapping) == p['mapped']
        raw_mapping = [m['line'] for o in cp['owners'] if o['tid'] == p['tid'] for m in o['map_entries'] if '/tmp/work/reused' in m['line']]
        assert raw_mapping == mapping
        held = bool(fd_views or path_views or mapping or p['queued_bytes'])
        tag = p['tag']
        if 'queue-only' in tag:
            assert p['queued_bytes'] == 1 and fd_views == path_views == 0 and not mapping
        if tag == 'mapping-only':
            assert mapping and fd_views == path_views == 0
        if tag.endswith(('discharged', 'unmapped')):
            assert not held
        stages.append(dict(tag=tag, identity=identity, gate_seq=gate, inventory_seq=cp['seq'], fd_views=fd_views, path_views=path_views, mapped=bool(mapping), queued_bytes=p['queued_bytes'], observation='held' if held else 'absent-in-views', last_reference='unknown'))
    reuses = []
    for prefix in ('receive', 'drop', 'mapping'):
        old = next(p for p in points if p['tag'] == prefix+'-opened')
        new = next(p for p in points if p['tag'] == prefix+'-reused')
        stage = next(s for s in stages if s['tag'] == prefix+'-reused')
        birth = max((b for b in fs_births if b['stat_seq'] < stage['inventory_seq']), key=lambda b:b['stat_seq'])
        assert key(old['source']) == key(new['source']) == birth['identity']
        assert old['source']['fd'] == new['source']['fd'] and new['source']['size'] == 4096
        reuses.append(dict(mode=prefix, identity=birth['identity'], birth=birth, conclusion='distinct-exclusive-creation; no exact retirement event'))
    # Semantic challenges use independent helper facts, not model-output equality.
    claims = []
    indistinguishable_fd_views = []
    for prefix in ('receive', 'drop'):
        held = next(s for s in stages if s['tag'] == prefix+'-queue-only')
        absent = next(s for s in stages if s['tag'] == prefix+'-discharged')
        projection = lambda s: (s['identity'], s['fd_views'], s['path_views'], s['mapped'])
        assert projection(held) == projection(absent)
        assert held['observation'] == 'held' and absent['observation'] == 'absent-in-views'
        indistinguishable_fd_views.append([held['tag'], absent['tag']])
    for s in stages:
        if s['mapped'] or s['queued_bytes']:
            assert s['fd_views'] == 0 and s['observation'] == 'held'
            claims.append(dict(claim='last FD absence ends '+s['tag'], rejected=True, witness=s['tag']))
    for r in reuses:
        claims.append(dict(claim='same FD/path/dev/ino implies same creation: '+r['mode'], rejected=True, witness=r['birth']))
    # Empty views admit both hidden-holder and no-holder histories: no exact end.
    for s in stages:
        if s['observation'] == 'absent-in-views':
            claims.append(dict(claim='absence certifies retirement: '+s['tag'], rejected=True, alternatives=['no reference', 'outside census or unobserved kernel reference']))
    output = dict(schema='ops042-proof-audit-v1', source_report_sha256=digest(SOURCE/'report.json'), input_hashes={n:digest(SOURCE/n) for n in names}, pg_candidates=len(pg), successful_exclusive_open_adjacent_bindings=births, repeated_numeric_identity={k:sorted(v) if isinstance(v,set) else v for k,v in special.items()}, fs_creations=fs_births, stages=stages, reuses=reuses, semantic_challenges=claims, indistinguishable_fd_views=indistinguishable_fd_views, supported=False, bounds=None)
    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
