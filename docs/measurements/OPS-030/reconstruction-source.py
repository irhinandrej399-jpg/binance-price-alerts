"""Research prototype only. Not a collector or proof of trace completeness."""
import gzip,json,re,sys
from decimal import Decimal
from pathlib import Path


def ns(s):
    return int(Decimal(s)*10**9)


def calls(raw):
    pending={}; result=[]
    for n,line in enumerate(raw.splitlines(),1):
        m=re.fullmatch(r'(\d+)\s+(\d+\.\d+)\s+(.*)',line)
        if not m: raise ValueError(f'line {n}: unknown syntax')
        tid,stamp,body=int(m[1]),ns(m[2]),m[3]
        if body.startswith('--- '): continue
        if body.startswith('+++ '):
            if body != '+++ exited with 0 +++': raise ValueError('abnormal process termination')
            continue
        resumed_stamp=stamp
        if '<unfinished ...>' in body:
            if tid in pending: raise ValueError('duplicate unfinished syscall')
            pending[tid]=(stamp,body.replace('<unfinished ...>',''),n)
            continue
        if body.startswith('<... '):
            r=re.fullmatch(r'<\.\.\. (\w+) resumed>(.*)',body)
            if not r or tid not in pending: raise ValueError('unpaired resume')
            stamp,head,n=pending.pop(tid)
            if not head.startswith(r[1]+'('): raise ValueError('resume mismatch')
            body=head+r[2]
        if re.fullmatch(r'exit(?:_group)?\(0\)\s+= \?',body):
            result.append(dict(tid=tid,start=stamp,end=stamp,op='exit_group',args='',ret=0,line=n)); continue
        m=re.fullmatch(r'(\w+)\((.*)\)\s+= (-?\d+|0x[0-9a-f]+).* <(\d+\.\d+)>',body)
        if not m: raise ValueError(f'line {n}: incomplete syscall {body}')
        result.append(dict(tid=tid,start=stamp-1000,end=max(stamp+ns(m[4]),resumed_stamp)+1000,op=m[1],args=m[2].strip(),ret=int(m[3],0) if m[3].startswith('0x') else int(m[3]),line=n))
    if pending: raise ValueError('incomplete raw')
    return sorted(result,key=lambda c:(c['start'],c['line']))


def reconstruct(cs):
    # Deliberately narrow: private ordinary files, pwrite64, no inheritance of
    # live work files. No inode identity asserted from pathname alone.
    fds={}; names={}; files=[]; events=[]; issues=[]
    def issue(c,s):
        issues.append({'line':c['line'],'reason':s})
    def event(c,f,kind,size=0):
        events.append(dict(file=f,kind=kind,size=size,start=c['start'],end=c['end'],line=c['line']))
    for c in cs:
        op,a,ret,tid=c['op'],c['args'],c['ret'],c['tid']
        table=fds.setdefault(tid,{})
        if op=='clone' and ret>0:
            if table: issue(c,'fork/clone with live work descriptors unsupported')
        if ret<0: continue
        if op=='openat':
            m=re.match(r'AT_FDCWD<([^>]+)>, "([^"]+)", (.*)',a)
            if 'pgsql_tmp' not in a: continue
            if not m: issue(c,'unresolved work path'); continue
            root,path,flags=m.groups()
            if 'O_DIRECTORY' in flags: continue
            if not re.fullmatch(r'base/pgsql_tmp/pgsql_tmp\d+\.\d+',path): issue(c,'shared/custom/unknown work path'); continue
            if 'O_CREAT|O_TRUNC' not in flags or path in names: issue(c,'reopen/shared file unsupported'); continue
            f=len(files); files.append({'path':root+'/'+path,'creator':tid,'open_line':c['line']})
            names[path]=f
            if ret in table: issue(c,'descriptor reused without close')
            table[ret]=f
        elif op=='pwrite64':
            m=re.fullmatch(r'(\d+)(?:<[^>]+>)?, .*, (\d+), (\d+)',a)
            if not m: issue(c,'unparsed pwrite64'); continue
            fd,count,offset=map(int,m.groups())
            if fd in table:
                if ret>count: issue(c,'invalid write result')
                if ret: event(c,table[fd],'grow',offset+ret)
        elif op=='close':
            m=re.match(r'(\d+)',a)
            if not m: issue(c,'unparsed close'); continue
            f=table.pop(int(m[1]),None)
            if f is not None and f not in names.values() and not any(f in t.values() for t in fds.values()): event(c,f,'free')
        elif op=='unlinkat':
            m=re.fullmatch(r'AT_FDCWD<[^>]+>, "([^"]+)", 0',a)
            if 'pgsql_tmp' not in a: continue
            if not m or m[1] not in names: issue(c,'unresolved unlink'); continue
            f=names.pop(m[1])
            if not any(f in t.values() for t in fds.values()): event(c,f,'free')
        elif op=='exit_group':
            if table: issue(c,'exit with live work descriptors unsupported')
        elif op=='lseek':
            pass # positional writes do not depend on this offset
        elif op in ('mmap','munmap'):
            if op=='mmap' and 'MAP_SHARED' in a and 'PROT_WRITE' in a and 'MAP_ANONYMOUS' not in a:
                issue(c,'shared writable mapping requires identity proof')
        elif op not in ('clone','openat','close','pwrite64','unlinkat','exit_group'):
            issue(c,'unsupported operation '+op)
    if names or any(fds.values()): issues.append({'line':None,'reason':'work lifetimes incomplete'})
    # Check lifecycle intervals cannot overlap another operation on that file.
    byfile={f:[] for f in range(len(files))}
    for e in events: byfile[e['file']].append(e)
    for f,es in byfile.items():
        for e in es:
            if e['kind']=='free' and any(o is not e and o['start']<=e['end'] and e['start']<=o['end'] for o in es):
                issues.append({'line':e['line'],'reason':'overlapping free/write'})
    def peak(upper):
        points=[]
        for e in events:
            # Grow may happen anywhere inside its syscall. Releasing an
            # unlinked/closed inode is conservatively early for lower bound.
            stamp=e['start'] if (e['kind']=='grow')==upper else e['end']
            # At equal timestamps: upper applies growth before free, lower
            # free before growth; microsecond timestamp precision is retained.
            priority=(0 if e['kind']=='grow' else 1) if upper else (0 if e['kind']=='free' else 1)
            points.append((stamp,priority,e['line'],e))
        sizes={}; live=0; maximum=0; witness=None
        for stamp,_,_,e in sorted(points):
            f=e['file']; before=sizes.get(f,0)
            after=max(before,e['size']) if e['kind']=='grow' else 0
            sizes[f]=after; live+=after-before
            if live>maximum: maximum=live; witness=stamp
        return maximum,witness
    lo,lot=peak(False); hi,hit=peak(True)
    return {'status':'conditional-reconstruction' if not issues else 'unknown',
        'qualification_blocked':True,'temp_peak_live_bytes':None,
        'conditional_lower_bytes':lo,'conditional_upper_bytes':hi,
        'bounds_equal':lo==hi,'lower_witness_ns':lot,'upper_witness_ns':hit,
        'files':files,'events':events,'issues':issues,
        'timestamp_uncertainty':'entry minus 1us; exit at max(entry + elapsed, resume timestamp) plus 1us; no realtime clock-step guarantee',
        'scope':'ordinary private zero-created files with positional writes and nonoverlapping release; trace completeness and inode identity NOT proved'}

if __name__=='__main__':
    source=Path(sys.argv[1]); raw=gzip.open(source,'rt').read() if source.suffix=='.gz' else source.read_text()
    result=reconstruct(calls(raw))
    Path(sys.argv[2]).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('events','files','issues')},indent=2)); print('issues:',len(result['issues']),result['issues'][:8])
