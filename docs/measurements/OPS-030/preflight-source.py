import gzip
import json
import subprocess
import time
import uuid
from pathlib import Path

out = Path('docs/measurements/OPS-030/preflight')
out.mkdir(exist_ok=False)
project = 'binance-ops030-preflight-' + uuid.uuid4().hex[:10]
pg, tracer, volume = project + '-pg', project + '-trace', project + '-data'
label = 'com.docker.compose.project=' + project
report = {'project': project, 'status': 'running', 'commands': []}

def run(args, check=True, **kwargs):
    report['commands'].append(args)
    r = subprocess.run(args, text=True, capture_output=True, timeout=120, **kwargs)
    if check and r.returncode:
        raise RuntimeError((r.returncode, args, r.stderr))
    return r.stdout

def sql(query):
    return run(['docker', 'exec', '-i', pg, 'psql', '-XAt', '-U', 'postgres', '-v', 'ON_ERROR_STOP=1'], input=query)

try:
    run(['docker', 'volume', 'create', '--label', label, volume])
    report['pg_image'] = run(['docker', 'image', 'inspect', '--format', '{{json .Id}}', 'postgres:18.6-bookworm']).strip()
    report['observer_image'] = run(['docker', 'image', 'inspect', '--format', '{{json .Id}}', 'binance-fsync-observer:ops029']).strip()
    run(['docker', 'run', '-d', '--name', pg, '--label', label, '--network', 'none', '--cpus', '1', '--memory', '1g', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '-v', volume + ':/var/lib/postgresql', 'postgres:18.6-bookworm@sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc', 'postgres', '-c', 'fsync=on', '-c', 'synchronous_commit=on', '-c', 'full_page_writes=on'])
    for _ in range(100):
        r = subprocess.run(['docker', 'exec', pg, 'pg_isready', '-U', 'postgres'], capture_output=True)
        if r.returncode == 0:
            # initdb temporary postmaster also answers pg_isready; require PID 1.
            if run(['docker','exec',pg,'cat','/proc/1/comm']).strip() == 'postgres':
                break
        time.sleep(.2)
    else:
        raise RuntimeError('postgres not ready')
    report['settings'] = sql("SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ('server_version','data_directory','fsync','synchronous_commit','full_page_writes','io_method','temp_tablespaces','work_mem');")
    report['tablespaces'] = sql('SELECT json_agg(t) FROM (SELECT oid,spcname,pg_tablespace_location(oid) AS location FROM pg_tablespace) t;')
    report['kernel'] = run(['docker','exec',pg,'uname','-a'])
    run(['docker', 'run', '-d', '--name', tracer, '--label', label, '--pid=container:' + pg, '--network', 'none', '--cap-drop', 'ALL', '--cap-add', 'SYS_PTRACE', '--cap-add', 'KILL', '--security-opt', 'seccomp=unconfined', '--entrypoint', 'sh', 'binance-fsync-observer:ops029', '-c', 'mkdir /trace; strace -V >/trace/version; args=""; for p in /proc/[0-9]*; do exe=$(readlink "$p/exe" || true); case "$exe" in */postgres) args="$args -p ${p##*/}";; esac; done; echo "$args" >/trace/attach; strace -f -yy -ttt -T -s 64 -e trace=openat,close,write,writev,pwrite64,pwritev,pwritev2,lseek,ftruncate,truncate,unlink,unlinkat,rename,renameat,renameat2,fallocate,mmap,munmap,msync,io_uring_setup,io_uring_enter,clone,clone3,fork,vfork,exit,exit_group -o /trace/raw $args 2>/trace/diagnostics & t=$!; echo "$t" >/trace/pid; wait "$t"; echo "$?" >/trace/exit; while :; do sleep 1; done'])
    time.sleep(1)
    report['attach'] = run(['docker','exec',tracer,'cat','/trace/attach'])
    report['traced_processes'] = run(['docker','exec',tracer,'sh','-c','for p in /proc/[0-9]*; do exe=$(readlink "$p/exe" || true); case "$exe" in */postgres) echo "$p $exe"; awk \'/^Pid:|^TracerPid:/ {print}\' "$p/status";; esac; done'])
    report['before'] = sql("SELECT json_build_object('files',(SELECT coalesce(json_agg(t),'[]') FROM pg_ls_tmpdir() t),'stats',(SELECT row_to_json(s) FROM (SELECT temp_files,temp_bytes FROM pg_stat_database WHERE datname=current_database()) s));")
    query = "SET work_mem='64kB'; SET max_parallel_workers_per_gather=0; BEGIN; DECLARE c NO SCROLL CURSOR FOR SELECT i, repeat(md5(i::text),8) AS payload FROM generate_series(1,60000) i ORDER BY payload; FETCH 1 FROM c; SELECT pg_sleep(5); CLOSE c; COMMIT; SELECT pg_stat_force_next_flush();"
    clients = [subprocess.Popen(['docker','exec','-i',pg,'psql','-XAt','-U','postgres','-v','ON_ERROR_STOP=1'], stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(2)]
    for client in clients:
        client.stdin.write(query)
        client.stdin.close()
    time.sleep(3)
    report['held'] = sql("SELECT json_build_object('files',(SELECT coalesce(json_agg(t),'[]') FROM pg_ls_tmpdir() t),'activity',(SELECT json_agg(t) FROM (SELECT pid,wait_event_type,wait_event FROM pg_stat_activity WHERE datname=current_database()) t));")
    for i,c in enumerate(clients):
        stdout,stderr = c.stdout.read(),c.stderr.read()
        rc = c.wait(timeout=30)
        (out / ('session-%s.txt' % i)).write_text(stdout + stderr)
        if rc:
            raise RuntimeError('spill failed')
    time.sleep(1)
    report['after'] = sql("SELECT json_build_object('files',(SELECT coalesce(json_agg(t),'[]') FROM pg_ls_tmpdir() t),'stats',(SELECT row_to_json(s) FROM (SELECT temp_files,temp_bytes FROM pg_stat_database WHERE datname=current_database()) s));")
    run(['docker','exec',tracer,'sh','-c','kill -INT "$(cat /trace/pid)"; for i in $(seq 1 100); do test -f /trace/exit && exit 0; sleep .1; done; exit 1'])
    raw = run(['docker','exec',tracer,'cat','/trace/raw'])
    with gzip.open(out / 'syscalls.trace.gz','wt') as f:
        f.write(raw)
    report['diagnostics'] = run(['docker','exec',tracer,'cat','/trace/diagnostics'])
    report['tracer_exit'] = run(['docker','exec',tracer,'cat','/trace/exit']).strip()
    report['strace_version'] = run(['docker','exec',tracer,'cat','/trace/version'])
    report['status'] = 'preflight-collected-not-qualified'
finally:
    for name in (tracer,pg):
        run(['docker','rm','-f',name],check=False)
    run(['docker','volume','rm',volume],check=False)
    report['cleanup'] = {kind: run(['docker',kind,'ls',*(['-a'] if kind=='container' else []),'-q','--filter','label='+label]).splitlines() for kind in ('container','network','volume')}
    (out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(report['status'])
