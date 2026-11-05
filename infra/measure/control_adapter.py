#!/usr/bin/env python3
"""Trusted control fixture: existing account and Kafka adapters, persistent producer."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

root = Path(__file__).resolve().parents[2]
with tempfile.TemporaryDirectory(prefix='joint-control-') as tmp:
    pg = str(Path(tmp) / 'pg')
    kf = str(Path(tmp) / 'kafka')
    producer = None
    try:
        for line in sys.stdin:
            phase = json.loads(line)['phase_id']
            if phase == 'setup':
                for binary, package in ((pg, 'pgmeasure'), (kf, 'kafkameasure')):
                    subprocess.run(['go', 'build', '-o', binary, './internal/tools/' + package], cwd=root, check=True, stdout=subprocess.DEVNULL)
                producer = subprocess.Popen([kf, os.environ['KAFKA_BROKERS'], os.environ['KAFKA_PREFIX']], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
                assert json.loads(producer.stdout.readline())['operation'] == 'prepared'
            elif phase == 'empty':
                pass
            elif phase == 'write':
                if len(sys.argv) > 1 and sys.argv[1] == 'fail':
                    raise RuntimeError('injected adapter error')
                if len(sys.argv) > 1 and sys.argv[1] == 'timeout':
                    time.sleep(300)
                subprocess.run([pg, '--rows', '10'], cwd=root, check=True, stdout=subprocess.DEVNULL)
                producer.stdin.write('publish\n')
                producer.stdin.flush()
                assert json.loads(producer.stdout.readline())['committed_records'] == 2
            elif phase == 'verification':
                producer.stdin.write('read\n')
                producer.stdin.flush()
                ack = json.loads(producer.stdout.readline())
                assert ack['isolation'] == 'read_committed' and len(ack['records']) == 2
            else:
                raise RuntimeError('unknown phase')
            print(json.dumps({'phase_id': phase, 'status': 'ok'}), flush=True)
    finally:
        if producer:
            producer.stdin.close()
            producer.wait(timeout=10)
