#!/usr/bin/env python3
"""OPS-024 infrastructure driver; preserve evidence outside the measured process."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parents[2]
evidence_path = Path(sys.argv[1])
assert not evidence_path.exists()
with tempfile.TemporaryDirectory(prefix='api-measure-') as tmp:
    binary = str(Path(tmp) / 'driver')
    process = None
    try:
        for line in sys.stdin:
            request = json.loads(line)
            if request['phase_id'] == 'setup':
                subprocess.run(['go', 'test', '-c', '-o', binary, './internal/tools/apimeasure'],
                               cwd=root, check=True, stdout=subprocess.DEVNULL)
                process = subprocess.Popen([binary, '-test.run=^TestMeasurementDriver$', '-test.timeout=30m'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                    env=dict(os.environ, OPS024_MEASURE='1'))
            process.stdin.write(line)
            process.stdin.flush()
            reply = process.stdout.readline()
            try:
                ack = json.loads(reply)
            except (ValueError, TypeError):
                evidence_path.with_suffix('.failure.txt').write_text(reply + process.stdout.read())
                raise RuntimeError('measurement driver failed; see .failure.txt') from None
            assert ack['phase_id'] == request['phase_id'] and ack['status'] == 'ok'
            if request['phase_id'] == 'verification':
                evidence_path.write_text(json.dumps(ack['evidence'], indent=2) + '\n')
            print(json.dumps({'phase_id': request['phase_id'], 'status': 'ok'}), flush=True)
    finally:
        if process:
            process.stdin.close()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=5)
