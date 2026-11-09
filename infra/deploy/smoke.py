#!/usr/bin/env python3
"""Disposable OPS-074 smoke; never accepts a permanent project or credentials."""
import json
import os
from pathlib import Path
import shutil
import secrets
import socket
import tempfile
import time

from deploy import APPS, Deployment, private


def main():
    os.umask(0o077)
    folder = Path(tempfile.mkdtemp(prefix='binance-ops074-'))
    project = 'binance-ops074-' + secrets.token_hex(4)
    config = folder / 'deploy.env'
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    private(config, f'DATA_DIR={folder}\nPROJECT={project}\nPREFIX=smoke\nAPI_PORT={port}\nMODE=smoke\n')
    d = Deployment(config, smoke=True)

    def phase(name, seconds=140):
        deadline = time.monotonic() + seconds
        while not (folder / 'smoke' / name).exists():
            states = json.loads(d.dc('ps', '--all', '--format', 'json').replace('}\n{', '},{').join(('[', ']')))
            if any(s.get('Service') == 'smoke' and s.get('State') == 'exited' for s in states):
                raise RuntimeError('smoke exited before ' + name)
            if time.monotonic() >= deadline:
                raise RuntimeError('deadline waiting for ' + name)
            time.sleep(0.5)

    try:
        d.prepare()
        d.pki()
        # Configuration failure must precede any storage/runtime mutation.
        token = folder / 'secrets/bot_token'
        token.rename(token.with_suffix('.held'))
        try:
            d.base()
        except ValueError:
            print('missing-secret configuration failure PASS', flush=True)
        else:
            raise RuntimeError('missing secret accepted')
        finally:
            token.with_suffix('.held').rename(token)
        d.base()
        try:
            d.dc('build', '--build-arg', 'SERVICE=does-not-exist', 'api')
        except RuntimeError:
            print('invalid-service image build failure PASS', flush=True)
        else:
            raise RuntimeError('invalid service built')
        print('Building services and isolated smoke fixtures', flush=True)
        d.dc('build', *APPS, 'source-provision', 'smoke')
        d.dc('up', '-d', 'smoke')
        phase('fixtures')
        d.start()
        phase('restart')
        before = (folder / 'inventory/sources.json').read_bytes()
        volumes = json.loads(d.run(['docker', 'volume', 'ls', '--filter', 'label=com.docker.compose.project='+project, '--format', 'json']).replace('}\n{', '},{').join(('[', ']')))
        if len(volumes) != 2:
            raise RuntimeError('expected two persistent volumes')
        d.stop()
        for name in APPS:
            cid = d.dc('ps', '--all', '--quiet', name).strip()
            if d.run(['docker', 'inspect', '--format', '{{.State.ExitCode}}', cid]).strip() != '0':
                raise RuntimeError('non-graceful stop: ' + name)
        # Actual runtime startup failure, not just host-side file validation.
        key = folder / 'pki/api/server.key'
        key.rename(key.with_suffix('.held'))
        try:
            d.dc('run', '--rm', '--no-deps', 'api')
        except RuntimeError:
            print('missing-TLS runtime startup failure PASS', flush=True)
        else:
            raise RuntimeError('runtime accepted missing key')
        finally:
            key.with_suffix('.held').rename(key)
        d.start()
        if before != (folder / 'inventory/sources.json').read_bytes():
            raise RuntimeError('inventory changed on restart')
        private(folder / 'smoke/restarted', 'ok')
        phase('done')
        # Wait for the test to exit, so fake Close and handler joins are included.
        deadline = time.monotonic() + 15
        while d.dc('ps', '--status', 'running', '--quiet', 'smoke').strip():
            if time.monotonic() > deadline:
                raise RuntimeError('smoke did not exit')
            time.sleep(0.2)
        cid = d.dc('ps', '--all', '--quiet', 'smoke').strip()
        if d.run(['docker', 'inspect', '--format', '{{.State.ExitCode}}', cid]).strip() != '0':
            raise RuntimeError('smoke failure')
        print(d.dc('logs', '--no-log-prefix', 'smoke'), end='')
        print('ClusterID/TopicIDs/transactional IDs/inventory and two volumes preserved PASS', flush=True)
        return 0
    except BaseException:
        # Only this generated test dataset is eligible for diagnostics. Redact
        # even its random credentials before printing application/test logs.
        output = d.dc('logs', '--no-log-prefix', *APPS, 'smoke')
        for secret in (folder / 'secrets').glob('*'):
            output = output.replace(secret.read_text().strip(), '[redacted]')
        print(output, flush=True)
        raise
    finally:
        # The controller generated BOTH project and credentials. No permanent
        # profile can be supplied here. Deletion is confined to this dataset.
        d.dc('--profile', 'smoke', '--profile', 'tools', 'down', '--volumes', '--remove-orphans')
        if d.run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project='+project]).strip():
            raise RuntimeError('container cleanup incomplete')
        if d.run(['docker', 'volume', 'ls', '-q', '--filter', 'label=com.docker.compose.project='+project]).strip():
            raise RuntimeError('volume cleanup incomplete')
        if d.run(['docker', 'network', 'ls', '-q', '--filter', 'label=com.docker.compose.project='+project]).strip():
            raise RuntimeError('network cleanup incomplete')
        shutil.rmtree(folder)
        print('Disposable containers/volumes/network/private files cleanup PASS', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
