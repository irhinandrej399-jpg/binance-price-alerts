#!/usr/bin/env python3
"""Disposable OPS-074 smoke; never accepts a permanent project or credentials."""
import argparse
import http.client
import ssl
import json
import os
from pathlib import Path
import shutil
import secrets
import socket
import tempfile
import time

from deploy import APPS, Deployment, private
from backup import snapshot, restore, verify, volume_names


def ingress_checks(d, folder):
    context = ssl.create_default_context(cafile=str(folder / 'ingress/tls.pem'))
    # Preserve SNI and certificate verification while connecting to the mapped port.
    def request(method, path, body='', headers=None, tls=True):
        conn = http.client.HTTPConnection('127.0.0.1', int(d.env['HTTPS_PORT'] if tls else d.env['HTTP_PORT']), timeout=15)
        conn.connect()
        if tls:
            conn.sock = context.wrap_socket(conn.sock, server_hostname='alerts.test')
        conn.request(method, path, body, {'Host': 'alerts.test', 'Content-Type': 'application/json', **(headers or {})})
        res = conn.getresponse()
        status, location = res.status, res.getheader('Location')
        res.read()
        conn.close()
        return status, location
    deadline = time.monotonic() + 20
    while True:
        try:
            assert request('GET', '/healthz')[0] == 404
            break
        except (OSError, AssertionError):
            if time.monotonic() > deadline:
                raise RuntimeError('TLS ingress did not start')
            time.sleep(0.2)
    assert request('GET', '/v1/subscriptions', tls=False) == (308, 'https://alerts.test/v1/subscriptions')
    for path in ('/healthz', '/readyz', '/metrics', '/debug/pprof/', '/api.v1.Admission/Admit'):
        assert request('GET', path)[0] == 404
    assert request('GET', '/v1/subscriptions')[0] == 401
    assert request('POST', '/v1/telegram/link-codes')[0] == 401
    assert request('POST', '/v1/telegram/webhook', '{}', {'X-Telegram-Bot-Api-Secret-Token': 'wrong'})[0] == 403
    assert request('POST', '/v1/accounts', 'x' * 65537)[0] == 413
    # Stay away from a minute boundary for the fixed-window quota.
    if time.time() % 60 > 55:
        time.sleep(60 - time.time() % 60)
    for i in range(21):
        status, _ = request('POST', '/v1/sessions', '{}', {'X-Real-IP': f'192.0.2.{i+1}', 'X-Forwarded-For': f'198.51.100.{i+1}', 'Forwarded': f'for=198.51.100.{i+1}'})
        assert status == (429 if i == 20 else 400), f'quota attempt {i+1}: status {status}'
    # Docker's actual bindings are authoritative: only ingress may publish ports.
    states = json.loads(d.dc('ps', '--all', '--format', 'json').replace('}\n{', '},{').join(('[', ']')))
    for state in states:
        if state.get('Service') != 'ingress':
            assert not any(p.get('PublishedPort') for p in state.get('Publishers') or []), 'internal port published'
    print('TLS verification/redirect/closed bindings/protected routes/webhook/body limit/spoof-resistant quota PASS', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ingress', action='store_true')
    parser.add_argument('--restore', action='store_true', help='OPS-076 cold restore into independent volumes')
    args = parser.parse_args()
    if args.ingress and args.restore:
        parser.error('choose one smoke profile')
    os.umask(0o077)
    folder = Path(tempfile.mkdtemp(prefix='binance-ops074-'))
    project = 'binance-ops074-' + secrets.token_hex(4)
    config = folder / 'deploy.env'
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    private(config, f'DATA_DIR={folder}\nPROJECT={project}\nPREFIX=smoke\nAPI_PORT={port}\nMODE=smoke\n')
    if args.ingress:
        with config.open('a') as out:
            out.write('PUBLIC_DOMAIN=alerts.test\n')
    d = Deployment(config, smoke=True)
    original = d
    recovery = None
    backup_folder = None
    if args.ingress:
        # Host test ports stay loopback-only and disposable. Production uses 80/443.
        d.env.update(INGRESS_BIND='127.0.0.1', HTTP_PORT=str(port))
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            d.env['HTTPS_PORT'] = str(listener.getsockname()[1])
        override = folder / 'ingress-smoke.yaml'
        private(override, 'services:\n  smoke:\n    environment: {DEPLOY_INGRESS: "1"}\n    volumes:\n      - '+str(folder)+'/ingress:/run/ingress:ro\n    networks:\n      ingress:\n        ipv4_address: '+d.env['INGRESS_API_IP'].rsplit('.',1)[0]+'.4\n')
        d.compose += ['-f', str(override)]

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
        if args.restore:
            # A test-only cleaner client, issued by this disposable CA.
            (folder / 'cleaner').mkdir(mode=0o700)
            certs = folder / 'smoke/cleaner-client'
            certs.mkdir(mode=0o700)
            private(certs / 'extensions', 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=clientAuth\nsubjectAltName=URI:spiffe://deploy/cleaner\n')
            d.run(['openssl', 'req', '-new', '-sha256', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-pkeyopt', 'ec_param_enc:named_curve', '-nodes', '-keyout', str(certs / 'client.key'), '-out', str(certs / 'request.csr'), '-subj', '/CN=cleaner'])
            d.run(['openssl', 'x509', '-req', '-sha256', '-in', str(certs / 'request.csr'), '-CA', str(folder / 'pki/ca.pem'), '-CAkey', str(folder / 'pki/ca.key'), '-set_serial', '76', '-out', str(certs / 'client.pem'), '-days', '1', '-extfile', str(certs / 'extensions')])
            override = folder / 'backup-smoke.yaml'
            private(override, 'services:\n  smoke:\n    environment: {DEPLOY_RESTORE: "1"}\n    volumes:\n      - '+str(folder)+'/cleaner:/cleaner\n')
            d.compose += ['-f', str(override)]
        if args.ingress:
            d.run(['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-pkeyopt', 'ec_param_enc:named_curve', '-sha256', '-nodes',
                   '-keyout', str(folder / 'ingress/tls.key'), '-out', str(folder / 'ingress/tls.pem'),
                   '-days', '1', '-subj', '/CN=alerts.test', '-addext', 'subjectAltName=DNS:alerts.test'])
            (folder / 'ingress/tls.key').chmod(0o600)
            private(folder / 'ingress/tls.caddy', 'tls /run/ingress/tls.pem /run/ingress/tls.key\n')
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
        if args.restore:
            private(folder / 'smoke/inventory.json', (folder / 'inventory/sources.json').read_text())
        if args.ingress:
            ingress_checks(d, folder)
            private(folder / 'smoke/ingress_checked', 'ok')
        phase('restart')
        before = (folder / 'inventory/sources.json').read_bytes()
        volumes = json.loads(d.run(['docker', 'volume', 'ls', '--filter', 'label=com.docker.compose.project='+project, '--format', 'json']).replace('}\n{', '},{').join(('[', ']')))
        if len(volumes) != 2:
            raise RuntimeError('expected two persistent volumes')
        if args.restore:
            # snapshot owns all cleaner locks and stops the entire source.
            backup_folder = Path(tempfile.mkdtemp(prefix='binance-ops076-backup-'))
            backup_folder.rmdir()
            snapshot(d, backup_folder)
            saved_cleaner = {p.relative_to(folder): p.read_bytes() for p in (folder / 'cleaner').rglob('registry.json')}
            assert saved_cleaner and any(json.loads(v)['floor'] > 0 for v in saved_cleaner.values())
            # Real mixed/corrupt component: refuse before creating target state.
            private_tar = backup_folder / 'private.tar'
            length = private_tar.stat().st_size
            with private_tar.open('ab') as handle:
                handle.write(b'wrong snapshot component')
            target = Path(tempfile.mkdtemp(prefix='binance-ops076-restore-'))
            target.rmdir()
            restore_config = folder / 'restore.env'
            private(restore_config, f'DATA_DIR={target}\nPROJECT={project}-copy\nPREFIX=smoke\nAPI_PORT={port}\nMODE=smoke\n')
            recovery = Deployment(restore_config, smoke=True)
            fixture = d.dc('ps', '--all', '--quiet', 'smoke').strip()
            override = folder / 'recovery-smoke.yaml'
            private(override, 'services:\n  notifications:\n    network_mode: "container:'+fixture+'"\n')
            recovery.compose += ['-f', str(override)]
            try:
                restore(recovery, backup_folder)
            except ValueError:
                assert not target.exists()
                assert not set(volume_names(recovery).values()).intersection(d.run(['docker', 'volume', 'ls', '-q']).split())
                print('mixed/corrupt snapshot rejected before writes PASS', flush=True)
            else:
                raise RuntimeError('mixed snapshot accepted')
            with private_tar.open('r+b') as handle:
                handle.truncate(length)
            verify(backup_folder)
            restore(recovery, backup_folder)
            assert before == (target / 'inventory/sources.json').read_bytes()
            assert all((target / p).read_bytes() == v for p, v in saved_cleaner.items())
            assert not set(volume_names(d).values()).intersection(volume_names(recovery).values())
            # Keep the SAME fake and request history; only connect it to the
            # restored isolated network. Original SQL/Kafka never restart.
            d.run(['docker', 'network', 'disconnect', project+'_default', fixture])
            d.run(['docker', 'network', 'connect', '--alias', 'smoke', '--alias', 'notifications', project+'-copy_default', fixture])
            d = recovery
            print('independent volumes + exact inventory/cleaner deletion floor preserved PASS', flush=True)
        else:
            d.stop()
        for name in APPS:
            cid = original.dc('ps', '--all', '--quiet', name).strip()
            if d.run(['docker', 'inspect', '--format', '{{.State.ExitCode}}', cid]).strip() != '0':
                raise RuntimeError('non-graceful stop: ' + name)
        # Actual runtime startup failure, not just host-side file validation.
        key = d.data / 'pki/api/server.key'
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
        if before != (d.data / 'inventory/sources.json').read_bytes():
            raise RuntimeError('inventory changed on restart')
        private(folder / 'smoke/restarted', 'ok')
        phase('done')
        # Wait for the test to exit, so fake Close and handler joins are included.
        deadline = time.monotonic() + 15
        while original.dc('ps', '--status', 'running', '--quiet', 'smoke').strip():
            if time.monotonic() > deadline:
                raise RuntimeError('smoke did not exit')
            time.sleep(0.2)
        cid = original.dc('ps', '--all', '--quiet', 'smoke').strip()
        if d.run(['docker', 'inspect', '--format', '{{.State.ExitCode}}', cid]).strip() != '0':
            raise RuntimeError('smoke failure')
        print(original.dc('logs', '--no-log-prefix', 'smoke'), end='')
        print('ClusterID/TopicIDs/transactional IDs/inventory and two volumes preserved PASS', flush=True)
        return 0
    except BaseException:
        # Only this generated test dataset is eligible for diagnostics. Redact
        # even its random credentials before printing application/test logs.
        output = d.dc('logs', '--no-log-prefix', *APPS) + original.dc('logs', '--no-log-prefix', 'smoke')
        for secret in (folder / 'secrets').glob('*'):
            output = output.replace(secret.read_text().strip(), '[redacted]')
        print(output, flush=True)
        raise
    finally:
        # The controller generated BOTH project and credentials. No permanent
        # profile can be supplied here. Deletion is confined to this dataset.
        if recovery:
            recovery.dc('--profile', 'smoke', '--profile', 'tools', 'down', '--volumes', '--remove-orphans')
            if recovery.data.exists():
                shutil.rmtree(recovery.data)
            for resource in ('ps', 'volume', 'network'):
                command = ['docker', 'ps', '-aq'] if resource == 'ps' else ['docker', resource, 'ls', '-q']
                assert not original.run(command + ['--filter', 'label=com.docker.compose.project='+project+'-copy']).strip()
        original.dc('--profile', 'smoke', '--profile', 'tools', 'down', '--volumes', '--remove-orphans')
        if d.run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project='+project]).strip():
            raise RuntimeError('container cleanup incomplete')
        if d.run(['docker', 'volume', 'ls', '-q', '--filter', 'label=com.docker.compose.project='+project]).strip():
            raise RuntimeError('volume cleanup incomplete')
        if d.run(['docker', 'network', 'ls', '-q', '--filter', 'label=com.docker.compose.project='+project]).strip():
            raise RuntimeError('network cleanup incomplete')
        shutil.rmtree(folder)
        if backup_folder and backup_folder.exists():
            shutil.rmtree(backup_folder)
        print('Disposable containers/volumes/network/private files cleanup PASS', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
