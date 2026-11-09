#!/usr/bin/env python3
"""Cold physical snapshot of the one-host deployment. See BACKUP.md."""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import tarfile
import time

from deploy import APPS, ROOT, Deployment, maintenance, private


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def spec(d):
    return json.loads(d.dc('config', '--format', 'json'))


def volume_names(d):
    return {key: value['name'] for key, value in spec(d)['volumes'].items()}


def tar_volume(d, volume, folder, filename, image, restore=False):
    # The helper has no network, credentials or application entrypoint. GNU tar
    # preserves numeric owners/modes, required by PostgreSQL and Kafka.
    d.run(['docker', 'run', '--rm', '--network', 'none', '--user', '0',
           '--entrypoint', '/bin/sh', '--mount', f'type=volume,source={volume},target=/volume' + ('' if restore else ',readonly'),
           '--mount', f'type=bind,source={folder},target=/backup' + (',readonly' if restore else ''),
           image, '-ec', 'umask 077; exec tar ' + ('--numeric-owner -xpf ' if restore else '--numeric-owner -cpf ')
           + '/backup/' + filename + ' -C /volume' + ('' if restore else ' .')])


def stopped(d, volumes):
    ids = d.dc('ps', '--all', '--quiet').split()
    if not ids:
        raise ValueError('deployment has no containers')
    states = json.loads(d.run(['docker', 'inspect', *ids]))
    for state in states:
        service = state['Config']['Labels'].get('com.docker.compose.service')
        if service == 'smoke' and d.smoke:
            continue
        # Kafka's image launcher returns 128+SIGTERM even after Java completes
        # shutdown. Require its on-disk clean marker as well, never accept 137.
        allowed = (0, 143) if service == 'kafka' else (0,)
        if state['State']['Running'] or state['State']['OOMKilled'] or state['State']['ExitCode'] not in allowed:
            raise ValueError('not a clean offline service: ' + str(service))
    if not set(APPS + ('postgres', 'kafka')).issubset({s['Config']['Labels'].get('com.docker.compose.service') for s in states}):
        raise ValueError('missing stopped service')
    for volume in volumes.values():
        if d.run(['docker', 'ps', '-q', '--filter', 'volume=' + volume]).strip():
            raise ValueError('volume still mounted by running container')
    marker = json.loads(d.run(['docker', 'run', '--rm', '--network', 'none', '--entrypoint', '/bin/cat',
                              '--mount', f'type=volume,source={volumes["kafka-data"]},target=/volume,readonly',
                              spec(d)['services']['postgres']['image'], '/volume/logs/.kafka_cleanshutdown']))
    if marker.get('version') != 0 or not isinstance(marker.get('brokerEpoch'), int) or marker['brokerEpoch'] < 0:
        raise ValueError('Kafka clean shutdown marker missing/invalid')


def snapshot(d, destination, no_cleaner=False):
    """Caller holds deployment maintenance lock; returns only after durable seal."""
    started = time.monotonic()
    destination = destination.resolve()
    if destination.is_relative_to(d.data) or d.data.is_relative_to(destination):
        raise ValueError('backup must be outside DATA_DIR')
    destination.mkdir(mode=0o700)  # exclusive: failed/old snapshot never overwritten
    with ExitStack() as locks:
        root = d.data / 'cleaner'
        registries = sorted(root.rglob('registry.json')) if root.exists() else []
        if no_cleaner != (not registries) or (no_cleaner and root.exists() and any(root.iterdir())):
            raise ValueError('declare all cleaner registries or explicit unused cleaner')
        # All cleaner commands use exactly these persistent lock inodes. Acquire
        # before stopping writers and hold until the entire bundle is sealed.
        for registry in registries:
            fd = os.open(registry.parent / 'registry.lock', os.O_RDWR | os.O_NOFOLLOW)
            lock = locks.enter_context(os.fdopen(fd, 'r'))
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        d.stop()
        volumes = volume_names(d)
        stopped(d, volumes)
        config = spec(d)
        pg_image = config['services']['postgres']['image']
        images = {name: config['services'][name]['image'] for name in ('postgres', 'kafka')}
        # Physical restore is deliberately restricted to the same images/source.
        revision = d.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD']).strip()
        if not d.smoke and d.run(['git', '-C', str(ROOT), 'status', '--porcelain', '--untracked-files=no', '--',
                                 'infra/deploy', 'cmd', 'internal', 'migrations', 'go.mod', 'go.sum', 'proto']).strip():
            raise ValueError('commit deployment source before backup')
        files = {}
        for key, volume in volumes.items():
            filename = key + '.tar'
            tar_volume(d, volume, destination, filename, pg_image)
            (destination / filename).chmod(0o600)
            files[filename] = digest(destination / filename)
        # Private inputs include all four roles, PKI, inventory, cleaner registry,
        # floors/tombstones, and Caddy ACME state. Test signals are not recovery data.
        with tarfile.open(destination / 'private.tar', 'w') as archive:
            for child in sorted(d.data.iterdir()):
                if child.name in ('maintenance.lock', 'smoke'):
                    continue
                paths = [child] + (list(child.rglob('*')) if child.is_dir() else [])
                if any(p.is_symlink() or not (p.is_file() or p.is_dir()) for p in paths):
                    raise ValueError('private inputs must be regular files/directories')
                archive.add(child, arcname=child.name)
        files['private.tar'] = digest(destination / 'private.tar')
        manifest = {'version': 1, 'created_utc': datetime.now(timezone.utc).isoformat(),
                    'revision': revision, 'prefix': d.values['PREFIX'], 'mode': 'smoke' if d.smoke else 'real',
                    'platform': d.run(['docker', 'info', '--format', '{{.OSType}}/{{.Architecture}}']).strip(),
                    'images': images, 'cleaner': [str(p.parent.relative_to(d.data)) for p in registries],
                    'files': files, 'seconds': round(time.monotonic() - started, 3),
                    'bytes': sum((destination / name).stat().st_size for name in files)}
        for name in files:
            with (destination / name).open('rb') as handle:
                os.fsync(handle.fileno())
        private(destination / 'manifest.json', json.dumps(manifest, indent=2) + '\n')
        fd = os.open(destination, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        print(f"Cold snapshot sealed: {manifest['bytes']} bytes, {manifest['seconds']} s; services remain stopped", flush=True)
        return manifest


def verify(folder):
    folder = folder.resolve()
    manifest = json.loads((folder / 'manifest.json').read_text())
    if manifest['version'] != 1 or set(manifest['files']) != {'postgres-data.tar', 'kafka-data.tar', 'private.tar'}:
        raise ValueError('unsupported/incomplete snapshot')
    for name, expected in manifest['files'].items():
        path = folder / name
        if path.is_symlink() or not path.is_file() or digest(path) != expected:
            raise ValueError('snapshot checksum mismatch')
    return manifest


def restore(d, folder):
    """Restore only into an absent private directory and absent volumes."""
    started = time.monotonic()
    folder = folder.resolve()
    manifest = verify(folder)  # validate ALL parts before creating any state
    config = spec(d)
    if (manifest['prefix'] != d.values['PREFIX'] or manifest['mode'] != ('smoke' if d.smoke else 'real')
            or manifest['revision'] != d.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD']).strip()
            or manifest['platform'] != d.run(['docker', 'info', '--format', '{{.OSType}}/{{.Architecture}}']).strip()
            or manifest['images'] != {n: config['services'][n]['image'] for n in ('postgres', 'kafka')}):
        raise ValueError('source/mode/image mismatch')
    volumes = volume_names(d)
    if d.data.exists() or d.dc('ps', '--all', '--quiet').strip():
        raise ValueError('restore target exists')
    existing = set(d.run(['docker', 'volume', 'ls', '-q']).split())
    if existing.intersection(volumes.values()):
        raise ValueError('restore volume exists')
    # Extraction checks before mkdir; private bundles never contain links/devices.
    with tarfile.open(folder / 'private.tar') as archive:
        members = archive.getmembers()
        for member in members:
            path = Path(member.name)
            if path.is_absolute() or '..' in path.parts or not (member.isfile() or member.isdir()):
                raise ValueError('unsafe private archive')
        d.data.mkdir(mode=0o700, parents=True)
        with maintenance(d.data):
            # Strict regular-file/directory whitelist above also supports the
            # system Python 3.9, which predates tarfile extraction filters.
            archive.extractall(d.data, members=members)
            for key, name in volumes.items():
                d.run(['docker', 'volume', 'create', '--label', 'com.docker.compose.project=' + d.values['PROJECT'],
                       '--label', 'com.docker.compose.volume=' + key, name])
                tar_volume(d, name, folder, key + '.tar', manifest['images']['postgres'], restore=True)
            # No migrations/topic init/bootstrap: check the captured source, not a new one.
            d.base()
            d.dc('build', 'source-provision')
            try:
                d.storage()
                d.provision('verify')
            finally:
                d.dc('stop', 'kafka', 'postgres')
            print(f'Restore/source binding verification PASS in {time.monotonic()-started:.3f} s; services remain stopped', flush=True)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env', type=Path)
    sub = parser.add_subparsers(dest='command', required=True)
    backup = sub.add_parser('backup')
    backup.add_argument('folder', type=Path)
    backup.add_argument('--no-cleaner', action='store_true', help='explicitly declare cleaner has never been provisioned')
    for command in ('verify', 'restore'):
        sub.add_parser(command).add_argument('folder', type=Path)
    args = parser.parse_args()
    if args.command != 'verify' and args.env is None:
        parser.error('--env is required for backup/restore')
    try:
        if args.command == 'verify':
            verify(args.folder)
            print('Snapshot checksums PASS (not a restore test)')
        else:
            d = Deployment(args.env)
            if args.command == 'backup':
                with maintenance(d.data):
                    snapshot(d, args.folder, args.no_cleaner)
            else:
                restore(d, args.folder)
        return 0
    except (OSError, ValueError, KeyError, RuntimeError, tarfile.TarError):
        print('Backup/restore refused; preserve partial artifacts and keep services/cleaner stopped. See BACKUP.md. No private diagnostics printed.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
