#!/usr/bin/env python3
"""One-host deployment. No secret values are passed in argv or printed."""
import argparse
import base64
import importlib.util
import json
import ipaddress
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import fcntl
from contextlib import contextmanager

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
APPS = ('market-data', 'api', 'matcher', 'notifications')
OWNERS = ('api', 'market_data', 'matcher', 'notifications')


@contextmanager
def maintenance(data):
    """Serialize operator lifecycle and backup on this host; never unlink lock."""
    data.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(data / 'maintenance.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def private(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Replace atomically; never truncate an existing file through a symlink.
    temp = path.with_name(path.name + '.tmp')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as out:
        out.write(value)
        out.flush()
        os.fsync(out.fileno())
    temp.replace(path)


def shellenv(values):
    return ''.join(f'{key}={shlex.quote(value)}\n' for key, value in sorted(values.items()))


class Deployment:
    def __init__(self, config, smoke=False):
        self.values = {}
        for line in config.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith('#'):
                continue
            key, value = line.split('=', 1)
            if key in self.values or key not in {'DATA_DIR', 'PROJECT', 'PREFIX', 'API_PORT', 'MODE', 'PUBLIC_DOMAIN', 'INGRESS_SUBNET'}:
                raise ValueError('unknown or duplicate configuration key')
            self.values[key] = value
        v = self.values
        for key in ('PROJECT', 'PREFIX'):
            if not re.fullmatch(r'[a-z][a-z0-9-]{0,39}', v[key]):
                raise ValueError('invalid project or prefix')
        if not v['API_PORT'].isdigit() or not 1024 <= int(v['API_PORT']) <= 65535:
            raise ValueError('invalid API port')
        if v.get('MODE', 'real') != ('smoke' if smoke else 'real'):
            raise ValueError('mode refused')
        raw_data = Path(v['DATA_DIR'])
        self.data = raw_data.resolve()
        if not raw_data.is_absolute() or self.data == Path('/') or self.data.is_relative_to(ROOT):
            raise ValueError('DATA_DIR must be absolute and outside checkout')
        v['DATA_DIR'] = str(self.data)
        self.env = dict(os.environ)
        self.env.pop('COMPOSE_PROFILES', None)
        self.env.update(v, RUN_UID=str(os.getuid()), RUN_GID=str(os.getgid()))
        # stop/status must work even after a secret file is lost. These commands
        # only select existing resources; base() loads the real ID before writes.
        self.env['CLUSTER_ID'] = 'unconfigured'
        self.compose = ['docker', 'compose', '--project-name', v['PROJECT'], '--env-file', '/dev/null',
                        '-f', str(HERE / 'compose.yaml')]
        if smoke:
            self.compose += ['-f', str(HERE / 'compose.smoke.yaml')]
            self.env['COMPOSE_PROFILES'] = 'smoke'
        self.public = bool(v.get('PUBLIC_DOMAIN'))
        if self.public:
            domain = v['PUBLIC_DOMAIN']
            if not re.fullmatch(r'(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z][a-z0-9-]{1,62}', domain):
                raise ValueError('invalid public domain')
            subnet = ipaddress.ip_network(v.get('INGRESS_SUBNET', '172.29.75.0/29'))
            if subnet.version != 4 or subnet.prefixlen != 29 or not subnet.is_private:
                raise ValueError('ingress needs a private IPv4 /29')
            self.env.update(INGRESS_SUBNET=str(subnet), INGRESS_PROXY_IP=str(subnet[2]), INGRESS_API_IP=str(subnet[3]))
            self.compose += ['-f', str(HERE / 'compose.ingress.yaml')]
        self.smoke = smoke

    def run(self, args, **kw):
        # Capture command output, including errors from third parties: never echo
        # secret-bearing SQL, driver errors, or rendered Compose configuration.
        result = subprocess.run(args, env=self.env, text=True, capture_output=True, **kw)
        if result.returncode:
            raise RuntimeError('command failed: ' + args[0])
        return result.stdout

    def dc(self, *args, **kw):
        return self.run(self.compose + list(args), **kw)

    def prepare(self):
        self.data.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.data.stat().st_mode & 0o077:
            raise ValueError('DATA_DIR must be private (chmod 700)')
        for sub in ('secrets', 'config', 'inventory', 'smoke', 'ingress', 'caddy'):
            (self.data / sub).mkdir(mode=0o700, exist_ok=True)
        if self.public and not (self.data / 'ingress/tls.caddy').exists():
            private(self.data / 'ingress/tls.caddy', '# Public ACME; no local CA or manual certificate.\n')
        names = ['postgres_password', 'webhook_secret', 'cluster_id']
        names += [f'{o}_{role}_password' for o in OWNERS for role in ('runtime', 'migration')]
        for name in names:
            p = self.data / 'secrets' / name
            if not p.exists():
                value = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip('=') if name == 'cluster_id' else secrets.token_hex(24)
                private(p, value + '\n')
        if self.smoke:
            private(self.data / 'secrets' / 'bot_token', '123:e2e-test-token\n')
            private(self.data / 'secrets' / 'webhook_secret', 'e2e_test_webhook_secret\n')

    def secret(self, name):
        p = self.data / 'secrets' / name
        if p.is_symlink() or not p.is_file() or p.stat().st_mode & 0o077:
            raise ValueError('missing or non-private secret file: ' + name)
        value = p.read_text().strip()
        if not value or '\n' in value or '\r' in value:
            raise ValueError('invalid secret file: ' + name)
        return value

    def dsn(self, owner, role='runtime'):
        from urllib.parse import quote
        password = quote(self.secret(f'{owner}_{role}_password'), safe='')
        return f'postgres://{owner}_{role}:{password}@postgres:5432/{owner}?sslmode=disable'

    def base(self):
        if self.public and not self.smoke:
            subject = self.run(['openssl', 'x509', '-in', str(self.data / 'pki/api/ca.pem'), '-noout', '-subject'])
            if (self.data / 'pki/dev-only').exists() or 'Binance local development CA' in subject:
                raise ValueError('development PKI refused for public profile')
            if (self.data / 'ingress/tls.caddy').read_text() != '# Public ACME; no local CA or manual certificate.\n':
                raise ValueError('public profile requires automatic public TLS')
        self.env['CLUSTER_ID'] = self.secret('cluster_id')
        if not re.fullmatch(r'[A-Za-z0-9_-]{22}', self.env['CLUSTER_ID']):
            raise ValueError('invalid cluster id')
        token, webhook = self.secret('bot_token'), self.secret('webhook_secret')
        if not re.fullmatch(r'[1-9][0-9]*:[A-Za-z0-9_-]+', token) or not re.fullmatch(r'[A-Za-z0-9_-]{1,256}', webhook):
            raise ValueError('invalid vendor credentials')
        for name in APPS:
            for f in ('ca.pem', 'server.pem', 'server.key') + (() if name == 'market-data' else ('client.pem', 'client.key')):
                p = self.data / 'pki' / name / f
                if not p.is_file() or p.is_symlink() or p.stat().st_mode & 0o077:
                    raise ValueError('missing or non-private PKI file')
        pg = {'POSTGRES_USER': 'postgres', 'POSTGRES_DB': 'postgres', 'POSTGRES_PASSWORD': self.secret('postgres_password'),
              'POSTGRES_INITDB_ARGS': '--auth-host=scram-sha-256 --auth-local=trust'}
        for o in OWNERS:
            for role in ('runtime', 'migration'):
                pg[f'{o}_{role}_password'.upper()] = self.secret(f'{o}_{role}_password')
        private(self.data / 'config/postgres.env', shellenv(pg))
        private(self.data / 'config/source-provision/runtime.env', shellenv({
            'NOTIFICATIONS_DATABASE_URL': self.dsn('notifications'), 'KAFKA_BROKERS': 'kafka:9092'}))
        for name in APPS:
            (self.data / 'config' / name).mkdir(mode=0o700, exist_ok=True)
        self.dc('config', '--quiet')
        if self.public:
            self.dc('run', '--rm', '--no-deps', 'ingress', 'caddy', 'validate', '--config', '/etc/caddy/Caddyfile', '--adapter', 'caddyfile')

    def pki(self):
        """Local development CA only. Never overwrite an existing authority."""
        pki = self.data / 'pki'
        pki.mkdir(mode=0o700)
        private(pki / 'dev-only', 'Development only\n')
        ca = pki / 'ca.pem'
        key = pki / 'ca.key'
        self.run(['openssl', 'req', '-x509', '-sha256', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-pkeyopt', 'ec_param_enc:named_curve',
                  '-nodes', '-keyout', str(key), '-out', str(ca), '-days', '30', '-subj', '/CN=Binance local development CA',
                  '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign'])
        for name in APPS:
            folder = pki / name
            folder.mkdir(mode=0o700)
            private(folder / 'ca.pem', ca.read_text())
            for kind in ('server',) + (() if name == 'market-data' else ('client',)):
                # Matcher has one server-name setting for BOTH API and Latest.
                san = 'DNS:services.internal' if kind == 'server' else f'URI:spiffe://deploy/{name}'
                ext = folder / 'extensions'
                private(ext, f'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage={"serverAuth" if kind == "server" else "clientAuth"}\nsubjectAltName={san}\n')
                csr = folder / 'request.csr'
                self.run(['openssl', 'req', '-new', '-sha256', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-pkeyopt', 'ec_param_enc:named_curve', '-nodes',
                          '-keyout', str(folder / (kind + '.key')), '-out', str(csr), '-subj', f'/CN={name}-{kind}'])
                self.run(['openssl', 'x509', '-req', '-sha256', '-in', str(csr), '-CA', str(ca), '-CAkey', str(key),
                          '-set_serial', str(secrets.randbits(128)), '-out', str(folder / (kind + '.pem')), '-days', '7', '-extfile', str(ext)])
                csr.unlink()
                ext.unlink()
        for p in pki.rglob('*'):
            if p.is_file():
                p.chmod(0o600)

    def runtime(self, inventory):
        uri = lambda name: 'spiffe://deploy/' + name
        common = {'HTTP_ADDR': '0.0.0.0:8080', 'KAFKA_BROKERS': 'kafka:9092', 'RPC_CA_FILE': '/run/pki/ca.pem',
                  'RPC_ADDR': '0.0.0.0:9080', 'RPC_CERT_FILE': '/run/pki/server.pem', 'RPC_KEY_FILE': '/run/pki/server.key',
                  'CLEANER_SERVICE_URI': uri('cleaner')}
        for name in APPS:
            values = dict(common)
            # Provisioning emits stable non-secret source/runtime settings.
            values.update(inventory)
            owner = name.replace('-', '_')
            values['DATABASE_URL' if name == 'api' else owner.upper() + '_DATABASE_URL'] = self.dsn(owner)
            if name == 'market-data':
                values.update(API_SERVICE_URI=uri('api'), MATCHER_SERVICE_URI=uri('matcher'),
                              RAW_TRADE_URL='ws://smoke:18080/ws' if self.smoke else 'wss://stream.binance.com:9443/ws',
                              CATALOG_BASE_URL='http://smoke:18080' if self.smoke else 'https://api.binance.com')
            if name == 'api':
                if self.public:
                    values['TRUSTED_PROXY_IP'] = self.env['INGRESS_PROXY_IP']
                values.update(TELEGRAM_WEBHOOK_SECRET=self.secret('webhook_secret'), MATCHER_SERVICE_URI=uri('matcher'),
                              NOTIFICATIONS_SERVICE_URI=uri('notifications'), ADMISSION_PERMIT_TTL='10s',
                              MARKET_RPC_ADDR='market-data:9080', MARKET_RPC_SERVER_NAME='services.internal',
                              MARKET_RPC_CERT_FILE='/run/pki/client.pem', MARKET_RPC_KEY_FILE='/run/pki/client.key', MARKET_RPC_CA_FILE='/run/pki/ca.pem')
            if name in ('matcher', 'notifications'):
                values.update(API_RPC_ADDR='api:9080', RPC_SERVER_NAME='services.internal',
                              RPC_CERT_FILE='/run/pki/client.pem', RPC_KEY_FILE='/run/pki/client.key')
            if name == 'matcher':
                values.update(LATEST_RPC_ADDR='market-data:9080', MATCHER_RPC_ADDR='0.0.0.0:9080',
                              MATCHER_RPC_CERT_FILE='/run/pki/server.pem', MATCHER_RPC_KEY_FILE='/run/pki/server.key',
                              SNAPSHOT_TTL='1h', REPLAY_TTL='1h', HEAD_INTERVAL='1s', SNAPSHOT_PAGE_SIZE='1000', REPLAY_PAGE_SIZE='100')
            if name == 'notifications':
                values.update(RPC_IDENTITY=uri(name), RPC_SERVER_CERT_FILE='/run/pki/server.pem', RPC_SERVER_KEY_FILE='/run/pki/server.key',
                              TELEGRAM_BASE_URL='http://127.0.0.1:18081' if self.smoke else 'https://api.telegram.org',
                              TELEGRAM_BOT_TOKEN=self.secret('bot_token'), TELEGRAM_TIMEOUT='2s', BOT_RATE='25', CHAT_INTERVAL='1s',
                              RETRY_BASE_DELAY='1s', RETRY_MAX_DELAY='1m', CONDITION_INTERVAL='1s', MAX_FAILURES='5', LEASE_TTL='15s',
                              RECOVERY_INTERVAL='1s', REPORT_LIMIT='10', WORKER_CONCURRENCY='2', WORKER_INTERVAL='100ms', WORKER_TIMEOUT='5s', WRITE_TIMEOUT='2s')
            private(self.data / 'config' / name / 'runtime.env', shellenv(values))

    def storage(self):
        self.dc('up', '-d', '--wait', '--wait-timeout', '150', 'postgres', 'kafka')

    def migrate(self):
        spec = importlib.util.spec_from_file_location('migrate', ROOT / 'scripts/migrate.py')
        migrate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migrate)
        for owner in OWNERS:
            sql = migrate.program(owner, migrate.load(ROOT / 'migrations' / owner))
            # Load passwords from the mounted file inside the container, not argv.
            self.dc('exec', '-T', 'postgres', 'bash', '-ec',
                    'set -a; source /run/config/postgres.env; key=${1^^}_MIGRATION_PASSWORD; '
                    'export PGPASSWORD=${!key}; exec psql -X -h 127.0.0.1 -U "${1}_migration" -d "$1" -v ON_ERROR_STOP=1',
                    'migrate', owner, input=sql)

    def provision(self, mode=None):
        mode = mode or ('verify' if (self.data / 'inventory/sources.json').exists() else 'init')
        output = self.dc('run', '--rm', '--no-deps', 'source-provision', '--mode', mode,
                         '--inventory', '/state/sources.json', '--prefix', self.values['PREFIX'])
        self.runtime(json.loads(output))

    def start_apps(self):
        for name in APPS:
            self.dc('up', '-d', '--wait', '--wait-timeout', '70', name)

    def stop_apps(self):
        # Lifecycle changes require an explicit review of all historical and new
        # consumer identities before the next scheduled deletion cycle.
        (self.data / 'cleaner/enabled').unlink(missing_ok=True)
        if self.public:
            self.dc('stop', 'ingress')
        # Compose stops a list concurrently; argument order is not a dependency
        # order. Join each runtime before stopping its SQL/Kafka dependencies.
        for name in reversed(APPS):
            self.dc('stop', name)

    def stop(self):
        self.stop_apps()
        self.dc('stop', 'kafka', 'postgres')

    def start(self):
        self.base()
        print('Building four services and provisioning tool', flush=True)
        self.dc('build', *APPS, 'source-provision')
        self.stop_apps()
        self.storage()
        print('Applying migrations and checking topics', flush=True)
        self.migrate()
        self.dc('exec', '-T', 'kafka', 'bash', '-s', '--', self.values['PREFIX'], input=(ROOT / 'infra/kafka/init-topics.sh').read_text())
        self.provision()
        print('Starting services', flush=True)
        self.start_apps()
        if self.public:
            self.dc('up', '-d', 'ingress')
        print('Started; market-data raw feed remains unconfirmed (/readyz=503)', flush=True)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env', type=Path, required=True)
    parser.add_argument('command', choices=('prepare', 'dev-pki', 'check', 'start', 'update', 'status', 'stop', 'resume'))
    args = parser.parse_args()
    try:
        d = Deployment(args.env)
        # Hold through the complete operation, including stop/build/start.
        with maintenance(d.data):
            return operate(d, args.command)
    except (OSError, ValueError, KeyError, RuntimeError):
        print('Deployment refused: check configuration/private inputs, Docker, build and service startup; preserve inventory and volumes. No secret-bearing diagnostics printed.', file=sys.stderr)
        return 1


def operate(d, command):
    if command == 'prepare':
        d.prepare()
        print('Prepared private database credentials; supply secrets/bot_token, then dev-pki')
    elif command == 'dev-pki':
        d.pki()
        print('Issued local development certificates (7 days); CA key remains outside service mounts')
    elif command in ('start', 'update'):
        d.start()
    elif command == 'check':
        d.base()
        print('Configuration and required private files OK')
    elif command == 'resume':
        d.base()
        d.stop_apps()
        d.storage()
        d.provision('resume')
        print('Resumed provisioning; services remain stopped, run start')
    elif command == 'stop':
        d.stop()
        print('Stopped; volumes and inventory preserved')
    else:
        # Compose interpolation still needs cluster ID for stop/status.
        print(d.dc('ps', '--all'), end='')
    return 0


if __name__ == '__main__':
    sys.exit(main())
