#!/usr/bin/env python3
"""One local operator observation. Emits only fixed warning codes and numbers."""
import argparse
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

from deploy import APPS, Deployment, maintenance, private

DISKS = ('host', 'postgres', 'kafka')


@dataclass
class Limits:
    free_gib: float = 5
    free_percent: float = 15
    quote_age: float = 60
    delivery_age: float = 300
    relay_pending: float = 100
    growth_per_minute: float = 100


def http_body(raw):
    header, body = raw.replace('\r\n', '\n').split('\n\n', 1)
    status = int(header.splitlines()[0].split()[1])
    if status not in (200, 503):
        raise ValueError('probe status')
    return status, body


def metric_values(raw):
    status, body = http_body(raw)
    if status != 200:
        raise ValueError('metrics unavailable')
    values = {}
    for name, target in (('delivery_backlog', 'backlog'), ('delivery_oldest_seconds', 'oldest'),
                         ('delivery_blocks', 'blocked'), ('delivery_pending_reports', 'reports')):
        matches = re.findall(r'^' + name + r'(?:\{[^\n]*\})? ([^\s]+)$', body, re.M)
        numbers = [float(v) for v in matches]
        if not numbers or any(not math.isfinite(v) or v < 0 for v in numbers):
            raise ValueError('metrics incomplete')
        values[target] = sum(numbers)
    return values


def probe(call):
    try:
        return call()
    except (OSError, RuntimeError, ValueError, KeyError, IndexError, subprocess.SubprocessError):
        # Never print third-party responses, argv, DSN, paths, or raw errors.
        return None


def sql(d, owner, statement):
    return json.loads(d.dc('exec', '-T', 'postgres', 'bash', '-ec',
        'set -a; source /run/config/postgres.env; key=${1^^}_RUNTIME_PASSWORD; '
        'export PGPASSWORD=${!key}; export PGOPTIONS="-c statement_timeout=2000 -c default_transaction_read_only=on"; '
        'exec psql -XAt -h 127.0.0.1 -U "${1}_runtime" -d "$1" -v ON_ERROR_STOP=1',
        'observe', owner, input=statement, timeout=12))


def collect(d):
    def processes():
        raw = d.dc('ps', '--all', '--format', 'json', timeout=12).strip()
        states = json.loads(raw) if raw.startswith('[') else [json.loads(v) for v in raw.splitlines()]
        expected = set(APPS) | {'postgres', 'kafka'} | ({'ingress'} if d.public else set())
        return all(any(s['Service'] == name and s['State'] == 'running' and
                       s.get('Health', '') in ('', 'healthy') for s in states) for name in expected)

    def http(name, path):
        # Literal arguments only; no shell interpolation of configuration.
        return d.dc('exec', '-T', name, '/bin/bash', '-ec',
                    'exec 3<>/dev/tcp/127.0.0.1/8080; printf "GET /'+path+' HTTP/1.0\r\n\r\n" >&3; cat <&3', timeout=8)

    def ready(name):
        status, body = http_body(http(name, 'readyz'))
        data = json.loads(body)
        return {'status': 'ready' if status == 200 else 'not_ready', 'feed': data.get('feed')}

    def disk(service, path):
        fields = d.dc('exec', '-T', service, 'df', '-Pk', path, timeout=8).splitlines()[-1].split()
        total, available = int(fields[1]) * 1024, int(fields[3]) * 1024
        return {'free_gib': available / 2**30, 'free_percent': available / total * 100}

    def host_disk():
        v = shutil.disk_usage(d.data)
        return {'free_gib': v.free / 2**30, 'free_percent': v.free / v.total * 100}

    return dict(time=time.time(), processes=probe(processes),
        readiness={name: probe(lambda: ready(name)) for name in APPS},
        metrics=probe(lambda: metric_values(http('notifications', 'metrics'))),
        market=probe(lambda: sql(d, 'market_data', """
SELECT json_build_object('pending',(SELECT count(*) FROM app.price_outbox WHERE NOT published),
 'age',(SELECT extract(epoch FROM clock_timestamp()-(latest->>'trade_time')::timestamptz)
        FROM app.market_streams WHERE pair='BTCUSDT'));
""")),
        api=probe(lambda: sql(d, 'api', """
SELECT json_build_object('changes',(SELECT count(*) FROM app.api_journal_batches
 WHERE transaction_end>(SELECT position FROM app.subscription_relay WHERE singleton)),
 'delivery',(SELECT count(*) FROM app.delivery_outbox o WHERE NOT EXISTS
 (SELECT FROM app.delivery_published p WHERE p.subscription_id=o.subscription_id)));
""")),
        disks={'host': probe(host_disk), 'postgres': probe(lambda: disk('postgres', '/var/lib/postgresql')),
               'kafka': probe(lambda: disk('kafka', '/var/lib/kafka/data'))})


def signals(s, previous, limits):
    alerts = []
    for key in ('processes', 'metrics', 'market', 'api'):
        if s[key] is None:
            alerts.append('probe.' + key)
    if s['processes'] is False:
        alerts.append('process')
    for name in APPS:
        value = s['readiness'][name]
        if value is None:
            alerts.append('probe.readiness.' + name)
        else:
            if name == 'market-data' and value.get('feed') != 'live':
                alerts.append('feed')
            if value['status'] != 'ready':
                alerts.append('readiness.' + name)
    for name in DISKS:
        value = s['disks'][name]
        if value is None:
            alerts.append('probe.disk.' + name)
        elif value['free_gib'] < limits.free_gib or value['free_percent'] < limits.free_percent:
            alerts.append('disk.' + name)
    market, api, metrics = s['market'], s['api'], s['metrics']
    if market is not None:
        if market['age'] is None or market['age'] > limits.quote_age or market['age'] < -5:
            alerts.append('quote_age')
    if (market and market['pending'] > limits.relay_pending) or (api and max(api.values()) > limits.relay_pending):
        alerts.append('relay')
    if metrics:
        for field, code in (('blocked', 'delivery_blocked'), ('reports', 'delivery_pending')):
            if metrics[field] > 0:
                alerts.append(code)
        if metrics['oldest'] > limits.delivery_age:
            alerts.append('delivery_age')
        # One DB-wide scrape target; never add gauges from replicas.
        if previous and previous.get('metrics'):
            elapsed = s['time'] - previous['time']
            if 30 <= elapsed <= 300 and (metrics['backlog'] - previous['metrics']['backlog']) * 60 / elapsed > limits.growth_per_minute:
                alerts.append('queue_growth')
    return alerts


def emit(alerts):
    for code in alerts:
        print('WARNING ' + code, file=sys.stderr, flush=True)


def observe(d, limits):
    with maintenance(d.data):
        state = d.data / 'observation.json'
        previous = probe(lambda: json.loads(state.read_text())) if state.exists() else None
        sample = collect(d)
        alerts = signals(sample, previous, limits)
        # Persist before reporting success; a write failure reaches the CLI's safe error.
        private(state, json.dumps(sample) + '\n')
        print(json.dumps(sample, sort_keys=True), flush=True)
        emit(alerts)
        return alerts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env', type=Path, required=True)
    for field, default in vars(Limits()).items():
        p.add_argument('--' + field.replace('_', '-'), type=float, default=default)
    args = p.parse_args()
    limits = Limits(**{field: getattr(args, field) for field in vars(Limits())})
    if any(not math.isfinite(v) or v <= 0 for v in vars(limits).values()) or limits.free_percent > 100:
        p.error('thresholds must be finite and positive; percent <= 100')
    os.umask(0o077)
    try:
        return int(bool(observe(Deployment(args.env), limits)))
    except Exception:
        emit(['observation_failed'])
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
