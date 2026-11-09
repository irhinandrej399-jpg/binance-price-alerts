"""Small signal fixture; no Docker/vendor credentials."""
import copy
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

import observe
import clean_cycle


class Signals(unittest.TestCase):
    def healthy(self):
        return dict(time=100, processes=True,
                    readiness={s: {'status': 'ready', 'feed': 'live'} for s in observe.APPS},
                    metrics={'backlog': 0, 'oldest': 0, 'blocked': 0, 'reports': 0},
                    market={'pending': 0, 'age': 1}, api={'changes': 0, 'delivery': 0},
                    disks={s: {'free_gib': 20, 'free_percent': 50} for s in observe.DISKS})

    def test_each_signal_and_local_delivery(self):
        base = self.healthy()
        self.assertEqual(observe.signals(base, None, observe.Limits()), [])
        cases = [
            ('process', lambda s: s.update(processes=False)),
            ('feed', lambda s: s['readiness']['market-data'].update(status='not_ready', feed='unconfirmed')),
            ('feed', lambda s: s['readiness']['market-data'].update(status='not_ready', feed='disconnected')),
            ('readiness.api', lambda s: s['readiness']['api'].update(status='not_ready')),
            ('quote_age', lambda s: s['market'].update(age=61)),
            ('relay', lambda s: s['market'].update(pending=101)),
            ('relay', lambda s: s['api'].update(delivery=101)),
            ('delivery_pending', lambda s: s['metrics'].update(reports=1)),
            ('delivery_blocked', lambda s: s['metrics'].update(blocked=1)),
            ('delivery_age', lambda s: s['metrics'].update(oldest=301)),
            ('disk.postgres', lambda s: s['disks']['postgres'].update(free_gib=4)),
            ('disk.kafka', lambda s: s['disks']['kafka'].update(free_percent=14)),
        ]
        for code, mutate in cases:
            with self.subTest(code=code):
                sample = copy.deepcopy(base)
                mutate(sample)
                alerts = observe.signals(sample, None, observe.Limits())
                self.assertIn(code, alerts)
                out = io.StringIO()
                with redirect_stderr(out):
                    observe.emit(alerts)
                self.assertIn('WARNING ' + code + '\n', out.getvalue())

    def test_growth_uses_elapsed_time_and_reset(self):
        prev, now = self.healthy(), self.healthy()
        now['time'] += 60
        now['metrics']['backlog'] = 101
        self.assertIn('queue_growth', observe.signals(now, prev, observe.Limits()))
        now['time'] += 240
        self.assertNotIn('queue_growth', observe.signals(now, prev, observe.Limits()))
        prev['metrics']['backlog'] = 200
        self.assertNotIn('queue_growth', observe.signals(now, prev, observe.Limits()))

    def test_missing_data_is_unknown_never_zero(self):
        for key in ('metrics', 'market', 'api', 'processes'):
            sample = self.healthy()
            sample[key] = None
            self.assertIn('probe.' + key, observe.signals(sample, None, observe.Limits()))
        for text in ('HTTP/1.0 503 Failure\r\n\r\npassword=secret',
                     'HTTP/1.0 200 OK\r\n\r\ndelivery_backlog NaN\n'):
            with self.assertRaises(ValueError):
                observe.metric_values(text)


class CleanerInventory(unittest.TestCase):
    def test_historical_and_prospective_consumers_cannot_be_omitted(self):
        with tempfile.TemporaryDirectory() as folder:
            d = SimpleNamespace(data=Path(folder))
            (d.data / 'inventory').mkdir()
            source = {'ClusterID': 'cluster', 'TopicID': [1] * 16, 'Topic': 'prices'}
            inv = {'Version': 1, 'Runtime': {'MATCHER_ENVIRONMENT': 'local', 'MATCHER_GROUP': 'local-matcher', 'MATCHER_REPLICA_ID': 'one'},
                   'Sources': {key: dict(source, Topic=key) for key in ('prices', 'subscription-changes', 'delivery-requests')},
                   'Binding': {'ConsumerID': 'notifications', 'Generation': 'generation'}}
            (d.data / 'inventory/sources.json').write_text(json.dumps(inv))
            census = {'legacy': False, 'projections': ['historical'], 'prices': [
                dict(id='historical-scope', partition=3, cluster='cluster', tid=bytes([1]*16).hex(), topic='prices')],
                'assignments': [{'id': 'unknown-scope', 'identity': {'Topic': 'prices', 'Partition': 4}}]}
            with patch.object(clean_cycle, 'sql', return_value=census):
                plans = clean_cycle.plans(d)
            self.assertEqual(set(plans), set(clean_cycle.REGISTRIES))
            self.assertEqual(len(plans['changes-p0']['consumers']), 2)
            self.assertEqual(len(plans['prices-p3']['consumers']), 2)
            self.assertIn('unknown-scope', [c['id'] for c in plans['prices-p4']['consumers']])
            inv['Binding']['Generation'] = 'next-generation'
            (d.data / 'inventory/sources.json').write_text(json.dumps(inv))
            with patch.object(clean_cycle, 'sql', return_value=census):
                next_plan = clean_cycle.plans(d)
            self.assertNotEqual(plans['delivery-p0']['consumers'][0]['name'], next_plan['delivery-p0']['consumers'][0]['name'])
            for name, plan in plans.items():
                parent = d.data / 'cleaner' / name
                parent.mkdir(parents=True)
                (parent / 'registry.json').write_text(json.dumps(plan))
            clean_cycle.review(d, plans)
            state = copy.deepcopy(plans['changes-p0'])
            path = d.data / 'cleaner/changes-p0/registry.json'
            omitted = copy.deepcopy(state)
            omitted['consumers'] = omitted['consumers'][:1]
            path.write_text(json.dumps(omitted))
            with self.assertRaises(ValueError):
                clean_cycle.review(d, plans)
            state['consumers'][0]['retired'] = True
            path.write_text(json.dumps(state))
            clean_cycle.review(d, plans)
            census['legacy'] = True
            with patch.object(clean_cycle, 'sql', return_value=census), self.assertRaises(ValueError):
                clean_cycle.plans(d)


if __name__ == '__main__':
    unittest.main()
