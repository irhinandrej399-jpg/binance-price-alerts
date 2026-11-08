"""Checks that the explicit execution queue governs ticket selection."""

from contextlib import redirect_stdout
import importlib.util
from io import StringIO
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('backlog_order_test_module', SOURCE / 'scripts/backlog.py')
backlog = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = backlog
SPEC.loader.exec_module(backlog)


class BacklogOrderTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='backlog-order-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.base = self.root / 'docs/backlog'
        (self.base / 'tickets').mkdir(parents=True)
        patcher = patch.multiple(backlog, ROOT=self.root, BASE=self.base)
        patcher.start()
        self.addCleanup(patcher.stop)

    def fixture(self, order=None, statuses=None, container=False):
        statuses = statuses or {}
        entries = [('ZED-001', ()), ('BETA-001', ('ZED-001',)), ('ALPHA-001', ())]
        if container:
            entries.append(('SPLIT-001', ()))
            statuses = statuses | {'SPLIT-001': 'split'}
        rows = []
        for tid, deps in entries:
            status = statuses.get(tid, 'planned')
            dep_links = ', '.join(f'[{d}]({d}.md)' for d in deps) or 'Нет.'
            (self.base / 'tickets' / f'{tid}.md').write_text(
                f'# {tid} — Fixture\n\nСтатус: `{status}`.\n\nЗависимости: {dep_links}\n', encoding='utf-8')
            rows.append(f'| [{tid}](tickets/{tid}.md) | Fixture | {", ".join(deps) or "—"} | {status} |')
        (self.base / 'README.md').write_text('\n'.join(rows) + '\n', encoding='utf-8')
        self.order(order if order is not None else ['ZED-001', 'BETA-001', 'ALPHA-001'])

    def order(self, ids):
        (self.base / 'ORDER.md').write_text(''.join(
            f'{n}. [{tid}](tickets/{tid}.md) — Fixture.\n' for n, tid in enumerate(ids, 1)), encoding='utf-8')

    def errors(self):
        return backlog.validate(backlog.read_tickets())

    def next(self, *args):
        out = StringIO()
        with patch.object(sys, 'argv', ['backlog.py', 'next', *args]), redirect_stdout(out):
            result = backlog.main()
        self.assertEqual(result, 0)
        return out.getvalue()

    def test_valid_order_covers_all_active_tickets(self):
        self.fixture()
        self.assertEqual(self.errors(), [])

    def test_missing_ticket_is_rejected(self):
        self.fixture(['ZED-001', 'ALPHA-001'])
        self.assertTrue(self.errors())

    def test_duplicate_is_rejected(self):
        self.fixture(['ZED-001', 'BETA-001', 'ALPHA-001', 'ALPHA-001'])
        self.assertTrue(self.errors())

    def test_dependency_must_appear_first(self):
        self.fixture(['BETA-001', 'ZED-001', 'ALPHA-001'])
        self.assertTrue(self.errors())

    def test_container_is_not_executable_queue_item(self):
        self.fixture(['ZED-001', 'BETA-001', 'ALPHA-001', 'SPLIT-001'], container=True)
        self.assertTrue(self.errors())

    def test_next_follows_file_instead_of_alphabet(self):
        self.fixture()
        text = self.next()
        self.assertIn('ZED-001 [planned]', text)
        self.assertNotIn('ALPHA-001 [planned]', text)

    def test_next_shows_model_and_effort_from_the_card(self):
        self.fixture()
        card = self.base / 'tickets/ZED-001.md'
        with card.open('a', encoding='utf-8') as stream:
            stream.write('\n## Модель для сессии\n\n- Модель: `gpt-6.1-sol` (GPT-6.1 Sol).\n- Уровень размышления: `medium` (Medium).\n')
        text = self.next()
        self.assertIn('gpt-6.1-sol', text)
        self.assertIn('medium', text)

    def test_done_is_skipped(self):
        self.fixture(statuses={'ZED-001': 'done'})
        text = self.next()
        self.assertIn('BETA-001 [planned]', text)
        self.assertNotIn('ZED-001 [done]', text)

    def test_existing_active_work_is_resumed(self):
        self.fixture(statuses={'ALPHA-001': 'in-progress'})
        text = self.next()
        self.assertIn('ALPHA-001 [in-progress]', text)
        self.assertNotIn('ZED-001 [planned]', text)

    def test_preview_preserves_queue_order(self):
        self.fixture()
        text = self.next('--limit', '3')
        self.assertLess(text.index('ZED-001 [planned]'), text.index('BETA-001 [planned]'))
        self.assertLess(text.index('BETA-001 [planned]'), text.index('ALPHA-001 [planned]'))

    def test_deferred_stage_is_not_selected_even_if_in_progress(self):
        self.fixture(statuses={'ZED-001': 'done', 'ALPHA-001': 'in-progress'})
        path = self.base / 'ORDER.md'
        path.write_text(path.read_text().replace('2. [BETA',
                        '## Отложенный этап\n\n2. [BETA'))
        text = self.next()
        self.assertIn('Текущий этап завершён', text)
        self.assertNotIn('[planned]', text)
        self.assertNotIn('[in-progress]', text)
        self.assertIn('ALPHA-001 [in-progress]', self.next('--include-deferred'))
        self.assertEqual(self.errors(), [])

    def test_cancelled_ticket_is_kept_in_index_but_not_queue(self):
        self.fixture(['ZED-001', 'ALPHA-001'], statuses={'BETA-001': 'cancelled'})
        self.assertEqual(self.errors(), [])
        self.assertNotIn('BETA-001', self.next('--include-deferred', '--limit', '5'))

    def test_cancelled_dependency_does_not_make_active_ticket_ready(self):
        self.fixture(['BETA-001', 'ALPHA-001'], statuses={'ZED-001': 'cancelled'})
        self.assertTrue(any('отменён' in error for error in self.errors()))

    def test_cancelled_ticket_cannot_remain_in_execution_queue(self):
        self.fixture(statuses={'BETA-001': 'cancelled'})
        self.assertTrue(any('ORDER.md' in error for error in self.errors()))

    def test_cancelled_chain_keeps_historical_dependencies(self):
        self.fixture(['ALPHA-001'], statuses={'ZED-001': 'cancelled', 'BETA-001': 'cancelled'})
        self.assertEqual(self.errors(), [])
        self.assertIn('ALPHA-001 [planned]', self.next())


if __name__ == '__main__':
    unittest.main()
