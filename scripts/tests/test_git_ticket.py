"""Integration checks for scoped commits in disposable local repositories."""

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2]


class TicketCommitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='git-ticket-test-')
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / 'repo'
        (self.repo / 'scripts').mkdir(parents=True)
        (self.repo / 'docs/backlog/tickets').mkdir(parents=True)
        (self.repo / 'docs/decisions').mkdir()
        for name in ('git_ticket.py', 'backlog.py'):
            shutil.copyfile(SOURCE / 'scripts' / name, self.repo / 'scripts' / name)
        self.card = 'docs/backlog/tickets/DEC-001.md'
        self.index = 'docs/backlog/README.md'
        self.state = 'docs/STATE.md'
        self.adr = 'docs/decisions/DEC-001.md'
        self.write(self.card, '# DEC-001 — Test decision\n\nСтатус: `decision`.\n\nЗависимости: Нет.\n')
        self.write(self.index, '| [DEC-001](tickets/DEC-001.md) | Test decision | — | decision |\n')
        self.write(self.state, 'Before\n')
        self.write('feature.txt', 'before\n')
        self.write('unrelated.txt', 'before\n')
        self.git('init', '-b', 'main')
        self.git('config', 'user.name', 'Ticket Test')
        self.git('config', 'user.email', 'ticket-test@example.invalid')
        self.git('config', 'commit.gpgsign', 'false')
        self.git('add', '--', 'scripts', 'docs', 'feature.txt', 'unrelated.txt')
        self.git('commit', '-m', 'Fixture baseline')
        self.write(self.card, '# DEC-001 — Test decision\n\nСтатус: `done`.\n\nЗависимости: Нет.\n\n## Ход работы\n\nChecked examples.\n\n## Передача следующей сессии\n\nDone.\n')
        self.write(self.index, '| [DEC-001](tickets/DEC-001.md) | Test decision | — | done |\n')
        self.write(self.state, 'After\n')
        self.write(self.adr, '# DEC-001\nAccepted fixture decision.\n')
        self.write('feature.txt', 'after\n')
        self.body = Path(self.temp.name) / 'body.txt'
        self.body.write_text('Изменения:\n- Test decision.\n\nПроверки:\n- Fixture verified.\n', encoding='utf-8')
        self.files = ['feature.txt', self.card, self.index, self.state, self.adr]

    def write(self, path, text):
        (self.repo / path).write_text(text, encoding='utf-8')

    def git(self, *args):
        result = subprocess.run(['git', '--literal-pathspecs', *args], cwd=self.repo, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def commit(self, files=None):
        return subprocess.run(
            [sys.executable, str(self.repo / 'scripts/git_ticket.py'), 'DEC-001',
             '--type', 'docs', '--summary', 'record decision', '--body-file', str(self.body),
             '--files', *(self.files if files is None else files)],
            cwd=self.repo, capture_output=True, text=True,
        )

    def assert_no_commit(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.git('rev-list', '--count', 'HEAD'), '1')

    def test_scoped_commit_preserves_other_work(self):
        self.write('unrelated.txt', 'user changes with trailing whitespace  \n')
        result = self.commit()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.git('log', '-1', '--format=%s'), 'docs(DEC-001): record decision')
        self.assertIn('Проверки:', self.git('log', '-1', '--format=%b'))
        committed = set(self.git('diff-tree', '--no-commit-id', '--name-only', '-r', 'HEAD').splitlines())
        self.assertEqual(committed, set(self.files))
        self.assertEqual(self.git('diff', '--name-only'), 'unrelated.txt')
        self.assertEqual(self.git('diff', '--cached', '--name-only'), '')

    def test_existing_staging_is_preserved(self):
        self.write('unrelated.txt', 'staged user changes\n')
        self.git('add', '--', 'unrelated.txt')
        self.assert_no_commit(self.commit())
        self.assertEqual(self.git('diff', '--cached', '--name-only'), 'unrelated.txt')

    def test_unfinished_ticket_is_not_committed(self):
        text = (self.repo / self.card).read_text().replace('`done`', '`decision`')
        self.write(self.card, text)
        self.assert_no_commit(self.commit())
        self.assertEqual(self.git('diff', '--cached', '--name-only'), '')

    def test_stale_index_is_not_committed(self):
        text = (self.repo / self.index).read_text().replace('| done |', '| decision |')
        self.write(self.index, text)
        self.assert_no_commit(self.commit())
        self.assertEqual(self.git('diff', '--cached', '--name-only'), '')

    def test_missing_metadata_is_rejected(self):
        self.assert_no_commit(self.commit([p for p in self.files if p != self.state]))

    def test_directories_are_not_staged(self):
        self.assert_no_commit(self.commit(self.files + ['docs']))
        self.assertEqual(self.git('diff', '--cached', '--name-only'), '')

    def test_message_requires_description_and_checks(self):
        self.body.write_text('Test decision without checks.\n')
        self.assert_no_commit(self.commit())
        self.assertEqual(self.git('diff', '--cached', '--name-only'), '')

    def test_failed_hook_keeps_only_selected_staging(self):
        self.write('unrelated.txt', 'user changes\n')
        hook = self.repo / '.git/hooks/pre-commit'
        hook.write_text('#!/bin/sh\nexit 1\n')
        hook.chmod(0o755)
        self.assert_no_commit(self.commit())
        self.assertEqual(set(self.git('diff', '--cached', '--name-only').splitlines()), set(self.files))
        self.assertEqual(self.git('diff', '--name-only'), 'unrelated.txt')

    def test_tracked_deletion_is_supported(self):
        (self.repo / 'feature.txt').unlink()
        result = self.commit()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('D\tfeature.txt', self.git('diff-tree', '--no-commit-id', '--name-status', '-r', 'HEAD'))

    def test_literal_paths_do_not_stage_matching_names(self):
        self.write('literal[1].txt', 'selected\n')
        self.write('literal1.txt', 'unrelated\n')
        files = self.files + ['literal[1].txt']
        result = self.commit(files)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('literal1.txt', self.git('show', '--format=', '--name-only', 'HEAD').splitlines())
        self.assertIn('?? literal1.txt', self.git('status', '--short'))


if __name__ == '__main__':
    unittest.main()
