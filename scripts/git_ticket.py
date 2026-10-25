#!/usr/bin/env python3
"""Commit one completed ticket and an explicit list of related files."""

import argparse
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TICKET_ID = r'[A-Z][A-Z0-9]*-\d+'
TYPES = ('feat', 'fix', 'refactor', 'test', 'docs', 'chore', 'perf', 'build', 'ci')


def git(*args, check=True):
    result = subprocess.run(
        ['git', '--literal-pathspecs', *args], cwd=ROOT,
        capture_output=True, text=True, errors='surrogateescape',
    )
    if check and result.returncode:
        raise ValueError(result.stderr.strip() or result.stdout.strip() or 'Ошибка Git')
    return result


def names(*args):
    return {x for x in git(*args).stdout.split('\0') if x}


def selected_files(raw_paths):
    selected = set()
    for raw in raw_paths:
        path = Path(raw)
        if path.is_absolute() or '..' in path.parts or not path.parts or path.parts[0] == '.git':
            raise ValueError(f'Нужен явный относительный путь файла внутри проекта: {raw}')
        actual = ROOT / path
        if actual.is_dir():
            raise ValueError(f'Перечисли файлы, а не каталог: {raw}')
        if not actual.resolve().is_relative_to(ROOT.resolve()):
            raise ValueError(f'Путь выходит за пределы проекта: {raw}')
        if not actual.exists() and git('ls-files', '--error-unmatch', '--', path.as_posix(), check=False).returncode:
            raise ValueError(f'Нет файла или отслеживаемого удаления: {raw}')
        selected.add(path.as_posix())
    return selected


def commit_ticket(args):
    repo = git('rev-parse', '--show-toplevel').stdout.strip()
    if Path(repo).resolve() != ROOT.resolve():
        raise ValueError('Скрипт должен работать с Git-репозиторием этого проекта')
    git('rev-parse', '--verify', 'HEAD')
    git('symbolic-ref', '--quiet', '--short', 'HEAD')
    git('var', 'GIT_AUTHOR_IDENT')
    if names('diff', '--cached', '--name-only', '-z'):
        raise ValueError('Индекс уже содержит изменения. Сначала проверь git diff --cached; скрипт не очищает чужой staging.')

    tid = args.id.upper()
    if not re.fullmatch(TICKET_ID, tid):
        raise ValueError(f'Неверный ID: {tid}')
    card_path = f'docs/backlog/tickets/{tid}.md'
    card = (ROOT / card_path).read_text(encoding='utf-8')
    if not re.search(r'^Статус: `done`', card, re.M):
        raise ValueError(f'{tid}: коммит завершения требует выполненных критериев и статуса done')
    for section in ('## Ход работы', '## Передача следующей сессии'):
        if section not in card:
            raise ValueError(f'{tid}: отсутствует раздел передачи {section}')

    summary = args.summary.strip()
    title = f'{args.type}({tid}): {summary}'
    if not summary or '\n' in summary or '\r' in summary or len(title) > 100:
        raise ValueError('Нужен однострочный заголовок до 100 символов вместе с типом и ID')
    body = Path(args.body_file).read_text(encoding='utf-8').strip()
    if not re.fullmatch(r'Изменения:\s*\S.*?\nПроверки:\s*\S.*', body, re.S):
        raise ValueError('Описание должно содержать непустые разделы «Изменения:» и «Проверки:»')

    files = selected_files(args.files)
    required = {card_path, 'docs/backlog/README.md', 'docs/STATE.md'}
    if tid.startswith('DEC-'):
        required.add(f'docs/decisions/{tid}.md')
    if not required <= files:
        raise ValueError('Добавь в --files метаданные результата: ' + ', '.join(sorted(required - files)))
    check = subprocess.run([sys.executable, str(ROOT / 'scripts/backlog.py'), 'check'], cwd=ROOT)
    if check.returncode:
        raise ValueError('Проверка бэклога не пройдена')
    git('diff', '--check', '--', *sorted(files))

    # Stage only explicitly supplied files; preserve all other working changes.
    git('add', '--', *sorted(files))
    staged = names('diff', '--cached', '--no-renames', '--name-only', '-z')
    if not staged or not staged <= files or not required <= staged:
        raise ValueError('Индекс не соответствует результату тикета: обязательные метаданные должны измениться. Проверь git diff --cached.')
    git('diff', '--cached', '--check')
    print(git('diff', '--cached', '--stat').stdout, end='', flush=True)
    with tempfile.TemporaryDirectory(prefix='ticket-commit-') as directory:
        message = Path(directory) / 'message.txt'
        message.write_text(f'{title}\n\n{body}\n', encoding='utf-8')
        print(git('commit', '--file', str(message)).stdout, end='')
    print('Commit: ' + git('rev-parse', '--short', 'HEAD').stdout.strip())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('id')
    parser.add_argument('--type', choices=TYPES, required=True)
    parser.add_argument('--summary', required=True)
    parser.add_argument('--body-file', required=True)
    parser.add_argument('--files', nargs='+', required=True)
    args = parser.parse_args()
    try:
        commit_ticket(args)
    except (ValueError, OSError) as exc:
        print(f'Ошибка: {exc}', file=sys.stderr)
        print('Коммит не подтверждён. Если staging уже выполнен, выбранные файлы остаются в индексе; проверь git status и git diff --cached.', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
