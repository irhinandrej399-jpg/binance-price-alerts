#!/usr/bin/env python3
"""Read-only ticket navigation. Markdown cards remain the source of truth."""

import argparse
from collections import Counter
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'docs' / 'backlog'
ID = r'[A-Z][A-Z0-9]*-\d+'
ALLOWED = {'split', 'decision', 'planned', 'ready', 'in-progress', 'review', 'done'}


@dataclass
class Ticket:
    id: str
    title: str
    status: str
    deps: tuple
    path: Path


def read_tickets():
    tickets = {}
    for path in sorted((BASE / 'tickets').glob('*.md')):
        text = path.read_text(encoding='utf-8')
        header = re.search(rf'^# ({ID}) — (.+)$', text, re.M)
        status = re.search(r'^Статус: `([^`]+)`', text, re.M)
        if not header or header[1] != path.stem or not status:
            raise ValueError(f'{path.name}: неверный заголовок или отсутствует статус')
        if status[1] not in ALLOWED:
            raise ValueError(f'{path.name}: неизвестный статус {status[1]}')
        line = re.search(r'^Зависимости: (.+)$', text, re.M)
        if status[1] != 'split' and not line:
            raise ValueError(f'{path.name}: отсутствует строка зависимостей')
        deps = tuple(re.findall(rf'\[({ID})\]', line[1])) if line else ()
        tickets[path.stem] = Ticket(path.stem, header[2], status[1], deps, path)
    if not tickets:
        raise ValueError('Карточки не найдены')
    return tickets


def validate(tickets):
    errors = []
    graph = {t.id: t.deps for t in tickets.values() if t.status != 'split'}
    for ticket in tickets.values():
        for dep in ticket.deps:
            if dep not in tickets:
                errors.append(f'{ticket.id}: нет зависимости {dep}')
            elif tickets[dep].status == 'split':
                errors.append(f'{ticket.id}: зависимость {dep} — контейнер; нужны дочерние задачи')
            elif ticket.status in {'ready', 'in-progress', 'review', 'done'} and tickets[dep].status != 'done':
                errors.append(f'{ticket.id}: статус {ticket.status}, но {dep} ещё {tickets[dep].status}')
    try:
        tuple(TopologicalSorter(graph).static_order())
    except CycleError as exc:
        errors.append(f'Цикл зависимостей: {exc.args[1]}')
    index = (BASE / 'README.md').read_text(encoding='utf-8')
    indexed = {}
    index_deps = {}
    for line in index.splitlines():
        if not line.startswith('| ['):
            continue
        cells = [x.strip() for x in line.strip('|').split('|')]
        match = re.fullmatch(rf'\[({ID})\]\(tickets/({ID})\.md\)', cells[0])
        if not match:
            continue
        tid = match[1]
        if tid in indexed:
            errors.append(f'Дублирующий ID в индексе: {tid}')
        indexed[tid] = cells[-1].strip('`')
        if indexed[tid] != 'split' and len(cells) == 4:
            index_deps[tid] = set(re.findall(ID, cells[2]))
        if tid != match[2]:
            errors.append(f'{tid}: ссылка индекса указывает на {match[2]}')
    for tid, ticket in tickets.items():
        if tid not in indexed:
            errors.append(f'{tid}: отсутствует в индексе')
        elif indexed[tid] != ticket.status:
            errors.append(f'{tid}: карточка {ticket.status}, индекс {indexed[tid]}')
        if ticket.status != 'split' and tid in indexed:
            if tid not in index_deps:
                errors.append(f'{tid}: неверная строка зависимостей индекса')
            elif index_deps[tid] != set(ticket.deps):
                errors.append(f'{tid}: зависимости карточки и индекса расходятся')
    for tid in indexed.keys() - tickets.keys():
        errors.append(f'{tid}: в индексе есть ID без карточки')
    return errors


def print_ticket(ticket):
    print(f'{ticket.id} [{ticket.status}] {ticket.title}')
    print(f'  {ticket.path.relative_to(ROOT)}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    nxt = sub.add_parser('next', help='Показать доступные карточки, не меняя статусы')
    nxt.add_argument('--limit', type=int, default=5)
    show = sub.add_parser('show', help='Показать карточку и состояния зависимостей')
    show.add_argument('id')
    sub.add_parser('check', help='Проверить граф, статусы и индекс')
    args = parser.parse_args()
    try:
        tickets = read_tickets()
        errors = validate(tickets)
    except (ValueError, OSError) as exc:
        print(f'Ошибка: {exc}', file=sys.stderr)
        return 1
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    if args.command == 'check':
        counts = Counter(t.status for t in tickets.values())
        print('OK: карточки, зависимости, отсутствие циклов и статусы индекса.')
        print(', '.join(f'{s}: {counts[s]}' for s in sorted(counts)))
        return 0
    if args.command == 'show':
        ticket = tickets.get(args.id.upper())
        if not ticket:
            print(f'Нет тикета {args.id}', file=sys.stderr)
            return 1
        print_ticket(ticket)
        print('Зависимости:')
        for dep in ticket.deps:
            print(f'  {dep}: {tickets[dep].status}')
        if not ticket.deps:
            print('  нет')
        print('\n' + ticket.path.read_text(encoding='utf-8'))
        return 0
    if args.limit <= 0:
        parser.error('--limit должен быть положительным')
    active = [t for t in tickets.values() if t.status in {'in-progress', 'review'}]
    if active:
        print('Сначала продолжить или проверить уже начатую работу:')
        for ticket in active:
            print_ticket(ticket)
        return 0
    candidates = [t for t in tickets.values()
                  if t.status in {'decision', 'planned', 'ready'}
                  and all(tickets[d].status == 'done' for d in t.deps)]
    candidates.sort(key=lambda t: (t.status != 'decision', t.status != 'ready', t.id))
    if not candidates:
        print('Доступных карточек нет. Проверьте незавершённые решения и зависимости.')
        return 0
    print('Зависимости завершены; перед реализацией ещё нужна проверка готовности:')
    for ticket in candidates[:args.limit]:
        print_ticket(ticket)
    if len(candidates) > args.limit:
        print(f'Ещё доступно: {len(candidates) - args.limit}. Увеличьте --limit для просмотра.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
