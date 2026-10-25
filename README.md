# Binance Price Alerts

Учебный backend на Go: пользователь создаёт до 15 активных ценовых подписок на Binance Spot и получает одноразовые уведомления в Telegram. Разработка — через TDD; система проектируется как четыре микросервиса.

Кода приложения пока нет. Этот репозиторий фиксирует требования и задачи.

- [Согласованные требования и открытые решения](docs/requirements.md)
- [Бэклог с порядком работ и отдельными тикетами](docs/backlog/README.md)
- [Единая очередь выполнения всех тикетов](docs/backlog/ORDER.md)
- [Правила размера тикета и TDD](docs/backlog/workflow.md)
- [Проверенные ограничения Binance и Telegram](docs/integration-facts.md)
- [Как вести сессии с агентом и передавать состояние](docs/WORKING.md)
- [Текущее состояние проекта](docs/STATE.md)
- [Автоматические коммиты агента по тикетам](docs/GIT.md)

Для новой сессии укажи один ID тикета и попроси прочитать `AGENTS.md`, `docs/STATE.md` и карточку. Следующая задача по `docs/backlog/ORDER.md`: `python3 scripts/backlog.py next`. Проверка целостности бэклога и очереди: `python3 scripts/backlog.py check`.
