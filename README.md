# Binance Price Alerts

[Публичный репозиторий GitHub](https://github.com/irhinandrej399-jpg/binance-price-alerts)

Учебный backend на Go: пользователь создаёт до 15 активных ценовых подписок на Binance Spot и получает одноразовые уведомления в Telegram. Разработка — через TDD; система проектируется как четыре микросервиса.

Кода приложения пока нет. Этот репозиторий фиксирует требования и задачи.

- [Согласованные требования и открытые решения](docs/requirements.md)
- [Бэклог с порядком работ и отдельными тикетами](docs/backlog/README.md)
- [Единая очередь выполнения всех тикетов](docs/backlog/ORDER.md)
- [Модели и уровни размышления для задач](docs/backlog/MODELS.md)
- [Правила размера тикета и TDD](docs/backlog/workflow.md)
- [Проверенные ограничения Binance и Telegram](docs/integration-facts.md)
- [Как вести сессии с агентом и передавать состояние](docs/WORKING.md)
- [Текущее состояние проекта](docs/STATE.md)
- [Автоматические коммиты агента по тикетам](docs/GIT.md)

Для новой сессии в этой папке достаточно «Выполни тикет <ID>»: `AGENTS.md` требует прочитать [skill сессии](.agents/skills/binance-ticket-session/SKILL.md), состояние проекта и нужную карточку. Skill сообщает рекомендацию модели и reasoning; выбор делается в клиенте. Можно также поручить «Возьми следующий тикет из бэклога». Следующая задача по `docs/backlog/ORDER.md`: `python3 scripts/backlog.py next`. Проверка целостности бэклога и очереди: `python3 scripts/backlog.py check`.
