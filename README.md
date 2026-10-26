# Binance Price Alerts

[Публичный репозиторий GitHub](https://github.com/irhinandrej399-jpg/binance-price-alerts)

Учебный backend на Go: пользователь создаёт до 15 активных ценовых подписок на Binance Spot и получает одноразовые уведомления в Telegram. Разработка — через TDD; система проектируется как четыре микросервиса.

Создан каркас одного Go-модуля с четырьмя точками входа. Бизнес-логика и runtime ещё не реализованы: команды пока завершаются сразу, без запуска серверов и подключения к внешним системам.

## Локальная разработка

Версия Go — **1.26.3**, закреплена в `.go-version` и директиве `go` в `go.mod`. Установите эту версию через свой менеджер версий Go или вручную. Директива `go` задаёт минимальную версию; `.go-version` фиксирует версию для разработки и будущего CI. `GOTOOLCHAIN=local` в командах ниже использует установленный toolchain без автоматического скачивания.

Из корня репозитория:

```sh
go version
GOTOOLCHAIN=local go list ./...
GOTOOLCHAIN=local go test ./...
GOTOOLCHAIN=local go build ./...
```

Сборка всех четырёх исполняемых файлов в игнорируемый Git каталог `bin/`:

```sh
mkdir -p bin
GOTOOLCHAIN=local go build -o ./bin/ ./cmd/...
```

| Каталог | Процесс | Запуск каркаса |
| --- | --- | --- |
| `cmd/api` | Аккаунты и подписки | `GOTOOLCHAIN=local go run ./cmd/api` |
| `cmd/market-data` | Приём рыночных данных Binance | `GOTOOLCHAIN=local go run ./cmd/market-data` |
| `cmd/matcher` | Проверка условий подписок | `GOTOOLCHAIN=local go run ./cmd/matcher` |
| `cmd/notifications` | Доставка уведомлений Telegram | `GOTOOLCHAIN=local go run ./cmd/notifications` |

Внешних Go-зависимостей пока нет, поэтому `go.sum` не требуется. Каркас не требует переменных окружения, ключей или иных секретов. Прикладные тесты появятся вместе с поведением через TDD; текущий `go test ./...` проверяет компиляцию пакетов без тестовых сценариев.

## Документация и работа по тикетам

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
