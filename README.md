# Binance Price Alerts

[Публичный репозиторий GitHub](https://github.com/irhinandrej399-jpg/binance-price-alerts)

Учебный backend на Go: пользователь создаёт до 15 активных ценовых подписок на Binance Spot и получает одноразовые уведомления в Telegram. Разработка — через TDD; система проектируется как четыре микросервиса.

Создан каркас одного Go-модуля с четырьмя точками входа, доменный тип точной цены с расчётом процентного порога и проверкой текущего ценового условия, а также модель жизненного цикла подписки. Runtime ещё не реализован: команды пока завершаются сразу, без запуска серверов и подключения к внешним системам.

## Локальная разработка

Версия Go — **1.26.3**, закреплена в `.go-version` и директиве `go` в `go.mod`. Установите эту версию через свой менеджер версий Go или вручную. Директива `go` задаёт минимальную версию; `.go-version` фиксирует версию для разработки и CI. `GOTOOLCHAIN=local` в командах ниже использует установленный toolchain без автоматического скачивания.

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

Для подключения к PostgreSQL закреплён pgx v5.11.0; зависимости и контрольные суммы находятся в `go.mod`/`go.sum`. Каркас не требует переменных окружения, ключей или иных секретов. Пакет `internal/domain/price` содержит тип `Price` с точным парсингом, сравнением и строковой JSON/text-сериализацией. Внутри хранится целый коэффициент `big.Int` и масштаб (цена = коэффициент / 10^масштаб); сравнение выравнивает масштабы без float и без изменения исходных значений. Для сравнения цен используется `Cmp`. Диапазон — от 0 до 999999999999999999.999999999999999999; максимум 18 цифр после точки, без округления. Нулевое значение Go — цена 0. Контракт и ограничения описаны в [DOM-001](docs/backlog/tickets/DOM-001.md). `NewPercentageThreshold` рассчитывает фиксированный порог без округления и возвращает исходную цену, знаковый процент, направление и порог. Нулевой процент, падение ≥100%, нулевая база и непредставимый в Price результат отклоняются; контракт — [DOM-002](docs/backlog/tickets/DOM-002.md). `Satisfied(current, threshold, direction)` проверяет `up: P >= T` и `down: P <= T` без предыдущей цены; неизвестное направление даёт false. Результат относится только к переданной цене: перед попыткой доставки потребитель обязан проверить последнюю цену снова. Контракт — [DOM-003](docs/backlog/tickets/DOM-003.md). `go test ./...` проверяет доменные сценарии цены, процентного порога, предиката и компиляцию точек входа.

`price.CheckFreshness(hasPrice, tradedAt, now)` проверяет возраст по времени сделки и явно переданным часам: 0…5 секунд включительно — fresh, больше — stale; отсутствие цены — missing, будущее/отсутствующее/некорректное время — invalid_time. Stale допускает использование цены с предупреждением. Парсинг времени и состояние потока остаются вызывающему коду; контракт входов и границы — [DOM-005](docs/backlog/tickets/DOM-005.md).

Пакет `internal/domain/subscription` предоставляет чистый `Lifecycle.Apply(Event)`: допустимые переходы шести состояний, признак освобождения слота и необходимость аудита подтверждённого успеха. Нулевой Lifecycle — active с занятым слотом. Восстановление канала требует новой версии привязки; прежний порог и workflow остаются в содержащей подписке. Durable-аудит, атомарность счётчика слотов, проверка ID/версий попыток и идемпотентность команд обеспечиваются будущим API-хранилищем. Контракт и границы — [DOM-004](docs/backlog/tickets/DOM-004.md).

## Локальное хранилище

PostgreSQL с четырьмя изолированными БД, отдельными runtime/migration-ролями,
healthcheck и постоянным volume: [запуск, остановка и проверка](infra/postgres/README.md).
Образ закреплён по digest. Функциональный профиль — 1 vCPU/1 GiB;
нагрузочный override DEC-008 — 4 vCPU/10 GiB с отдельным внешним volume 120 GiB.

Миграции запускаются отдельно от runtime: `python3 scripts/migrate.py --owner api`.
[Формат файлов, история, блокировка и интеграционные проверки](migrations/README.md).

Go-пул и изолированные проверки транзакций: `python3 scripts/check_postgres.py`.
[Конфигурация, context timeout, очистка и использование в IAM-001](internal/platform/postgres/README.md).

Локальная Kafka KRaft: [запуск, topics, durability и smoke-проверка](infra/kafka/README.md).
Лимиты 2 vCPU/4 GiB, партиции 1/16/16; отдельный нагрузочный volume 40 GiB
подключается override. Функциональный volume не ограничен отдельной квотой.

## Проверки CI

[Go CI](.github/workflows/go.yml) запускается в GitHub Actions на push, pull request и вручную через workflow_dispatch. Один job на Ubuntu использует Go из `.go-version` и проверяет форматирование, актуальность generated Protobuf-кода, `go vet ./...` и `go test -race ./...`. Любая ошибка завершает job неуспешно; форматирование проверяется без изменения файлов. Кэширование Go отключено. Для проверки генерации нужны Python 3 и доступ к GitHub и Go module proxy.

Локальное воспроизведение из корня репозитория (Bash; для race detector нужны CGO и C-компилятор):

```bash
set -euo pipefail
export GOTOOLCHAIN=local CGO_ENABLED=1
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
  printf 'Run gofmt on:\n%s\n' "$unformatted"
  exit 1
fi
python3 scripts/generate_proto.py --check
go vet ./...
go test -race ./...
```

Генерация внутренних gRPC-контрактов из чистого checkout: `python3 scripts/generate_proto.py`. Закреплённые инструменты, размещение схем и loopback test helper описаны в [proto/README.md](proto/README.md).

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
