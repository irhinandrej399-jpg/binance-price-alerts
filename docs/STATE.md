# Текущее состояние проекта

Обновлено: 27 октября 2026. Это краткая навигация для новой сессии. Статусы живут в карточках, результаты — в их журнале, решения — в ADR; фактические файлы и проверки подтверждают сводку.

## Активная работа

- Активный продуктовый тикет: нет.
- Незавершённая реализация: нет. MKT-005 завершён; зависимость фильтра для MKT-012 готова. MKT-012 остаётся planned до собственной повторной сессионной проверки.
- Последний продуктовый тикет `done`: [MKT-005](backlog/tickets/MKT-005.md). Чистый `trade.CheckNext` отсекает повторные/старые raw trade ID одной пары, сохраняет границу через reconnect и признак возможного пропуска. [Контракт](../internal/marketdata/trade/README.md). Unit-тесты, общий race/vet/build пройдены; durable приём остаётся MKT-017, runtime не запускался.
- Последняя работа над процессом: добавлен проектный skill `binance-ticket-session` с автоматическим выбором и обязательным чтением через AGENTS.md. Он восстанавливает контекст одного тикета, сообщает модель из карточки и ведёт до проверки и передачи результата. Проверены формат skill, YAML, локальные ссылки и целостность бэклога; выбор skill в отдельном новом чате ещё не проверялся. Короткие запросы и проверка новой сессии — в WORKING.md. Продуктовые тикеты этим не закрывались.

## Что существует

- [Согласованные требования](requirements.md).
- WebSocket transport: `internal/marketdata/stream.Read` и `ReadWithCommands(ctx, endpoint, config, commands, handle)`, gorilla/websocket v1.5.3. Общий бюджет команд/pong по умолчанию 5/с, ограниченная очередь pong, отмена и ожидание writer при выходе. `Reconnect` восстанавливает URL/начальные команды после сбоя, сообщает разрыв и ждёт bounded backoff с jitter. Синхронные callbacks должны учитывать context; ротация и runtime wiring ещё не реализованы.
- [Карточки и зависимости](backlog/README.md), включая разделение крупных задач.
- [Единая последовательность выполнения](backlog/ORDER.md) — от первого тикета к последнему.
- [Рекомендации моделей и reasoning](backlog/MODELS.md); для DEC-001 — GPT-6.1 Sol · Medium.
- [Порядок сессии и готовые запросы](WORKING.md).
- [Проектный skill сессии](../.agents/skills/binance-ticket-session/SKILL.md), обязательный к чтению по AGENTS.md; настройки модели в клиенте не меняет.
- [Результат проверки размера](backlog/session-review.md).
- Каркас приложения: `go.mod`, `.go-version` (Go 1.26.3) и четыре точки входа в `cmd/`. Реализованы доменные цена, процентный порог, предикат и жизненный цикл подписки; добавлена миграция аккаунтов API; рабочего runtime-стенда ещё нет. Сборка и проверки описаны в [README](../README.md).
- Сессии API: `internal/api/session` выдаёт случайный 256-битный Bearer-токен на 24 часа и проверяет digest/срок, возвращая user_id. Схема — `migrations/api/002_sessions.sql`; [решение](decisions/IAM-004.md) согласовано пользователем. Вход — `internal/api/login`; middleware `internal/api/auth` проверяет Bearer и передаёт доверенный user_id через context.
- Webhook Telegram: `internal/api/telegramwebhook.Handler(secret, next)`, один заголовок X-Telegram-Bot-Api-Secret-Token, 403 при отказе; обработка приватного `/start <code>` — `internal/api/telegramlink`, подключается через next. Регистрация реального webhook не выполнялась.
- Коды Telegram: `internal/api/telegramcode`, миграция `003_telegram_link_codes.sql`; один текущий хеш на аккаунт, блокировка пользователя перед заменой. Поглощение — `internal/api/telegramlink`, миграция `004_telegram_binding.sql` добавляет chat/version в app.users; единая блокировка пользователя для выдачи, перепривязки и будущих создания/допуска подписок.
- Password adapter `internal/api/password`: Argon2id v19, 19 MiB / 2 прохода / 1 поток, соль 16 байт, ключ 32 байта; ошибки без credentials. Использует `golang.org/x/crypto` v0.57.0; проверены unit-тесты, race и fuzz. Runtime-лимиты остаются отдельными задачами.
- Доменный пакет `internal/domain/price` и его unit-тесты — точная цена, процентный порог, предикат и свежесть котировки; контракты и границы описаны в DOM-001/002/003/005.
- Доменный пакет `internal/domain/subscription` и unit-тесты — чистые переходы жизненного цикла; контракт эффектов и обязанности будущего хранилища описаны в DOM-004.
- Локальный PostgreSQL: [запуск и проверка](../infra/postgres/README.md), compose и init в `infra/postgres/`; рабочий проект `binance-storage` на loopback:5432. Локальные пароли в игнорируемом `.env`; миграция аккаунтов API существует, в рабочую БД этой сессией не применялась. Runner — `python3 scripts/migrate.py --owner <owner>`, отдельно от runtime; [контракт](../migrations/README.md).
- Подключение Go к PostgreSQL: `internal/platform/postgres.Open`, драйвер pgx v5.11.0. `internal/testutil/postgrestest.Open(t)` и `scripts/check_postgres.py` проверяют реальные транзакции на disposable проекте binance-go-check; обычный go test пропускает opt-in тесты. Первый прикладной репозиторий — `internal/api/account`; схема аккаунтов применяется runner на тестовом стенде.
- Локальная Kafka: [запуск и проверка](../infra/kafka/README.md), Compose/init и изолированный smoke-check в `infra/kafka/`. Рабочий проект `binance-broker` healthy на loopback:9092, префикс topics `local`. Обычный volume без отдельной квоты; нагрузочный override требует отдельную ФС 40 GiB. Прикладных producers/consumers пока нет.
- Общий конверт `internal/contracts/event`: JSON-метаданные, версия 1, Validate и типизированные ошибки; SubscriptionChanged — `internal/contracts/subscriptionchanged`; PriceObserved — `internal/contracts/priceobserved`; AlertTriggered — `internal/contracts/alerttriggered`; все четыре схемы готовы, транспорт и runtime ещё не реализованы.
- Go CI: `.github/workflows/go.yml`; команды воспроизведения — в README. Публикация коммита отдельно запустит первую проверку на GitHub.
- Локальный Git-репозиторий: основная ветка `main`. SHA текущего снимка определяется через `git log -1`; его не нужно дублировать внутри самого коммита.
- GitHub: [irhinandrej399-jpg/binance-price-alerts](https://github.com/irhinandrej399-jpg/binance-price-alerts), публичный. `origin` указывает на этот репозиторий; публикация текущей main поручена пользователем. Состояние синхронизации проверяется через `git status -sb` и SHA удалённой ветки.
- Агент самостоятельно коммитит завершённый тикет по [GIT.md](GIT.md), включая карточку, индекс и STATE, с описанием изменений и проверок.

## Решения и препятствия

[DEC-009](decisions/DEC-009.md) принят 27 октября: REST/JSON для внешнего API, gRPC/Protobuf для прямых внутренних вызовов, Kafka для событий. Карточки интерфейсов уточнены; сетевые адаптеры и генерация ещё не реализованы.

DEC-001–009 приняты. DEC-004 пересматривает прежнюю гарантию уведомления по снимку создания и terminal-срабатывание до доставки. Planned-предпосылки MAT-012, SUB-017/018/019/020 и NTF-014 включены в очередь.

Выбраны PostgreSQL и Kafka/KRaft, полный профиль стенда закреплён DEC-008. SQL-replay — SUB-022, cleaner Kafka — OPS-014, ограничители — OPS-015/016. Численные стоимости операций требуют проверки на будущих схемах; скорость cleaner пока считается нулевой. Telegram-политика и freshness согласованы, но не реализованы. Наличие каталога из четырёх сервисов не означает готовность этих решений.

## Следующий рекомендуемый шаг

По очереди следующий — [MKT-012](backlog/tickets/MKT-012.md): плановая ротация WebSocket. Фильтр MKT-005 готов; при замене соединения сохранять прежнюю границу source ID через `trade.CheckNext`. Выполнять только по новому поручению после повторной сессионной проверки.

`python3 scripts/backlog.py next` показывает следующий пункт из ORDER.md, предварительно предлагая завершить активную работу. Идти по этой очереди; разделы каталога и номера ID сами по себе порядок не задают.

## Проверка состояния

- `python3 scripts/backlog.py check` — целостность карточек, зависимостей, статусов индекса и последовательности ORDER.md.
- `python3 scripts/backlog.py show MKT-012` — следующая карточка и статус её зависимостей.
- `git status --short`, соответствующий diff и `git log -1`.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -v` — проверки очереди и инструмента коммитов во временных репозиториях.
- `GOTOOLCHAIN=local go list ./...`, `GOTOOLCHAIN=local go test ./...`, `GOTOOLCHAIN=local go build ./...` — проверки каркаса. Тесты цены проверяют доменный контракт; успешная компиляция cmd не подтверждает runtime-сценарии.

При завершении работы обновлять только изменившиеся факты: активный ID, последний результат, остающиеся препятствия и следующий шаг. Подробные команды и результаты оставлять в карточке.
