# Текущее состояние проекта

Обновлено: 28 октября 2026. Это краткая навигация для новой сессии. Статусы живут в карточках, результаты — в их журнале, решения — в ADR; фактические файлы и проверки подтверждают сводку.

## Активная работа

- Активный продуктовый тикет: нет.
- Незавершённая реализация: нет. SUB-014 завершён; SUB-017 требует собственной сессионной проверки.
- Последний завершённый тикет: [SUB-014](backlog/tickets/SUB-014.md). [Защищённый запрос работы доставки](../internal/api/deliveryrpc/README.md): unary gRPC с правом только matcher, канонические пара/порог и последняя цена через MKT-006, атомарная SUB-013. Повтор восстанавливает прежний workflow даже при недоступной цене; CAS, mTLS/deadline/отмена, две защищённые связи и конкурентные RPC проверены с race detector на disposable PostgreSQL; volume удалён. Runtime, публикация outbox и допуск отправки остаются отдельно.
- [SUB-013](backlog/tickets/SUB-013.md). [Атомарный запрос доставки](../internal/api/subscription/README.md): pending_delivery, устойчивый workflow, неизменяемый AlertTriggered и полный SubscriptionChanged/C сохраняются одной транзакцией, слот остаётся занят. Миграция API 009; конкуренция, отмена, rollback и потеря commit-ответа проверены на disposable PostgreSQL с race detector; volume удалён. Чтение последней цены/защищённый интерфейс готовы в SUB-014, публикация delivery_outbox — SUB-009, runtime отдельно.
- [SUB-007](backlog/tickets/SUB-007.md). [Relay подписок](../internal/api/relay/README.md): один целый пакет C на Kafka-транзакцию, исходные bytea/event_id, стабильный transactional.id/fencing, SQL cursor только после commit. Миграция API 008, lease/token и повторы после lost EndTxn/сбоя SQL-ack проверены на совместном disposable PostgreSQL/Kafka стенде с race detector; оба volumes удалены. Runtime, consumers и replay-pins/retention остаются отдельными задачами.
- [SUB-012](backlog/tickets/SUB-012.md). [HTTP-отмена и журнал](../internal/api/subscription/README.md): cancelled, версия, освобождение слота и полный SubscriptionChanged/C фиксируются одной транзакцией; terminal-повторы не создают новых записей. Полный disposable PostgreSQL-прогон с race detector прошёл, включая rollback INSERT/commit и 20 конкурентных отмен с одним событием. Relay готов в SUB-007; runtime остаётся отдельной задачей.
- [SUB-011](backlog/tickets/SUB-011.md). [HTTP-создание и журнал](../internal/api/subscription/README.md): подписка, слот, неизменяемый SubscriptionChanged и C фиксируются одной транзакцией. Проверены точный after-image, устойчивые байты/ID и полный rollback при ошибках INSERT/commit; итоговый disposable PostgreSQL-прогон с race detector прошёл. Отмена, relay и runtime остаются отдельными задачами.
- [SUB-021](backlog/tickets/SUB-021.md). [Журнал API](../internal/api/journal/README.md): Append полного пакета в pgx.Tx, транзакционный счётчик C и чтение целых пакетов строго после C. Миграция 007, rollback/commit-порядок, точный replay, неизменяемость и REPEATABLE READ проверены на реальном PostgreSQL с race detector. Создание подключено в SUB-011, отмена — SUB-012; relay готов в SUB-007; snapshot/retention и runtime остаются отдельными задачами.
- [SUB-005](backlog/tickets/SUB-005.md). [HTTP-отмена](../internal/api/subscription/README.md): DELETE /v1/subscriptions/{id} атомарно проверяет владельца/версию, отменяет любое незавершённое состояние и освобождает слот один раз. Повторы стабильны, ответ явно предупреждает о возможной отправке при наличии workflow. Реальные сессии/PostgreSQL и race detector прошли, включая 20 конкурентных отмен и ожидание новой версии допуска. Journal/outbox подключён к отмене в SUB-012; runtime и сами admission/result writers ещё отсутствуют.
- [SUB-004](backlog/tickets/SUB-004.md). [HTTP-список](../internal/api/subscription/README.md): GET /v1/subscriptions возвращает до 15 собственных незавершённых подписок в порядке UUID, включая workflow и причину blocked; показывает сохранённый результат восстановления с прежним порогом. Проверены реальные сессии/PostgreSQL, изоляция двух владельцев, все состояния и ошибки чтения с race detector. Runtime и само восстановление канала остаются отдельными задачами.
- [SUB-003](backlog/tickets/SUB-003.md). [HTTP-создание](../internal/api/subscription/README.md): POST /v1/subscriptions по цене/проценту, владелец из сессии, проверка пары через внутренний каталог, исходная котировка и предупреждения. Транзакционная квота/Telegram, active даже при true; реальный PostgreSQL/race-прогон пройден, runtime ещё отсутствует; journal/outbox создания подключён в SUB-011.
- [MKT-018](backlog/tickets/MKT-018.md). [Внутренний каталог](../internal/marketdata/catalogrpc/README.md): unary gRPC Fetch и Go-клиент, полный снимок с исходным UpdatedAt, mTLS-доступ только API, бюджет 1 с и передача отмены источнику. Проверено на защищённом loopback и локальном Binance fixture; runtime не подключён.
- [SUB-002](backlog/tickets/SUB-002.md). [Транзакционная квота API](../internal/api/subscription/README.md): общий user lock, проверка Telegram, создание с лимитом 15 и однократное освобождение terminal-слота. Миграция 006 и конкурентные сценарии проверены на disposable PostgreSQL с race detector; HTTP-создание подключает квоту в SUB-003 и журнал в SUB-011; runtime ещё отсутствует.
- Последняя работа над процессом: добавлен проектный skill `binance-ticket-session` с автоматическим выбором и обязательным чтением через AGENTS.md. Он восстанавливает контекст одного тикета, сообщает модель из карточки и ведёт до проверки и передачи результата. Проверены формат skill, YAML, локальные ссылки и целостность бэклога; выбор skill в отдельном новом чате ещё не проверялся. Короткие запросы и проверка новой сессии — в WORKING.md. Продуктовые тикеты этим не закрывались.

## Что существует

- Последняя котировка: [контракт и подключение](../internal/marketdata/latest/README.md), `proto/marketdata/v1/latest.proto`; `internal/marketdata/latest.NewClient` читает через gRPC с бюджетом 1 с, права только API/matcher. После рестарта до обслуживания runtime начинает durable поколение; чтение не подтверждает поток и не меняет время сделки.

- Unary gRPC: [генерация и использование](../proto/README.md), `python3 scripts/generate_proto.py`, CI-проверка `--check`. `internal/testutil/grpctest.Start` запускает реальный loopback TCP сервер с Cleanup; проверены ответ, deadline, отмена и недоступность. Probe — только тестовый контракт. Сервисные полномочия готовы: [mTLS и права](../internal/platform/serviceauth/README.md), ADR SET-008; защищённый loopback — `grpctest.StartWithClientOptions`.

- Market-data storage: [контракт](../internal/marketdata/storage/README.md), миграция `migrations/market_data/001_quotes_outbox.sql`. BeginGeneration → SetStatus/Accept, Latest, Claim/MarkPublished; gap сохраняется исторически. Перед чтением после рестарта runtime обязан начать новое поколение. `check_postgres.py` проверяет раздельные БД API/market_data на disposable стенде; рабочая миграция этой сессией не применялась.

- [Согласованные требования](requirements.md).
- WebSocket transport: `internal/marketdata/stream.Read` и `ReadWithCommands(ctx, endpoint, config, commands, handle)`, gorilla/websocket v1.5.3. Общий бюджет команд/pong по умолчанию 5/с, ограниченная очередь pong, отмена и ожидание writer при выходе. `Reconnect` восстанавливает URL/начальные команды после сбоя, сообщает разрыв и ждёт bounded backoff с jitter. Синхронные callbacks должны учитывать context. Ротация через 23 часа/serverShutdown реализована в Reconnect и Reconciler. `NewReconciler`/`Reconcile`/`Close` управляют планом каталога, ACK и повторами; runtime wiring ещё отсутствует.
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
- Локальный PostgreSQL: [запуск и проверка](../infra/postgres/README.md), compose и init в `infra/postgres/`; рабочий проект `binance-storage` настроен на loopback:5432; при проверке SET-009 контейнер остановлен. Локальные пароли в игнорируемом `.env`; миграция аккаунтов API существует, в рабочую БД этой сессией не применялась. Runner — `python3 scripts/migrate.py --owner <owner>`, отдельно от runtime; [контракт](../migrations/README.md).
- Подключение Go к PostgreSQL: `internal/platform/postgres.Open`, драйвер pgx v5.11.0. `internal/testutil/postgrestest.Open(t)` и `scripts/check_postgres.py` проверяют реальные транзакции на disposable проекте binance-go-check; обычный go test пропускает opt-in тесты. Первый прикладной репозиторий — `internal/api/account`; схема аккаунтов применяется runner на тестовом стенде.
- Локальная Kafka: [запуск и проверка](../infra/kafka/README.md), Compose/init и изолированный smoke-check в `infra/kafka/`. Рабочий проект `binance-broker` настроен на loopback:9092, префикс topics `local`; при проверке SET-009 контейнер уже давно остановлен (Exited 143). Обычный volume без отдельной квоты; нагрузочный override требует отдельную ФС 40 GiB. Producer PriceObserved: [marketdata/publish](../internal/marketdata/publish/README.md); consumers/runtime отсутствуют. Основа Go-producer — [internal/platform/kafka](../internal/platform/kafka/README.md): New(ctx, Config) и Publish(ctx, ...Record), stable transactional.id, классификация fencing/неизвестного commit. `bash infra/kafka/verify-go.sh` поднимает и удаляет уникальный disposable-стенд; fault proxy — `internal/testutil/kafkafault`.
- Общий конверт `internal/contracts/event`: JSON-метаданные, версия 1, Validate и типизированные ошибки; SubscriptionChanged — `internal/contracts/subscriptionchanged`; PriceObserved — `internal/contracts/priceobserved`; AlertTriggered — `internal/contracts/alerttriggered`; все четыре схемы готовы; Kafka publish PriceObserved готов, runtime отсутствует.
- Go CI: `.github/workflows/go.yml`; команды воспроизведения — в README. Публикация коммита отдельно запустит первую проверку на GitHub.
- Локальный Git-репозиторий: основная ветка `main`. SHA текущего снимка определяется через `git log -1`; его не нужно дублировать внутри самого коммита.
- GitHub: [irhinandrej399-jpg/binance-price-alerts](https://github.com/irhinandrej399-jpg/binance-price-alerts), публичный. `origin` указывает на этот репозиторий; публикация текущей main поручена пользователем. Состояние синхронизации проверяется через `git status -sb` и SHA удалённой ветки.
- Агент самостоятельно коммитит завершённый тикет по [GIT.md](GIT.md), включая карточку, индекс и STATE, с описанием изменений и проверок.

## Решения и препятствия

[DEC-009](decisions/DEC-009.md) принят 27 октября: REST/JSON для внешнего API, gRPC/Protobuf для прямых внутренних вызовов, Kafka для событий. Карточки интерфейсов уточнены; генерация готова в SET-007, Latest-котировки — в MKT-006; остальные прикладные сетевые адаптеры ещё не реализованы.

DEC-001–009 приняты. DEC-004 пересматривает прежнюю гарантию уведомления по снимку создания и terminal-срабатывание до доставки. Planned-предпосылки MAT-012, SUB-017/018/019/020 и NTF-014 включены в очередь.

Выбраны PostgreSQL и Kafka/KRaft, полный профиль стенда закреплён DEC-008. SQL-replay — SUB-022, cleaner Kafka — OPS-014, ограничители — OPS-015/016. Численные стоимости операций требуют проверки на будущих схемах; скорость cleaner пока считается нулевой. Telegram-политика и freshness согласованы, но не реализованы. Наличие каталога из четырёх сервисов не означает готовность этих решений.

## Следующий рекомендуемый шаг

По очереди следующий — [SUB-017](backlog/tickets/SUB-017.md): сохранить допуск одной попытки доставки. Перед началом проверить зависимости и сессионный размер. Атомарный запрос работы и защищённый RPC готовы в SUB-013/014; публикация delivery_outbox, runtime cmd/api и полный конвейер доставки остаются отдельными задачами.

`python3 scripts/backlog.py next` показывает следующий пункт из ORDER.md, предварительно предлагая завершить активную работу. Идти по этой очереди; разделы каталога и номера ID сами по себе порядок не задают.

## Проверка состояния

- `python3 scripts/backlog.py check` — целостность карточек, зависимостей, статусов индекса и последовательности ORDER.md.
- `python3 scripts/backlog.py show SUB-017` — следующая карточка и статус её зависимостей.
- `git status --short`, соответствующий diff и `git log -1`.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -v` — проверки очереди и инструмента коммитов во временных репозиториях.
- `GOTOOLCHAIN=local go list ./...`, `GOTOOLCHAIN=local go test ./...`, `GOTOOLCHAIN=local go build ./...` — проверки каркаса. Тесты цены проверяют доменный контракт; успешная компиляция cmd не подтверждает runtime-сценарии.

При завершении работы обновлять только изменившиеся факты: активный ID, последний результат, остающиеся препятствия и следующий шаг. Подробные команды и результаты оставлять в карточке.
