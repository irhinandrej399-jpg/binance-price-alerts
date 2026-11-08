# Market-data: каталог и raw trade (MKT-019)

Каталог поддерживаемых USDT-пар → prepared stream reconciler → durable
market_data latest/outbox → serial Kafka relay. Один процесс владеет всеми
группами потоков. SIGTERM/SIGINT обрабатываются владельцем процесса.

Перед запуском поднимите PostgreSQL/Kafka по `infra/*/README.md`, примените
`migrations/market_data` через `scripts/migrate.py --owner market_data` и создайте
prices topic штатным init (16 стабильных партиций). Runtime миграции не выполняет.
Передайте DSN роли `market_data_runtime` в БД `market_data` через окружение,
не аргумент командной строки:

```sh
export HTTP_ADDR=127.0.0.1:8081
# MARKET_DATA_DATABASE_URL задаётся отдельно из локального секрета.
export RAW_TRADE_URL=wss://stream.binance.com:9443/ws
export CATALOG_BASE_URL=https://data-api.binance.vision
# Необязательно: по умолчанию 1m, допустимо 100ms..1h.
export CATALOG_REFRESH_INTERVAL=1m
export KAFKA_BROKERS=127.0.0.1:9092
export PRICES_TOPIC=local-prices
export MARKET_DATA_RELAY_TRANSACTIONAL_ID=local-market-data-relay-shard-0
export RPC_ADDR=127.0.0.1:9081
export RPC_CERT_FILE=/path/to/market-data-cert.pem
export RPC_KEY_FILE=/path/to/market-data-key.pem
export RPC_CA_FILE=/path/to/service-ca.pem
export API_SERVICE_URI=spiffe://local/api
export MATCHER_SERVICE_URI=spiffe://local/matcher
GOTOOLCHAIN=local go run ./cmd/market-data
```

Остальные настройки обязательны. RAW_TRADE_URL — bare `/ws`, ws/wss, без
userinfo/query/fragment; время raw trade трактуется в миллисекундах.
Прежний fixed URL `/ws/btcusdt@trade` отклоняется; RAW_TRADE_PAIR больше
не используется. Для тестов оба Binance URL указывают только на loopback fake.
Все владельцы relay используют один stable transactional.id фиксированного
шарда; случайный ID на каждый запуск недопустим.

## Общий обслуживаемый каталог

Готовый catalog.Client выбирает TRADING + spot + quoteAsset=USDT. Runtime
оставляет символы `^[A-Z0-9]+USDT$`, совместимые с существующими контрактами
storage/PriceObserved; остальные (включая Unicode) не выдаёт через Catalog RPC
и не включает в ingest. Дубли символов объединяются. streamplan.Build разбивает
отсортированные raw `@trade` потоки на группы до 1024.

Первый Fetch обязателен при startup. Refresh выполняется с заданным интервалом;
HTTP timeout — 1 с, proxy/redirect отключены. Ошибка Fetch оставляет последний
успешный план, исходный UpdatedAt и безопасное сообщение в stderr. Успешный
пустой каталог снимает все потоки. Переподключение управляется reconciler.

Catalog/Fetch читает общий применённый снимок, без отдельного HTTP-запроса:
новые пары видны после ACK; удалённые исчезают до снятия старых подписок.
При смене набора prepared reconciler снимает все старые подписки и присоединяет
callbacks перед созданием новых групп. Повтор того же плана не меняет поколения.
ACK подтверждает план подписки, но не live. Снимок может оставаться доступным
во время reconnect или отказа REST; возраст определяется сохранённым UpdatedAt.

Удаление пары не удаляет её latest/outbox и пользовательские подписки. Последняя
котировка остаётся доступна с unconfirmed, историческим gap и собственным возрастом.
Новые подписки API отклоняются по отсутствию пары в каталоге; существующие остаются
на прежних условиях и не получают выдуманных цен. При возвращении пары начинается
новое поколение с прежней границей source ID. Backfill внешних сделок отсутствует.

## Durable fencing и публикация

Startup ограничен 15 с: credentials, bind HTTP/RPC, своя БД/схема, Kafka producer
epoch без Claim, fencing всех сохранённых пар (включая удалённые за downtime) и
первый каталог. До Latest RPC прежний live сбрасывается через BeginGeneration.
Затем каждый dial группы готовит отдельные durable поколения всех её пар.
Callbacks привязаны к immutable набору и поколениям; неожиданный символ — ошибка.

Decode передаёт предварительную ревизию; storage под блокировкой назначает durable
sequence, сохраняет source ID между соединениями/рестартами, latest и outbox в одной
synchronous транзакции. Исходные цена, trade/event/receive time не подменяются.
Повтор source ID не создаёт нового outbox. Sticky gap сохраняется при reconnect,
смене плана и остановке. SQL-ошибка не считается Accept и завершает владельца.

Relay публикует исходные bytes/event_id и подтверждает SQL после Kafka commit.
Неопределённый commit оставляет каноническую работу для повтора после DB-clock lease
до 30 с; возможны дубли. Терминальная ошибка любого компонента останавливает весь
процесс. Ошибки не раскрывают credentials, remote response bodies или payloads.

## Catalog/Latest RPC (OPS-069)

Один mTLS listener: API имеет Fetch/Latest, matcher — только Latest. URI должны
отличаться; остальные identities не получают прав. Клиент проверяет DNS SAN
сертификата через явную authority. PEM берутся из внешних файлов/secret mount.
Latest читает storage.Repository своей БД; ревизии и предупреждения сохраняются.

## Готовность и остановка

Операционное ожидание запуска: `/healthz` → 200, mTLS Fetch выдаёт применённый
каталог, после принятой сделки Latest возвращает точную цену и предупреждение.
`/readyz` остаётся **503**, `feed=unconfirmed`: dial, ACK и первая сделка не являются
независимым live-подтверждением. Принимать и публиковать цены можно в этом состоянии.
Не используйте readyz=200 как условие запуска. Ни runtime, ни smoke не выставляют
SQL live ради готовности.

SIGTERM снимает initialized, отменяет refresh/ingest/Accept/relay и RPC, присоединяет
все handlers, группы и retry loops до pool.Close. HTTP имеет 2 с; общий watchdog —
20 с, включая handshake ≤10 с и финальную SQL-запись/проверку ≤5 с. После join все
подготовленные пары получают fenced unconfirmed + gap=true; stale от нового владельца
не меняет его состояние. Ошибка gap/pending check или превышение deadline — exit 1.
Штатная остановка сообщает число pending publications без payloads/credentials.

Принятый буфер — durable outbox: остановка не ждёт его опустошения. При аварии или
недоступной БД запись gap не гарантируется; следующий startup всё равно fence-ит
сохранённые пары. Несколько одновременных feed-владельцев не поддерживаются.

## Проверка

```sh
GOCACHE=/tmp/binance-go-build PYTHONDONTWRITEBYTECODE=1 \
  python3 scripts/check_marketdata_process.py --package ./cmd/market-data
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./cmd/market-data
```

Runner создаёт disposable PostgreSQL/Kafka и проверяет cleanup. Малый динамический
smoke на binancefake проверяет разные цены двух пар, добавление/удаление, reconnect,
дедупликацию и неизменность принятого outbox после SIGTERM. Прежние process tests
проверяют read_committed bytes, restart, SQL fencing, mTLS policy, startup/terminal
failures и неопределённый Kafka commit с повтором после lease. Loopback-тест refresh
проверяет сохранение плана при HTTP 503 и снятие потоков при пустом каталоге.
Без opt-in SQL/Kafka процессные тесты явно SKIP. Реальные Binance/Telegram,
нагрузка и публичный deployment в этот результат не входят.
