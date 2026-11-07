# Минимальный market-data (OPS-048)

Один фиксированный raw trade поток → durable market_data latest/outbox → serial
Kafka relay. Используются готовые Decode, storage, ReconnectPrepared, relay и
health. Это executable для последующей проверки shutdown OPS-009.

Перед запуском отдельно поднимите PostgreSQL/Kafka по `infra/*/README.md`,
примените `migrations/market_data` через `scripts/migrate.py --owner market_data`
и создайте prices topic штатным init (16 стабильных партиций). Runtime миграции
не выполняет. Передайте собственный DSN роли `market_data_runtime` в БД
`market_data` через окружение, не аргумент командной строки:

```sh
export HTTP_ADDR=127.0.0.1:8081
# MARKET_DATA_DATABASE_URL задаётся отдельно из локального секрета.
export RAW_TRADE_PAIR=BTCUSDT
export RAW_TRADE_URL=wss://stream.binance.com:9443/ws/btcusdt@trade
export KAFKA_BROKERS=127.0.0.1:9092
export PRICES_TOPIC=local-prices
export MARKET_DATA_RELAY_TRANSACTIONAL_ID=local-market-data-relay-shard-0
GOTOOLCHAIN=local go run ./cmd/market-data
```

Все настройки обязательны. URL — ровно `/ws/<lowercase-pair>@trade`, ws/wss,
без userinfo/query/fragment; время raw trade трактуется в миллисекундах.
Пара — uppercase ASCII с суффиксом USDT. Каталог/актуальная торгуемость пары
не проверяются. Все владельцы relay используют один stable transactional.id
фиксированного шарда; случайный ID на каждый запуск недопустим.

Startup ограничен 15 с: bind HTTP, подключение своей БД, наличие схемы и
инициализация Kafka producer epoch без Claim. Затем relay получает свой epoch
перед Claim. Ошибка startup закрывает ресурсы; ошибки пользователю выводятся
без DSN, URL, брокерских response bodies и исходных payloads.

Перед **каждым** dial BeginGeneration отсекает старые callbacks. Decode
передаёт предварительную ревизию; storage под блокировкой назначает durable
sequence, сохраняет source ID между соединениями/рестартами, latest и outbox
в одной synchronous транзакции. Случайный 128-bit event_id/correlation_id и
occurred_at сохраняются с исходными bytes. Ошибка SQL не считается Accept;
в этом минимальном runtime ошибка ingest завершает владельца, после рестарта
дедупликация использует committed source ID. Разрыв сохраняется через SetStatus,
новая попытка создаёт новое поколение; старый status callback не меняет snapshot
после SQL fencing. Relay публикует сохранённые bytes и подтверждает SQL только
после Kafka commit; его terminal ошибка останавливает HTTP и ingest, компоненты
присоединяются до закрытия pool.

`GET /healthz` = 200. `GET /readyz` = 503 с `feed=unconfirmed` после начала
поколения, либо `starting`/`disconnected`. У фиксированного raw endpoint нет
независимого явного live-подтверждения; dial, ACK и первая сделка его не заменяют.
Здоровый процесс может durable принимать/публиковать цены и оставаться unconfirmed.
Не используйте readiness 200 как условие запуска этого минимального профиля.

Проверка настоящего executable с синтетическим raw feed и независимым
read_committed consumer, включая restart, fencing, startup и terminal failure:

```sh
GOCACHE=/tmp/binance-go-build PYTHONDONTWRITEBYTECODE=1 \
  python3 scripts/check_marketdata_process.py --package ./cmd/market-data
```

Runner использует только disposable PostgreSQL/Kafka и проверяет cleanup.
Без opt-in процессные SQL/Kafka тесты явно SKIP. Обычная проверка config/startup:
`GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./cmd/market-data`.

Ограничения: SIGTERM пока завершает процесс обычным способом ОС, graceful/drain
ещё OPS-009. Нет динамического каталога, внутренних RPC, backfill, нескольких
feed-реплик, полного E2E, capacity и нагрузочных гарантий. Kafka/SQL retry и
возможные дубли после неопределённого commit следуют контракту relay/storage.
Живое соединение Binance этим тестом не проверяется.
