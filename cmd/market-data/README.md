# Минимальный market-data (OPS-048)

Один фиксированный raw trade поток → durable market_data latest/outbox → serial
Kafka relay. Используются готовые Decode, storage, ReconnectPrepared, relay и
health. SIGTERM/SIGINT обрабатываются владельцем процесса (OPS-009).

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
# Также задайте RPC/каталог из раздела OPS-069 ниже.
GOTOOLCHAIN=local go run ./cmd/market-data
```

Все настройки обязательны. URL — ровно `/ws/<lowercase-pair>@trade`, ws/wss,
без userinfo/query/fragment; время raw trade трактуется в миллисекундах.
Пара — uppercase ASCII с суффиксом USDT. Каталог/актуальная торгуемость пары
не проверяются. Все владельцы relay используют один stable transactional.id
фиксированного шарда; случайный ID на каждый запуск недопустим.

Startup ограничен 15 с: credentials, bind HTTP/RPC, подключение своей БД, наличие схемы и
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

При SIGTERM/SIGINT HTTP снимается с обслуживания, отменяются ingest/Accept,
публикация и reconnect/backoff. HTTP имеет 2 с на завершение; ingest и relay
присоединяются до закрытия SQL pool. Весь процесс имеет предел 20 с, включая
существующий handshake ≤10 с и финальную SQL-запись/проверку ≤5 с. Превышение
предела — явная ошибка и exit 1, а не успешная graceful остановка.

После join сохраняются unconfirmed + gap_detected=true с fencing последнего
поколения владельца. Если поколение уже сменил другой процесс, stale status
не меняет его состояние. Ошибка записи gap или чтения pending-публикаций
выводится явно и завершает процесс с ошибкой. Штатное завершение сообщает
feed disconnected и число pending publications без payloads/credentials.

Принятый буфер — durable outbox, поэтому остановка не ждёт его полного
опустошения. Отменённая SQL-транзакция не объявляется принятой; committed
latest/outbox сохраняются. Неопределённый Kafka commit оставляет исходные
bytes/event_id без SQL-отметки; после DB-clock lease до 30 с следующий
владелец с тем же transactional.id повторяет публикацию. Возможны дубли.
При недоступной БД или аварийном выходе запись разрыва не гарантируется;
следующий BeginGeneration всё равно отмечает исторический gap.

Процессный тест скрывает успешный EndTxn response готовым kafkafault dialer
в test child, который вызывает тот же execute/run с production обработчиком
сигналов. Независимый consumer подтверждает commit до SIGTERM. Настоящий
executable затем повторяет bytes после lease, принимает новую сделку и
закрывает активный WebSocket по SIGTERM. Отдельный зависший handshake
проверяет завершение reconnect без принудительного kill из harness.

Ограничения: нет динамического каталога потоков, backfill, нескольких
feed-реплик, полного E2E, capacity и нагрузочных гарантий. Kafka/SQL retry и
возможные дубли после неопределённого commit следуют контракту relay/storage.
Живое соединение Binance этим тестом не проверяется.

## Catalog/Latest RPC (OPS-069)

Тот же процесс обслуживает один mTLS listener с Catalog/Fetch и Quotes/Latest.
Дополнительно обязательны:

```sh
export RPC_ADDR=127.0.0.1:9081
export RPC_CERT_FILE=/path/to/market-data-cert.pem
export RPC_KEY_FILE=/path/to/market-data-key.pem
export RPC_CA_FILE=/path/to/service-ca.pem
export API_SERVICE_URI=spiffe://local/api
export MATCHER_SERVICE_URI=spiffe://local/matcher
export CATALOG_BASE_URL=https://data-api.binance.vision
```

URI API и matcher должны отличаться. Общая serviceauth policy разрешает API
Fetch/Latest, matcher только Latest; остальные identities не получают прав.
Клиент проверяет DNS SAN серверного сертификата через свою явную authority.
PEM берутся из внешних файлов; ключ доступен только владельцу/secret mount.
Catalog использует готовый catalog.Client без кэша; URL задаёт origin без пути,
query, userinfo и fragment. Для тестов обязателен loopback fixture origin.
HTTP transport каталога принадлежит процессу, не использует proxy/redirect,
ограничен RPC budget 1 с и закрывается при остановке.
Latest читает storage.Repository своей БД; исходные цена, source ID, revision,
времена, unconfirmed и sticky gap сохраняются. До запуска listener выполняется
BeginGeneration фиксированной пары, чтобы прежний live не пережил рестарт.

Startup проверяет credentials/политику, bind HTTP/RPC, схему и Kafka за прежние
15 с. SIGTERM и terminal failure снимают готовность, отменяют ingest/relay,
останавливают RPC с отменой и join активных handlers, затем закрывают свою БД.
Это входит в существующий общий shutdown budget 20 с.

Для фиксированного raw-профиля ожидание запуска: `/healthz` → 200 и успешный
mTLS Fetch/Latest. `/readyz` остаётся **503**, `feed=unconfirmed` после первой
сделки; при disconnect также 503. Этот профиль не доказывает live и не должен
проверяться ожиданием readyz=200. Динамический каталог потоков сюда не входит.

```sh
GOCACHE=/tmp/binance-go-build PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_marketdata_process.py --package ./cmd/market-data
```

Runner создаёт disposable PostgreSQL/Kafka; тест выпускает ephemeral CA и
использует только loopback catalog/raw fixtures. Реальный Binance не вызывается.
