# Явный source provisioning — OPS-078

`source-provision` — операторская команда перед первым запуском четырёх сервисов.
Она использует готовые `kafkasource.Metadata` и
`intake.Store.Bootstrap/ReadProgress`. Runtime по-прежнему не bootstrap-ит SQL.
Команда не запускает миграции, не создаёт topics и не мигрирует legacy.

## Порядок первого запуска

1. Остановить все producers/consumers этой среды, включая API relay и market-data.
   Удерживать их остановленными до успешного provisioning/verify. Команда не
   является распределённой блокировкой и не может доказать остановку сервисов.
2. Применить штатные migrations, выполнить topics init (16 prices, 1
   subscription-changes, 16 delivery-requests). Использовать один доверенный Kafka
   cluster. Соединение Kafka соответствует текущему plaintext runtime; TLS/SASL
   эта команда не настраивает. Не адресовать старый dataset как новый.
3. Собрать бинарный файл и выбрать постоянный путь inventory на durable volume
   **вне SQL и вне Git**, например `/srv/binance/inventory/sources.json`.
   Родительский каталог должен существовать. Путь принадлежит оператору.
4. Передать `KAFKA_BROKERS` и ровно один из `NOTIFICATIONS_DATABASE_URL` либо
   `NOTIFICATIONS_DATABASE_URL_FILE`. Второй содержит путь к mounted secret file
   с DSN. Нужны БД `notifications` и роль `notifications_runtime`, не owner.
   Пароли не передаются через argv; команда не печатает raw driver/broker errors.
5. Выполнить:

```sh
go build -o /tmp/source-provision ./cmd/source-provision
/tmp/source-provision --mode init --prefix local --inventory /srv/binance/inventory/sources.json > /srv/binance/inventory/runtime-env.json
```

`init` требует отсутствующий файл. Он подтверждает один ClusterID, разные ненулевые
TopicID, точные partitions 0..15 / 0 / 0..15 и полные нулевые earliest/latest
read_committed vectors **всех трёх topics**. До SQL записывается version=1 inventory:
синхронизированный временный файл, exclusive link без замены существующего пути,
fsync каталога. Затем Bootstrap атомарно создаёт binding и 16 checkpoints.
Legacy jobs/checkpoints без binding отвергаются готовым SQL-контрактом.
При ошибке stdout не содержит настроек; код завершения 1. Не использовать файл
перенаправления stdout, если команда завершилась ошибкой.

## Inventory и передача в runtime

JSON inventory содержит `Version`, `Environment`, `Sources` (ClusterID, 16-byte
TopicID, Topic), `Binding` (ConsumerID, уникальная случайная Generation, Source),
полный `PricesBootstrap` 0..15=0 и стабильные `Runtime` IDs. Это несекретный
операторский артефакт. Сохранить его вместе с планом backup/restore; не редактировать
IDs вручную. Наличие файла само по себе не доказывает завершение SQL bootstrap.

При успешном выполнении stdout — JSON-объект строковых runtime env, **не shell
script**. Передавать значения через env API/Compose environment, без `eval` или
`source`. JSON в `PRICES_BOOTSTRAP` передаётся одной строкой без дополнительных
кавычек. Секреты, endpoints и PKI добавляет конфигурация OPS-074/075.

| Runtime | Ключи из успешного stdout |
| --- | --- |
| market-data | PRICES_TOPIC, MARKET_DATA_RELAY_TRANSACTIONAL_ID |
| api | SUBSCRIPTION_CHANGES_TOPIC, DELIVERY_REQUESTS_TOPIC, SUBSCRIPTION_RELAY_TRANSACTIONAL_ID, DELIVERY_RELAY_TRANSACTIONAL_ID |
| matcher | KAFKA_CLUSTER_ID, PRICES_TOPIC/PRICES_TOPIC_ID, SUBSCRIPTION_CHANGES_TOPIC/SUBSCRIPTION_CHANGES_TOPIC_ID, PRICES_BOOTSTRAP, MATCHER_ENVIRONMENT, MATCHER_GROUP, MATCHER_REPLICA_ID |
| notifications | KAFKA_CLUSTER_ID, DELIVERY_REQUESTS_TOPIC/DELIVERY_REQUESTS_TOPIC_ID, NOTIFICATIONS_GROUP, NOTIFICATIONS_CONSUMER_ID, NOTIFICATIONS_GENERATION |

Transactional IDs и groups привязаны к prefix, replica ID первоначального matcher
— `one`. Они сохраняются при рестарте, как и первоначальный нулевой vector.
Нельзя генерировать новые IDs для обычного restart или копировать inventory в
независимую среду. SQL runtime сам проверяет durable checkpoints/provenance.

## Рестарт и прерывание

Перед запуском остановленной среды проверить сохранённый inventory:

```sh
/tmp/source-provision --mode verify --inventory /srv/binance/inventory/sources.json > /srv/binance/inventory/runtime-env.json
```

`verify` ничего не пишет в SQL/inventory. Он сверяет все три metadata/partition
набора и persisted binding/partition/next для всех 16 intake partitions.
Ненулевые offsets и непустая Kafka разрешены. Нельзя подменить старую identity
нынешней metadata. Отсутствующий/частичный bootstrap, чужая generation/source,
legacy или недоступность — отказ. Verify не доказывает доступность retained
истории: её дополнительно проверяют готовые runtime readers.

Если `init` прерван:

- Inventory отсутствует: SQL ещё не вызывался этой командой; проверить каталог и
  права, повторить init при остановленных сервисах.
- Inventory существует: сохранить его, выполнить verify. При неопределённом SQL
  commit verify может уже подтвердить полный bootstrap.
- При незавершённом bootstrap выполнить `--mode resume --inventory <тот же путь>`.
  Все выбранные IDs берутся только из файла. Если все checkpoints отсутствуют,
  повторно требуется пустой broker и вызывается прежний Bootstrap. Если binding
  уже полный, только проверяется прогресс; никакого reset и требования пустоты.
  Частичные checkpoints и legacy не исправляются автоматически.
- При mismatch, потере inventory, повреждении SQL или непустом broker без
  bootstrap остановиться для отдельного операторского плана. Не удалять inventory,
  не выполнять новый init, не обнулять SQL. Restore/смена источника — OPS-076/077.

Сохранять inventory даже если команда вернула ошибку записи/fsync либо SQL:
возможен частичный результат файловой системы или неизвестный исход commit.
Пользователь должен сам исключить внешние записи и удаление файла между
проверками. Клонированные broker/SQL с сохранёнными IDs неразличимы.

## Проверки

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./internal/platform/sourceprovision ./cmd/source-provision
GOCACHE=/tmp/binance-go-build python3 scripts/check_e2e_environment.py --package ./cmd/source-provision
```

Unit: отказ неверного/неполного dataset/inventory, interrupted/resumed bootstrap,
partial/legacy и сохранение IDs/progress. Disposable E2E-005: missing bootstrap
после сохранения inventory, fresh command provisioning через secret file/runtime
роль, Kafka запись + реальный SQL AcceptBound, verify/resume без reset,
Kafka/SQL mismatch. Без opt-in — явный SKIP; с opt-in недоступность — FAIL.
Compose/deployment и внешние Binance/Telegram сюда не входят.
