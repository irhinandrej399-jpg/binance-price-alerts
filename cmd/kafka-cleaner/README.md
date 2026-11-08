# Локальная maintenance-команда Kafka cleaner

Сначала подготовьте явный полный inventory и один постоянный приватный каталог
для **одной** source/partition. Согласованная модель и recovery —
[cleaner](../../internal/cleaner/README.md), [ADR OPS-014](../../docs/decisions/OPS-014.md).
Команда не запускает background daemon и не является пятым микросервисом.

```sh
GOTOOLCHAIN=local go build -o /tmp/binance-kafka-cleaner ./cmd/kafka-cleaner
mkdir -m 700 /absolute/durable/cleaner-prices-p0
/tmp/binance-kafka-cleaner -operation init \
  -state-dir /absolute/durable/cleaner-prices-p0 -config /private/cleaner-clients.json \
  -inventory /private/prices-p0-inventory.json
/tmp/binance-kafka-cleaner -operation inspect -state-dir /absolute/durable/cleaner-prices-p0
/tmp/binance-kafka-cleaner -operation clean \
  -state-dir /absolute/durable/cleaner-prices-p0 -config /private/cleaner-clients.json \
  -max-offsets 1000 -timeout 15s
```

`/absolute/durable/...` — реальный persistent local storage, не временная папка.
Все исполнители обязаны использовать один каталог. Примеры inventory ниже —
шаблоны: IDs берутся из доверенного provisioning/существующих SQL generation и
Kafka source, а не приписываются старым checkpoints из текущей metadata.

`cleaner-clients.json` содержит только Kafka broker addresses и mTLS client
credentials для защищённых exporter endpoints; SQL DSN туда не входят:

```json
{
  "brokers": ["127.0.0.1:9092"],
  "endpoints": {
    "matcher": {
      "Address": "127.0.0.1:9201", "ServerName": "matcher.internal",
      "CAFile": "/private/ca.pem", "CertFile": "/private/cleaner.pem",
      "KeyFile": "/private/cleaner.key"
    }
  }
}
```

Kafka соединение соответствует текущему локальному plaintext профилю; TLS/ACL
Kafka этим тикетом не добавляются. RPC всегда mTLS с явным ServerName и правом
cleaner на единственный ReadProgress. Env родителя и DSN команда не использует.
Config не логируется; ошибки безопасные, без содержимого config/ключей/ответов SQL.

`prices-p0-inventory.json` (полный обязательный набор этой partition):

```json
{
  "source": {
    "ClusterID": "EXPECTED_KAFKA_CLUSTER_ID",
    "TopicID": [1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
    "Topic": "local-prices"
  },
  "partition": 0,
  "consumers": [
    {"name": "prices-stable", "kind": "prices", "id": "EXACT_SQL_SCOPE", "endpoint": "matcher"}
  ]
}
```

TopicID — ровно 16 исходных байт UUID в JSON array, не произвольное число из
примера. Для changefeed используйте `kind: "projection"`, ID текущей **и всех
остальных обязательных исторических** projection, а также отдельный реестр
правильного source/partition. Для notifications:

```json
{"name":"notifications-generation-1","kind":"notifications","id":"EXACT_CONSUMER_ID","generation":"EXACT_SQL_GENERATION","endpoint":"notifications"}
```

Такой descriptor — отдельный `consumer.json`, а endpoint `notifications`
добавляется в client config с собственными CA/credentials. Register/retire:

```sh
/tmp/binance-kafka-cleaner -operation register \
  -state-dir /absolute/durable/cleaner-delivery-p0 -config /private/cleaner-clients.json \
  -consumer /private/consumer.json
/tmp/binance-kafka-cleaner -operation retire \
  -state-dir /absolute/durable/cleaner-delivery-p0 -config /private/cleaner-clients.json \
  -consumer /private/consumer.json
```

Retire — явное прекращение требования этой exact identity/generation к истории,
а не команда restart. Descriptor передаётся целиком, `retired` во входе запрещён.
После retirement tombstone сохраняется; новое SQL поколение получает новый Name.
Новый consumer ниже сохранённого floor не регистрируется: сначала отдельный
явный bootstrap владельца. Никто не сбрасывается на latest автоматически. Нынешний notifications Bootstrap
создаёт только next=0; ненулевой bootstrap/migration требует отдельного плана
владельца и этой командой не реализуется.

`-max-offsets`: 1..1 000 000 (default 1000), только одна partition, не bytes.
`-timeout`: >0..30s (default 15s), включает ожидание lock/RPC/broker; OS fsync —
синхронный filesystem вызов. Exit 0 — подтверждённая операция/noop, 1 — отказ/
неизвестный результат, 2 — ошибка config/флагов. Clean печатает JSON, например:

```json
{"status":"deleted","start":10,"minimum":25,"target":20,"floor":20,"low_watermark":20,"attempted":true,"broker_code":0}
```

`unknown` означает, что запрос мог выполниться: не удаляйте state и не откатывайте
floor. Следующий clean заново проверит все proofs и broker log-start. Missing
или invalid state запрещает работу; init никогда не выполняется автоматически.
Реальная проверка на уникальном disposable стенде с race и cleanup:

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_cleaner_process.py --package ./internal/cleaner
```
