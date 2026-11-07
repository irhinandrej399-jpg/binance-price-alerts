# Минимальный matcher — OPS-053

`go run ./cmd/matcher` запускает одну готовую `replica`: Kafka group → SQL
fencing → snapshot/replay/live → Latest → canonical RequestDelivery API.
Миграции matcher применяются отдельно через `scripts/migrate.py --owner matcher`.
Прямого доступа к БД API или market-data у процесса нет.

Все настройки обязательны, defaults для bootstrap/identity отсутствуют:

| Переменная | Значение / пример |
| --- | --- |
| `HTTP_ADDR` | `127.0.0.1:8082` |
| `MATCHER_DATABASE_URL` | secret DSN БД `matcher`, роль `matcher_runtime` |
| `KAFKA_BROKERS` | список `host:port` через запятую |
| `PRICES_TOPIC`, `SUBSCRIPTION_CHANGES_TOPIC` | разные topics с 16 и 1 partitions |
| `MATCHER_ENVIRONMENT` | устойчивое имя окружения, например `local` |
| `MATCHER_GROUP` | общая Kafka group реплик |
| `MATCHER_REPLICA_ID` | устойчивое имя, уникальное среди одновременно работающих реплик |
| `PRICES_BOOTSTRAP` | JSON object всех ключей `"0"`…`"15"` с целыми absolute next offsets ≥0 |
| `API_RPC_ADDR` | `host:port` Snapshot/Replay/Delivery API |
| `LATEST_RPC_ADDR` | `host:port` Quotes.Latest market-data |
| `RPC_SERVER_NAME` | проверяемое DNS-имя обоих RPC серверов |
| `RPC_CA_FILE`, `RPC_CERT_FILE`, `RPC_KEY_FILE` | пути CA и matcher client cert/key, mTLS |
| `SNAPSHOT_TTL`, `REPLAY_TTL` | duration, например `1h`, от 1ms до 24h, целые миллисекунды |
| `HEAD_INTERVAL` | положительный duration, например `1s`, меньше REPLAY_TTL |
| `SNAPSHOT_PAGE_SIZE`, `REPLAY_PAGE_SIZE` | целые 1…1000, например 1000 / 100 |

Identity/topic/group используют ASCII letters/digits/`_.-`, 1…249 bytes,
кроме `.`/`..`. RPC адреса — явные host:port; credentials проверяются при startup,
полномочия — серверами. Секретные DSN/PEM/remote errors не логируются.

Bootstrap выбирается оператором по согласованному latest-price bootstrap до
первой публикации. Например, **только для подтверждённо пустого нового topic**:

```sh
export PRICES_BOOTSTRAP='{"0":0,"1":0,"2":0,"3":0,"4":0,"5":0,"6":0,"7":0,"8":0,"9":0,"10":0,"11":0,"12":0,"13":0,"14":0,"15":0}'
# Остальные обязательные настройки задать из локальной secret-конфигурации.
go run ./cmd/matcher
```

Этот vector сохраняется при restart. SQL Prefix имеет приоритет; брокерный
commit, ноль или конец topic не используются как fallback. Пропавшая позиция
завершает процесс. Одинаковый replica ID в двух процессах запрещён оператором;
команда не вводит отдельного протокола уникальности.

Snapshot request ID — стабильный SHA-256 opaque projection ID с допустимым
prefix. Changes открывает отдельный Source на каждую регистрацию, Register
до replay H, RegisterAt точно по SQL cursor; Close присоединяет Source.
Ни scope, ни TransportKey, ни fencing/grants в cmd не вычисляются.

`GET /healthz` — liveness 200 после HTTP startup. `GET /readyz` проверяет Ping
своей БД и replica.Status: 503 до full view/recovery/первой оценки, 200 после.
SQL/RPC/Kafka/pin ошибки завершают владельца; reset и автоматического нового
snapshot нет. Pin имеет фиксированный срок, HEAD_INTERVAL его не продлевает.
Terminal cleanup отменяет workers, закрывает HTTP, присоединяет replica, затем
закрывает pool/RPC. Обработка SIGTERM/drain будет добавлена в OPS-010; сейчас
сигналы имеют штатное поведение ОС. Restart после SIGKILL проверен отдельно.

Проверка настоящего executable (Docker Desktop, Go и локальная PostgreSQL
secret-конфигурация по infra/postgres/README):

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py --package ./cmd/matcher
```

Готовый OPS-052 harness поднимает и удаляет disposable PostgreSQL/Kafka, mTLS
Snapshot/Replay/Delivery/Latest fixture. Проверяет readiness 503 при задержанной
первой оценке, несколько кандидатов, API commit до ответа, SIGKILL, SQL recovery,
canonical trigger_id/outbox без дубля, Pending/SQL Prefix до broker ack, terminal
потерю SQL authority и startup failures. Без opt-in тест явно SKIP; при opt-in
без конфигурации — FAIL. Это минимальный local runtime; полный E2E четырёх
сервисов, production PKI, graceful shutdown и нагрузка остаются отдельно.
