# Минимальный matcher — OPS-053 / OPS-010 / OPS-062

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
| `KAFKA_CLUSTER_ID` | доверенный Kafka ClusterID обоих topics |
| `PRICES_TOPIC_ID`, `SUBSCRIPTION_CHANGES_TOPIC_ID` | разные ненулевые TopicID: 16 bytes в hex, 32 символа |
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
SIGTERM/SIGINT снимают readiness, отменяют Kafka intake, новые оценки и RPC.
HTTP admission закрывается через Shutdown с бюджетом 2 с. Replica присоединяет
live/changefeed/quote readers и Kafka group, отзывает SQL grants; только после
этого владелец закрывает pool/RPC. Drain опирается на durable Pending/исходы:
заканчивать всех кандидатов при остановке не требуется. Незавершённая цена не
получает broker ack; SQL Prefix остаётся источником восстановления.

Общий предел от сигнала до выхода — 20 с: он включает HTTP, joins, отзыв grants
(бюджет revoke-прохода 3 с) и закрытие ресурсов. Штатная отмена после успешного
cleanup даёт exit 0. Ошибка cleanup, зависимость или превышение общего предела
даёт exit 1 с безопасным сообщением; дедлайн не объявляется graceful успехом.
При нём successor восстанавливает durable работу и fencing, как после crash.
SIGKILL/restart проверен отдельно. Terminal component также отменяет и
присоединяет workers до закрытия ресурсов; SQL/pin reset не выполняется.

Проверка настоящего executable (Docker Desktop, Go и локальная PostgreSQL
secret-конфигурация по infra/postgres/README):

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py --package ./cmd/matcher
```

Готовый OPS-052 harness поднимает и удаляет disposable PostgreSQL/Kafka, mTLS
Snapshot/Replay/Delivery/Latest fixture. Проверяет readiness 503 при задержанной
первой оценке, несколько кандидатов, API commit до ответа, SIGKILL, SQL recovery,
canonical trigger_id/outbox без дубля, Pending/SQL Prefix до broker ack, terminal
потерю SQL authority и startup failures. Дополнительно запускается тот же
настоящий executable: SIGTERM после части кандидатов/неоднозначного API ответа,
exit 0 без harness Kill, отзыв всех 16 grants, неизменные Pending/исходы и
отсутствие преждевременного broker ack. Restart при новой подходящей цене
завершает кандидатов: уникальные trigger_id/outbox совпадают с каноническим API,
SQL/broker Prefix сходятся. SIGINT после recovery также даёт exit 0. Реальная
SQL row lock при отзыве authority проверяет exit 1 вместо скрытого успеха.

Без opt-in тест явно SKIP; при opt-in без конфигурации — FAIL. Это минимальный
local runtime; полный E2E четырёх сервисов, production PKI и нагрузка остаются отдельно.

## Источник и миграция — OPS-062

Остановить прежние writers и применить миграции, включая
`007_source_binding.sql`, прежде чем запускать runtime:

```sh
python3 scripts/migrate.py --owner matcher
# Задать KAFKA_CLUSTER_ID, PRICES_TOPIC_ID и SUBSCRIPTION_CHANGES_TOPIC_ID
# из доверенного provisioning/control plane, затем остальные настройки выше.
go run ./cmd/matcher
```

ClusterID/TopicID не являются секретами. TopicID из Kafka CLI в base64 UUID
нужно преобразовать в hex 16 bytes. Новый runtime сверяет config с metadata
без auto-create, затем проверяет TopicID каждого fetch до dispatch/ack.
Изменения и цены должны принадлежать одному настроенному cluster.
Metadata discovery для нового стенда предоставляет `kafkasource.Metadata`;
его результат нельзя назначать историческим offsets/Pending.

PrepareSource после Assign проверяет ReadScope всего prices scope до bootstrap.
При Checkpoint использует durable Source и next; пустой scope либо bound history
без cursor допускают только явно заданный PRICES_BOOTSTRAP. Latest не создаёт
transport offset. Projection Checkpoint/Recover проверяют SQL provenance до
выбора snapshot; NewResume передаёт SQL Source в RegisterAt. Replay C и offset
остаются независимыми. Scope/проекция не меняют имя при замене источника.

Legacy work любого состояния, включая completed Latest без cursor, либо legacy
transport cursor дают явный отказ. Другой ClusterID/TopicID даёт source mismatch,
даже при одинаковых имени/offsets. Ни новая конфигурация, ни новый snapshot
не связывают старые строки. Перенос/очистка legacy не реализованы; прежние данные
сохраняются. Ошибка SQL/metadata/source останавливает процесс с exit 1, отменой
RPC/readers и отзывом grants. Readiness до успешной проверки не появляется.

Process runner дополнительно проверяет legacy Latest без cursor, доказанный
cursor, Pending при delete/recreate своего topic, перекрывающиеся offsets,
отказ старой metadata и нового config против прежнего SQL binding, сохранность
Pending и отзыв grants. Доверие к broker/соединению и неразличимость клона storage
с теми же IDs описаны в [kafkasource](../../internal/platform/kafkasource/README.md).

## Durable progress RPC — OPS-056

Дополнительно обязательны `MATCHER_RPC_ADDR` (host:port, отдельный listener),
`MATCHER_RPC_CERT_FILE`, `MATCHER_RPC_KEY_FILE` (ServerAuth, DNS SAN matcher),
`CLEANER_SERVICE_URI` (точный URI SAN клиента cleaner). `RPC_CA_FILE` также
используется как client CA экспортера. Outbound `RPC_CERT_FILE`/`RPC_KEY_FILE`
остаются ClientAuth credentials. Пустая либо некорректная конфигурация запрещает
запуск; ошибки listener/Serve являются terminal. На shutdown gRPC Stop отменяет
активные RPC, ждёт handlers/SQL, Serve присоединяется до pool.Close.

[Wire и ошибки exporter](../../internal/matcher/progressrpc/README.md).
Пример дополнения к существующему окружению запуска:

```sh
export MATCHER_RPC_ADDR=127.0.0.1:8087
export MATCHER_RPC_CERT_FILE=/run/secrets/matcher-server.pem
export MATCHER_RPC_KEY_FILE=/run/secrets/matcher-server.key
export CLEANER_SERVICE_URI=spiffe://local/cleaner
```

Cleaner использует `progressrpc.NewClient(conn)` с mTLS и проверкой DNS SAN;
ему не передаётся matcher DSN. Вызывать ReadProgress с kind, exact consumer_id,
ClusterID/TopicID/topic/partition из доверенного provisioning. Для changes
consumer_id — зарегистрированный durable projection_id, для prices — стабильный
scope, а не текущий member/token. Перед DeleteRecords необходимо проверить все
обязательные consumers по протоколу OPS-014; успешного ответа одной projection
недостаточно. Отсутствие cursor, legacy или иной source запрещают очистку.
