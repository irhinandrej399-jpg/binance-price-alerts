# Минимальный notifications runtime (OPS-055)

`go run ./cmd/notifications` собирает intake → durable jobs → worker/handler →
Telegram → durable reporter/API. Миграции `migrations/notifications` применяются
отдельно; процесс использует только `notifications_runtime` / `notifications`.
SQL checkpoint определяет resume, а ack Kafka выполняется после сохранения job.
Повторный запуск сохраняет group, logical consumer, generation, source, bot ID и ту же БД.

Все настройки обязательны, без неявных бюджетов:

| Переменная | Значение / пример |
| --- | --- |
| `NOTIFICATIONS_DATABASE_URL` | DSN своей БД, роль `notifications_runtime` |
| `HTTP_ADDR` | `127.0.0.1:8083` |
| `RPC_ADDR` | отдельный exporter listener, например `127.0.0.1:8086` |
| `RPC_SERVER_CERT_FILE`, `RPC_SERVER_KEY_FILE` | PEM ServerAuth certificate/key с DNS SAN notifications |
| `CLEANER_SERVICE_URI` | точный URI SAN cleaner, например `spiffe://local/cleaner` |
| `KAFKA_BROKERS` | `localhost:9092`, несколько через запятую |
| `DELIVERY_REQUESTS_TOPIC` | canonical delivery topic с 16 partitions |
| `NOTIFICATIONS_GROUP` | стабильная consumer group окружения |
| `NOTIFICATIONS_CONSUMER_ID` | постоянный logical intake ID из operator inventory |
| `NOTIFICATIONS_GENERATION` | уникальное поколение явного SQL bootstrap из inventory |
| `KAFKA_CLUSTER_ID` | ожидаемый Kafka ClusterID |
| `DELIVERY_REQUESTS_TOPIC_ID` | ожидаемый ненулевой TopicID, 32 hex символа |
| `API_RPC_ADDR` | `localhost:8085`, общий Admission/Result endpoint |
| `RPC_SERVER_NAME` | DNS SAN сервера, например `api.local` |
| `RPC_IDENTITY` | точный SPIFFE URI единственного URI SAN client certificate |
| `RPC_CA_FILE`, `RPC_CERT_FILE`, `RPC_KEY_FILE` | PEM CA/client certificate/key |
| `TELEGRAM_BOT_TOKEN` | token вида `<числовой bot ID>:<секрет>` |
| `TELEGRAM_BASE_URL` | `https://api.telegram.org`, origin без пути/token/query |
| `TELEGRAM_TIMEOUT` | `2s`, полный timeout одной HTTP-попытки |
| `BOT_RATE` | `25`, общий SQL limiter допусков/с |
| `CHAT_INTERVAL` | `1s`, SQL limiter на chat |
| `RETRY_BASE_DELAY`, `RETRY_MAX_DELAY` | `1s`, `30s`, equal jitter готовой policy |
| `CONDITION_INTERVAL` | `1s`, повторная проверка условия |
| `MAX_FAILURES` | `5`, durable бюджет доказанных Telegram ошибок |
| `LEASE_TTL` | `15s`, максимум `1m`, без renewal |
| `RECOVERY_INTERVAL` | `5s`, общий для handler и worker, `1s`..`1h` |
| `REPORT_LIMIT` | `100`, 1..1000 |
| `WORKER_CONCURRENCY` | `8`, 1..1000 |
| `WORKER_INTERVAL` | `100ms`, пауза между ограниченными волнами |
| `WORKER_TIMEOUT` | `10s`, discovery и один вызов handler/recovery |
| `WRITE_TIMEOUT` | `2s`, одна попытка сохранения известного исхода |

Duration — положительные целые микросекунды, до `1h`; дополнительно
`TELEGRAM_TIMEOUT <= WORKER_TIMEOUT <= LEASE_TTL`. Bot ID извлекается из token,
сохраняется при ротации секрета. Rate 1..1 000 000 — технический диапазон,
он не отменяет ограничения Telegram. Изменение limiter budgets требует
согласованного обновления сохранённых интервалов при остановленных репликах,
как описано в botlimit/chatlimit README; удаление строк сбрасывает квоты.

TLS 1.3, явные CA/server authority, certificate/key и точный URI SAN.
Сервер API должен разрешать этой identity ровно Admission/PrepareRecipient/Result.
Ошибки процесса содержат только фиксированную категорию: DSN, URL/token, PEM,
тексты заданий и сырые ошибки SQL/RPC не выводятся. Локальный HTTP Telegram
разрешён только на IP loopback для fixture. Health не вызывает Telegram/API.

`/healthz` отвечает 200; `/readyz` — 503 до инициализации/старта worker, после
выхода любого компонента и при SQL Ping failure; после старта и успешного Ping
— 200. Readiness не доказывает доступность Telegram/API, синхронизацию intake
или производительность. Telegram 429 не снимает liveness. Intake/worker/HTTP/progress RPC
обязательны: их выход снимает readiness, отменяет siblings и присоединяет их
перед Close Kafka/SQL/RPC. Terminal failure завершает executable с кодом 1.

Claim/CAS, оба лимитера, свежий admission, одна HTTP-попытка и PendingWrite
исполняются готовыми пакетами. После потери ответа Result reporter повторяет
сохранённый исход без нового HTTP. Потеря процесса до durable записи внешнего
результата сохраняет риск внешнего дубля: неизвестную отправку нельзя считать
неотправленной.

SIGTERM/SIGINT снимают readiness, отменяют intake, worker и активные HTTP/RPC.
Новая работа не начинается; callbacks присоединяются до Close Kafka/SQL/RPC.
Kafka leave ограничен 2 с, затем отменяется client context и присоединяются
локальные goroutines; при неподтверждённом leave membership может жить
до session expiry. Resume всё равно определяется SQL checkpoint.
Worker даёт PendingWrite отдельный context с WRITE_TIMEOUT для последней
записи известного исхода, даже после отмены рабочего context. Неподтверждённый
HTTP сохраняется как uncertain; подтверждённый успех и его report не теряются
при штатном drain. Не зарегистрированная отправка не выдумывается: lease без
исхода остаётся до expiry для нового worker. Ошибка final write даёт exit 1;
чистая остановка — exit 0. Общий предел от сигнала, включая deferred Close, —
20 секунд; превышение принудительно завершает процесс с exit 1. Незавершённая
работа восстанавливается из SQL при restart. WRITE_TIMEOUT может превышать
этот предел: тогда его ограничит общий deadline, без обещания записи результата.

Проверка настоящего executable с race на disposable PostgreSQL/Kafka/mTLS/HTTP:

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_notifications_process.py --package ./cmd/notifications
```

Без opt-in process test явно SKIP; `NOTIFICATIONS_PROCESS_INTEGRATION=1` без
DB/Kafka — FAIL. Fixture и общая очистка описаны в
[notificationsprocess](../../internal/testutil/notificationsprocess/README.md).
Проверяются canonical bytes и SQL/broker Prefix, false Latest без HTTP,
оба limiter state, текущая quote в сообщении, 429/retry_after, unknown,
success/report после API commit, SIGKILL/restart с прежней identity, отсутствие
нового job/attempt/HTTP для подтверждённого успеха, startup и terminal intake fault.
SIGTERM проверяется до HTTP, во время запроса, после подтверждённого ответа
и при SQL lock на final write; SIGINT — после recovery. Проверяются exit code,
uncertain/success, lease, bytes и canonical API outcome.
Четыре процесса, общий E2E и нагрузка остаются отдельными задачами.

## Source-bound runtime — OPS-066

До запуска worker и HTTP процесс с пределом 15 с сверяет ожидаемый source
через metadata без auto-create, требует ровно partitions 0..15 и читает
`intake.Store.ReadProgress` каждой партиции. Ошибка, missing bootstrap, legacy
или несовпадение consumer/generation/source дают exit 1 до readiness и отправок.
Транспорт OPS-065 затем сверяет metadata при assignment/позициях/idle и TopicID
исходных fetch responses до decode/SQL/ack. Terminal ошибка отменяет siblings;
прежние bounded shutdown и join до pool.Close сохраняются. Readiness не является
доказательством назначения всех partitions или полноты обработки входа.

Provisioning выполняется **отдельно**, при остановленных процессах:

1. Применить миграции, включая `011_intake_source.sql`. Для нового пустого intake
   создать уникальную Generation, сохранить её вне этой БД вместе с ConsumerID
   и ожиданиями будущего cleaner. Group member/replica ID для этого не подходит.
2. Сверить Kafka ClusterID, ненулевой TopicID, имя topic и partitions 0..15
   с operator inventory. Metadata discovery допустима для нового источника;
   она не доказывает происхождение старых jobs/checkpoints.
3. В явном provisioning вызвать `intake.NewStore(pool).Bootstrap(ctx, binding,
   []int32{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15})` из доверенного Go-инструмента
   внутри модуля. Все позиции начнутся с 0; история должна оставаться доступной.
   Runtime Bootstrap не вызывает. Задать четыре обязательные переменные выше.
4. При restart/новом member сохранить те же IDs и БД. При уничтожении SQL создать
   новую Generation и отдельно обновить внешние ожидания. Новая БД не может
   проверить, что оператор повторно использовал старую Generation.

Legacy jobs/checkpoints, даже нулевые, не получают provenance автоматически.
Изменение source/Generation в env не меняет SQL registry. После delete/recreate
с тем же именем/offsets старый runtime отказывает; подстановка нового TopicID
тоже отказывает по SQL. Перенос legacy или смена source требуют отдельного
операторского плана, которого этот runtime не реализует. Клоны с теми же IDs
неразличимы. Неизвестный внешний результат по-прежнему допускает дубль Telegram.

Для OPS-057: runtime хранит проверяемый `config.binding`, а persisted
`intake.Store.ReadProgress(ctx, binding, partition)` возвращает Binding/Partition/
Next одним SELECT. Ошибка не является Next=0 и запрещает очистку. Будущий exporter
должен ограничить SQL/RPC deadline, проверить ожидания caller и присоединиться
при shutdown; RPC/exporter и реестр обязательных consumers здесь не добавлены.

Process/race проверка также покрывает SIGKILL pending job/report с сохранением
attempt IDs/bytes, binding всех partitions и двух одновременных members, чужие IDs до readiness, живой
процесс при recreate с перекрывающимся offset=0, сохранность SQL evidence и
отсутствие ack/HTTP, legacy отказ и новый пустой SQL с новой Generation.

## Durable progress RPC — OPS-057

Отдельный `RPC_ADDR` обслуживает только
`notifications.v1.NotificationsProgress/ReadProgress`. Сервер использует
`RPC_SERVER_CERT_FILE`/`RPC_SERVER_KEY_FILE`, доверяет client CA из `RPC_CA_FILE`
и разрешает метод только `CLEANER_SERVICE_URI`. Исходящие API credentials
`RPC_CERT_FILE`/`RPC_KEY_FILE` остаются отдельными ClientAuth credentials.
Cleaner проверяет DNS SAN notifications через свою authority и использует свой
ClientAuth certificate, без SQL-доступа. Все четыре новые настройки обязательны.

Запрос связывает consumer + SQL generation + Kafka ClusterID/TopicID/topic/partition
с persisted SQL next; чтение не вызывает Bootstrap/Checkpoint и не меняет pending
job/attempt/report. Бюджет SQL/RPC — 1 с, request/response — 4096 bytes, строки —
256 bytes. Неподтверждённый прогресс возвращает ошибку без offset.
Startup bind/PEM error не даёт readiness; выход сервера останавливает siblings.
При signal/terminal cancellation gRPC Stop отменяет активные запросы и ждёт
SQL handlers до pool.Close в существующем общем drain budget 20 с.
Wire/errors/cleaner obligations — [progressrpc](../../internal/notifications/progressrpc/README.md).

Операторская команда первоначального provisioning и проверка сохранённого inventory:
[source-provision (OPS-078)](../source-provision/README.md).
