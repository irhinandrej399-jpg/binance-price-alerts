# Локальный API (OPS-046, OPS-004, OPS-013, OPS-070, OPS-071)

Сборка и запуск из корня репозитория:

```sh
GOTOOLCHAIN=local go build -o /tmp/binance-api ./cmd/api
# DATABASE_URL загрузить в окружение из локального хранилища секретов.
# Пример формата: postgres://api_runtime:<password>@127.0.0.1:5432/api
HTTP_ADDR=127.0.0.1:8080 \
KAFKA_BROKERS=127.0.0.1:9092 \
SUBSCRIPTION_CHANGES_TOPIC=local-subscription-changes \
DELIVERY_REQUESTS_TOPIC=local-delivery-requests \
SUBSCRIPTION_RELAY_TRANSACTIONAL_ID=local-api-subscription-relay-0 \
DELIVERY_RELAY_TRANSACTIONAL_ID=local-api-delivery-relay-0 \
RPC_ADDR=127.0.0.1:9080 \
RPC_CERT_FILE=/path/to/api-server.pem \
RPC_KEY_FILE=/path/to/api-server-key.pem \
RPC_CA_FILE=/path/to/service-ca.pem \
MATCHER_SERVICE_URI=spiffe://local/matcher \
NOTIFICATIONS_SERVICE_URI=spiffe://local/notifications \
ADMISSION_PERMIT_TTL=10s \
MARKET_RPC_ADDR=127.0.0.1:9081 \
MARKET_RPC_SERVER_NAME=market.internal \
MARKET_RPC_CERT_FILE=/path/to/api-client.pem \
MARKET_RPC_KEY_FILE=/path/to/api-client-key.pem \
MARKET_RPC_CA_FILE=/path/to/market-ca.pem \
/tmp/binance-api
```

Все показанные переменные и DATABASE_URL обязательны. HTTP_ADDR и каждый адрес в KAFKA_BROKERS
(через запятую без пробелов) должны иметь host:port. DATABASE_URL — только
собственная API-БД под `api_runtime`; pool открывается один раз. Ошибки процесса
содержат только фиксированные сообщения, без DSN, токенов и broker responses.
Не передавайте секреты в аргументах запуска и не коммитьте локальный env.

Оба topic и transactional.id должны быть непустыми Kafka names; topics
различаются, ID различаются. ID задаются оператором, сохраняются между
перезапусками и одинаковы у всех реплик одной очереди в одной среде; PID и
случайные суффиксы runtime не добавляет. Subscription topic имеет одну партицию,
delivery topic — 16, как в [init Kafka](../../infra/kafka/README.md).
Запустить PostgreSQL/Kafka и создать topics нужно отдельно; миграции API
применяются существующим `python3 scripts/migrate.py --owner api` по
[инструкции](../../migrations/README.md). Runtime миграций не выполняет,
DSN других сервисов не читает.

HTTP маршруты:

| Метод и путь | Поведение |
| --- | --- |
| GET /healthz | Liveness, без запроса к БД |
| GET /readyz | После startup — API pool.Ping, бюджет 1 с |
| POST /v1/accounts | Регистрация, общий auth limit |
| POST /v1/sessions | Вход, общий auth limit |
| DELETE /v1/subscriptions/{id} | Bearer session/auth, каноническая отмена с journal |

Вход и регистрация используют [общую квоту PostgreSQL](../../internal/api/authlimit/README.md):
20 попыток на IP за фиксированную минуту совместно для всех реплик и обоих
маршрутов. IP — TCP peer, proxy headers игнорируются; за proxy/NAT квота общая.
429 содержит Retry-After, отказ хранилища даёт 503 до credentials. Нужна
миграция API 015; production-время берётся из БД. Очистка строк IP пока отсутствует.

Весь HTTP mux обёрнут в [bodylimit](../../internal/api/bodylimit/README.md):
максимум 64 КиБ тела, включая chunked; превышение даёт 413
`request_body_too_large` до auth/SQL. Health поддерживает HEAD. Подробности HTTP-отмены —
[контракт компонента](../../internal/api/subscription/README.md#http-отмена-sub-005).
Создание подписки, Telegram webhook, RPC notifications и метрики
не смонтированы. Аккаунт/сессия/подписка для проверки создаются fixture harness.
Это минимальный процесс для OPS-004; полный API для E2E ещё предстоит собрать.

До HTTP/RPC Serve проверяются оба bind, TLS, подключение/схема собственной БД и по одному
Step обеих очередей. Общий startup budget — 15 с, каждый Step — до 5 с.
Любая ошибка startup (включая busy lease) завершает процесс с кодом 1;
ресурсы освобождаются, durable работа остаётся. При pending outbox startup
также проверяет реальную публикацию. Пустая очередь не требует Kafka-запроса.
После startup оба существующих Run работают в этом же процессе: transient
SQL/Kafka ошибки повторяются по их политике; readiness проверяет только API-БД.
Fencing, потеря lease, повреждённый journal/outbox и другие terminal результаты
Run снимают готовность и завершают весь процесс; HTTP server закрывается,
соседний relay и RPC отменяются и дожидаются завершения перед закрытием pool.
Автоматического захвата producer после fencing в runtime нет.

SIGTERM и SIGINT снимают readiness и закрывают HTTP listener/idle connections.
Начатым HTTP handlers даётся 5 с на завершение с исходным context: транзакция
может commit и вернуть ответ. По deadline runtime отменяет request contexts,
закрывает соединения и ждёт освобождения handlers перед закрытием общего pool.
Незавершённая SQL-транзакция откатывается; потерянный ответ commit разрешается
повторным чтением канонического состояния/идемпотентной отменой.
Оба relay отменяются сразу и присоединяются перед закрытием pool. Допубликовывать
весь backlog при остановке не требуется: новые outbox/journal записи, включая
созданные HTTP во время drain, восстанавливаются при следующем запуске.
Отмена между Kafka commit и SQL ack может дать дубль с исходными ID/key/value.
Если отмена попала в неоднозначный SQL claim, lease может оставаться живой до
30 с. Startup с busy lease завершится ошибкой; повторять запуск после её expiry,
не сбрасывая lease вручную.
Успешная остановка возвращает код 0; истечение HTTP drain/ошибка компонента — 1.
От первого сигнала или terminal failure действует окончательный предел 10 с на весь выход, включая
cleanup и pool.Close: при его превышении процесс выходит с кодом 1 без ожидания
defers. Durable восстановление и lease expiry остаются обязательными.
Эти фиксированные бюджеты относятся только к минимальному API.

Проверка настоящего отдельного executable на disposable PostgreSQL/Kafka:

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_api_process.py --package ./cmd/api
```

Runner применяет только API migrations, передаёт только API DSN, удаляет
уникальные контейнеры/volumes/networks и зарегистрированные children.
Процессный тест проверяет authenticated HTTP-отмену, durable cancelled,
исходные key/value в обоих topics у read_committed consumer, terminal отказ
после startup, повторный startup с повреждённым journal и отказ подключения
к БД. Он намеренно добавляет invalid journal только в disposable dataset.
SIGTERM дополнительно проверяется с реальным PostgreSQL lock внутри начатой
отмены: commit после освобождения lock и rollback state/quota/journal по deadline,
отказ новых соединений и восстановление subscription relay. Lock SQL ack после
Kafka commit проверяет прерывание delivery relay и повтор исходных key/value
в read_committed после рестарта. Unit-тесты проверяют HTTP drain/deadline,
конфигурацию, bind и отмену/ожидание соседних компонентов. Без opt-in процессная интеграция явно SKIP.

## Matcher RPC (OPS-070)

Один listener RPC_ADDR регистрирует готовые SubscriptionSnapshots
(BuildSnapshot, ReadSnapshotPage), SubscriptionReplay (BeginReplay,
ReadReplayPage, AcknowledgeReplay, AbandonReplay) и Delivery/RequestDelivery.
Общая policy serviceauth разрешает эти полные имена методов
точному MATCHER_SERVICE_URI из проверенного клиентского сертификата.
Чужие сервисы, неизвестные/streaming методы и публичный Bearer прав не дают.
Owner snapshot/pin берётся адаптерами из сертификата, metadata его не меняет.

RPC_CERT_FILE/RPC_KEY_FILE — server certificate/key; RPC_CA_FILE — доверенный
CA клиентов. MARKET_RPC_* задают отдельную клиентскую mTLS связь API → Latest;
SERVER_NAME проверяется по DNS SAN сервера даже при подключении к IP.
Адреса — явные host:port без resolver URL. Ключи хранятся во внешних файлах с
доступом владельцу; PEM, credentials и DSN не логируются. Ротация требует
перезапуска. Runtime не выпускает сертификаты и не читает market-data SQL.

Все adapters используют один собственный API pool, исходные SQL/outbox
протоколы и бюджеты: до 3 с на matcher RPC, внутри RequestDelivery Latest
ограничен 1 с. Автоматических прикладных retries нет. Соединение Latest
ленивое: его недоступность возвращает ошибку RequestDelivery, readiness
по-прежнему проверяет API-БД. Восстановление workflow не зависит от Latest.

До relay Step/Serve проверяется наличие таблиц snapshot/request/pin/replay/outbox.
Startup bind/TLS/schema failure закрывает listeners/connection/pool без запуска
HTTP/RPC handlers. При сигнале или terminal failure RPC Stop немедленно отменяет
активные calls; WaitForHandlers присоединяет SQL handlers до pool.Close.
HTTP сохраняет прежний drain 5 с. Соединение Latest закрывается после handlers.
Общий watchdog охватывает также terminal failure и cleanup.

Тот же process runner проверяет все семь методов на настоящем race executable,
API PostgreSQL/Kafka и ephemeral mTLS: certificate pin owner, deny/unknown/Bearer,
точный Latest → canonical workflow/outbox → read_committed bytes, replay/ack,
устойчивые snapshot/workflow/bytes после restart, SIGTERM с удерживаемым SQL RPC,
отсутствие SQL waiters после exit, bind/TLS/schema failure и terminal relay exit.
Latest endpoint в этом тесте — локальная управляемая fixture; настоящий
market-data executable проверен отдельно в OPS-069. Полный E2E ещё впереди.

## Notifications RPC (OPS-071)

Тот же RPC_ADDR регистрирует готовые Admission/PrepareRecipient,
Admission/AdmitDelivery и DeliveryResults/SaveDeliveryResult. Общая policy
даёт только эти три метода точному NOTIFICATIONS_SERVICE_URI. URI matcher и
notifications обязательны и должны различаться; matcher сохраняет только свои
семь методов. Public Bearer и metadata не заменяют сертификат.

ADMISSION_PERMIT_TTL — обязательная Go duration от 1us до 1m с точностью 1us;
пример 10s — локальная настройка, не измеренный SLA. Слишком короткий permit
может истечь до внешнего запроса. TTL задаёт API, запрос его не меняет.
Admission использует тот же управляемый Latest mTLS client с явными
MARKET_RPC_ADDR/SERVER_NAME/credentials. API подключается только к своей БД.
Недоступность Latest проявляется в операции; readiness проверяет API-БД.

PrepareRecipient только читает адресат для лимитеров. AdmitDelivery проверяет
expected_recipient CAS, актуальную цену и атомарно сохраняет permit до ответа.
При Aborted нужен новый цикл подготовки/лимитеров и новый request ID.
При неизвестном commit повторяется исходный request ID/CAS; исторический
permit сохраняет attempt/fence/адрес/цену/срок и не разрешает повтор Telegram.
SaveDeliveryResult сохраняет исход по известному attempt до подтверждения;
при ошибке notifications удерживает тот же pending report и повторяет его,
включая после API restart. Подтверждённый успех не требует новой отправки.
Готовые adapters ограничивают каждый RPC 3 с, вложенный Latest — 1 с.

Startup проверяет также delivery_admissions/delivery_results; ошибки
конфигурации/TLS/SQL проходят через общий lifecycle. Stop отменяет SQL и
вложенный Latest, joins handlers до pool.Close и закрывает outbound connection.
HTTP drain и общий предел выхода остаются прежними.

Process runner выше проверяет PrepareRecipient → AdmitDelivery → результат
на настоящем race executable и disposable API PostgreSQL/Kafka: изоляцию прав,
owner/workflow, CAS, точную цену, отсутствие попытки при Latest failure/cancel,
SQL commit до ответа, canonical permit после restart, отмену сохранения под
SQL lock и повтор удержанного report, idempotent success/conflict и SIGTERM
с активным Latest. Telegram не вызывается; полный E2E остаётся отдельной задачей.
