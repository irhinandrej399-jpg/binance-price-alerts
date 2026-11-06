# Минимальный локальный API (OPS-046, OPS-004)

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
/tmp/binance-api
```

Все семь переменных обязательны. HTTP_ADDR и каждый адрес в KAFKA_BROKERS
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

Доступны только:

| Метод и путь | Поведение |
| --- | --- |
| GET /healthz | Liveness, без запроса к БД |
| GET /readyz | После startup — API pool.Ping, бюджет 1 с |
| DELETE /v1/subscriptions/{id} | Bearer session/auth, каноническая отмена с journal |

Весь HTTP mux обёрнут в [bodylimit](../../internal/api/bodylimit/README.md):
максимум 64 КиБ тела, включая chunked; превышение даёт 413
`request_body_too_large` до auth/SQL. Health поддерживает HEAD. Подробности HTTP-отмены —
[контракт компонента](../../internal/api/subscription/README.md#http-отмена-sub-005).
Регистрация, вход, создание подписки, Telegram webhook, внутренние RPC и метрики
не смонтированы. Аккаунт/сессия/подписка для проверки создаются fixture harness.
Это минимальный процесс для OPS-004; полный API для E2E ещё предстоит собрать.

До HTTP Serve проверяются bind, подключение/схема собственной БД и по одному
Step обеих очередей. Общий startup budget — 15 с, каждый Step — до 5 с.
Любая ошибка startup (включая busy lease) завершает процесс с кодом 1;
ресурсы освобождаются, durable работа остаётся. При pending outbox startup
также проверяет реальную публикацию. Пустая очередь не требует Kafka-запроса.
После startup оба существующих Run работают в этом же процессе: transient
SQL/Kafka ошибки повторяются по их политике; readiness проверяет только API-БД.
Fencing, потеря lease, повреждённый journal/outbox и другие terminal результаты
Run снимают готовность и завершают весь процесс; HTTP server закрывается,
соседний relay отменяется и дожидается завершения перед закрытием pool.
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
От первого сигнала действует окончательный предел 10 с на весь выход, включая
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
