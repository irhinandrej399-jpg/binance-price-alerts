# Durable notifications progress — OPS-057

`notifications.v1.NotificationsProgress/ReadProgress` — read-only unary gRPC
над собственной SQL БД notifications. `NewPostgres(pool)` возвращает готовый
`intake.Store`; используется только `ReadProgress`, один SELECT/MVCC snapshot
persisted binding + `app.delivery_checkpoints.next_offset`. `Checkpoint`,
Bootstrap и Kafka metadata не вызываются. Job и next приняты одной транзакцией
`AcceptBound`; pending job уже durable, независимо от Telegram и broker commit.
Экспорт не создаёт partition и не меняет jobs/attempts/outcomes/reports.

Запрос содержит точные ожидаемые `consumer_id`, `generation`, `cluster_id`,
`topic_id` (16 raw bytes Kafka UUID), `topic`, `partition`. Consumer — стабильный
logical intake, generation — уникальное поколение явного SQL bootstrap;
replica/member/restart их не меняют. Identity из запроса не приписывается SQL:
reader сверяет её с persisted provenance, сервер повторно проверяет весь результат.
Ответ повторяет persisted identity и содержит optional `next_offset` только при
успехе. Наличие поля обязательно; явный bootstrap zero допустим, synthetic
0/latest при отсутствующей строке запрещён. Offset — next, включая Kafka gaps.
Это тот же принцип exact SQL proof, что в [OPS-056](../../matcher/progressrpc/README.md),
с дополнительной ожидаемой SQL generation для notifications.

| gRPC code | Значение |
| --- | --- |
| InvalidArgument | Нет exact identity/topic, строки пусты/больше 256 bytes, неверный/нулевой TopicID, отрицательная partition |
| FailedPrecondition | Неизвестный consumer/generation/source/topic/partition, отсутствующий checkpoint/bootstrap, legacy/unbound или неверный SQL result |
| Unavailable | SQL fault; ответ не содержит DSN/внутренних ошибок |
| DeadlineExceeded / Canceled | Caller/server deadline или отмена SQL |
| PermissionDenied / Unauthenticated | mTLS caller без права / непригодная identity |
| ResourceExhausted | Transport message превышает 4096 bytes |

На любой ошибке нет progress response/offset и очистка запрещена. Budget сервера
и `NewClient(conn)` — 1 секунда, более короткий caller deadline наследуется;
автоматических повторов нет. Строки ограничены 256 bytes, сообщения — 4096.
`ServerOptions(cert,key,clientCA,cleanerURI)` включает TLS 1.3 и deny-by-default
serviceauth: только явно настроенный URI SAN cleaner, только этот unary method.
Server certificate требует ServerAuth и проверяемый DNS SAN; клиент — ClientAuth
и проверку доверенного имени сервера. Stop отменяет RPC, `WaitForHandlers` ждёт
reader до закрытия pool. Startup/terminal RPC error входит в runtime supervisor.

Cleaner получает endpoint и mTLS credentials, без notifications DSN. Один ответ
подтверждает только этот consumer/generation/source/partition, полноту inventory
не доказывает. OPS-014 должен проверить все обязательные consumers и минимум;
unknown/offline consumer не исключается автоматически. Этот RPC сам не разрешает
DeleteRecords. После сброса SQL нужна новая Generation в bootstrap и внешних
ожиданиях cleaner; topic recreate требует новой source identity и отдельного
плана переноса. Автоматической миграции legacy/source нет. Копии SQL с теми же
IDs неразличимы; inventory/bootstrap остаются ответственностью provisioning.

Runtime настройки и provisioning — [cmd/notifications](../../../cmd/notifications/README.md).
Проверки:

```sh
GOCACHE=/tmp/binance-go-build go test -race ./internal/notifications/progressrpc
GOCACHE=/tmp/binance-go-build python3 scripts/check_notifications_process.py --package ./cmd/notifications
GOCACHE=/tmp/binance-go-build python3 scripts/generate_proto.py --check
```

Первый набор использует loopback mTLS и проверяет rights/limits/deadlines/join.
Process runner использует disposable PostgreSQL/Kafka, независимый read-only SQL
pool и mTLS cleaner: commit/rollback, неизвестные identities без INSERT,
SQL lock/deadline/Stop/fault, lost broker commit, pending job и executable restart.
Обычный Go test явно пропускает opt-in process проверки; нагрузка не измерялась.
