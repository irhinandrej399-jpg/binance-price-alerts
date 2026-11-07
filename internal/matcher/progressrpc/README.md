# Durable matcher progress — OPS-056

`matcher.v1.MatcherProgress/ReadProgress` — read-only unary gRPC над собственной
БД matcher. `NewPostgres(pool)` вызывает `projection.Recover` либо
`pricework.ReadCheckpoint`; один SELECT даёт одну MVCC view cursor + source.
Не пишет binding/checkpoint, не снимает replay-pin, не выдаёт writer grant.
Исторические/revoked проекции доступны без активного replica/member/token.

Запрос: `kind=PROJECTION` и точный SQL `projection_id` либо `kind=PRICES` и
стабильный SQL `scope`; `consumer_id` не является ID текущего члена Kafka group.
Источник задаётся `cluster_id`, `topic_id` (16 raw bytes Kafka UUID), `topic`,
`partition`. Source сверяется с provenance из SQL. SnapshotID/C, текущая
конфигурация, broker commit, health и локальный Confirm не заменяют provenance.
Ответ повторяет exact consumer/source/topic/partition и содержит optional
`next_offset` только после подтверждённого SQL-чтения. Offset — next, Kafka gaps
допустимы; его нельзя вычислять из C или числа обработанных событий.

Один ответ подтверждает только этот consumer. Cleaner OPS-014 должен запросить
каждую зарегистрированную обязательную projection и каждый prices scope и
выбрать минимум для данного source/topic/partition. Нельзя выбрать более быструю
проекцию, забыть offline-потребителя или подменить scope текущим назначением.
Полнота реестра, retirement, bootstrap и сериализация реестра с DeleteRecords
остаются OPS-014. Этот RPC сам по себе не разрешает очистку Kafka.

Ошибки не содержат offset:

| gRPC code | Значение |
| --- | --- |
| InvalidArgument | Нет exact consumer/source, неверный kind, отрицательная partition, строки >256 bytes, TopicID не 16 bytes/нулевой |
| FailedPrecondition | Нет cursor/snapshot, только snapshot/replay C, legacy/unbound или source/topic/partition mismatch |
| Unavailable | Ошибка SQL, без внутренних ошибок/DSN |
| DeadlineExceeded / Canceled | Отмена SQL по deadline или завершению вызова |
| PermissionDenied / Unauthenticated | mTLS identity без права либо непригодная identity |
| ResourceExhausted | Превышен transport message limit |

Никаких synthetic 0/latest. Binding/terminal work без cursor не считается
подтверждением; Pending не продвигает Prefix. Existing cursor при новом Pending
остаётся подтверждённым только до прежнего next. Rollback не виден;
неизвестный writer commit проверяется повторным чтением SQL. После takeover или
restart тот же consumer читает прежнее durable состояние. Смена поколения
Kafka не переименовывает историческую строку: ожидаемый source даёт отказ.

`ServerOptions(cert,key,clientCA,cleanerURI)` использует serviceauth deny-by-default:
TLS 1.3, один явно настроенный URI SAN cleaner и единственное право на ReadProgress.
Streaming и неизвестные методы запрещены. Сертификат сервера имеет ServerAuth и
DNS SAN; ключ и CA загружаются из внешних файлов. Клиент использует собственные
ClientAuth credentials и проверяет доверенное имя сервера. Request/response
ограничены 4096 bytes; строки — 256 bytes, TopicID — 16. Budget сервера и
`NewClient(conn)` — 1 секунда с наследованием более короткого caller deadline.
Client не повторяет RPC автоматически. gRPC Stop отменяет RPC и с
WaitForHandlers ждёт окончания SQL перед закрытием pool; нет graceful ожидания
новых запросов после остановки runtime.

Подключение и переменные — [cmd/matcher](../../../cmd/matcher/README.md).
Воспроизведение:

```sh
GOCACHE=/tmp/binance-go-build go test -race ./internal/matcher/progressrpc
GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py --package ./cmd/matcher
GOCACHE=/tmp/binance-go-build python3 scripts/generate_proto.py --check
```

SQL tests требуют opt-in runner; обычный go test явно пропускает их.
Используются готовые disposable PostgreSQL/Kafka runners и loopback mTLS fixture,
рабочие topics/БД не затрагиваются. Производительность/нагрузка не измерялись.
