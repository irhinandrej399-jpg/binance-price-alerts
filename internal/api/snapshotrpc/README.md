# Страницы снимка подписок (SUB-016)

`proto/api/v1/snapshot.proto`: unary `SubscriptionSnapshots.BuildSnapshot` и
`ReadSnapshotPage`. API владеет данными; matcher получает страницы через gRPC,
без SQL-доступа. Регистрация:

```go
opts, err := snapshotrpc.ServerOptions(cert, key, clientCA, matcherURI)
// Проверить err; установить все opts вместе, без замены credentials/interceptors.
server := grpc.NewServer(opts...)
apiv1.RegisterSubscriptionSnapshotsServer(server,
    snapshotrpc.NewServer(subscription.NewSnapshots(pool)))
// Клиент использует caller-owned mTLS connection:
client := snapshotrpc.NewClient(conn)
```

`ServerOptions` разрешает оба метода только точной URI-идентичности matcher.
Владелец S/pin/request — проверенная URI из сертификата (до 200 байт), общая
для реплик одного consumer. Ввод owner отсутствует; metadata, Bearer-сессия и
публичный token не дают полномочий. Сервисный сертификат даёт доступ к полному
снимку всех владельцев подписок. TLS/CA и права должны устанавливаться совместно
с регистрацией, как требует `serviceauth`.

`BuildSnapshot` принимает устойчивый request ID (1..128 ASCII `[A-Za-z0-9_.-]`),
TTL в миллисекундах (1..86400000) и размер страницы (1..1000). Он вызывает
`SUB-023.BuildRequest`, затем читает первую страницу. После timeout/Unavailable
повторять прежние ID и TTL, включая новую связь/реплику API: commit уже мог
состояться. Автоматических повторов нет. Размер страницы не меняет ресурс и
может отличаться при повторе; дедупликация TTL не зависит от размера страницы.
Повтор не продлевает срок. Постоянные ключи и retention — контракт SUB-023.

Каждая страница возвращает `snapshot_id`, одну границу журнала `boundary_c`,
неизменный `expires_at` (UTC RFC3339Nano), точные decimal-строки и полные
замороженные after-images, включая все незавершённые состояния, workflow и
причину blocked. Цены создания — аудит, не разрешение отправки. Optional percent
и вложенные workflow/blocked сохраняют отсутствие. `C` передаётся как uint64,
не смешивается с ценовой ревизией. SQL-replay строго после C — отдельный SUB-022.

`ReadSnapshotPage` принимает snapshot ID в каноническом lowercase UUID-формате,
размер и cursor. Пустой cursor повторяет первую страницу. Непустой cursor —
версионированная позиция: префикс `subscription-snapshot-v1:` и base64url без
padding от `snapshot_id/last_subscription_id`. Клиент сохраняет его без разбора.
Cursor связан со снимком; storage проверяет наличие последней строки в этом
снимке. Это позиция, не bearer credential и не криптографическая подпись;
полномочия/owner/expiry проверяются независимо. Cursor другого снимка, цена или C
не принимаются. `next_cursor` пуст только в конце; повтор страницы стабилен.
Отсутствующий/чужой/истёкший/abandoned ресурс даёт явную ошибку, включая пустую
страницу. Адаптер проверяет срок также после SQL-чтения перед формированием ответа.

| gRPC status | Действие клиента |
| --- | --- |
| InvalidArgument | Исправить ID, cursor, TTL или размер страницы |
| AlreadyExists | Этот request ID использован с другим TTL; новый запрос требует нового ID |
| FailedPrecondition | Ресурс утрачен/истёк/отозван: отбросить частичную загрузку, новый ID и bootstrap |
| Unavailable | API/storage недоступны или исход неоднозначен; повторить прежний запрос |
| DeadlineExceeded / Canceled | Прекратить текущий вызов; повтор создания сохраняет ID/TTL |
| PermissionDenied / Unauthenticated | Исправить сервисные credentials/права |

Клиент и сервер ограничивают весь RPC бюджетом **3 секунды**, сохраняя более
ранний caller deadline; Build и первое Page делят один бюджет. Context доходит
до pgx, SQL-ожидания прерываются. Это ограничение операции, не измеренный SLA.
При большой материализации бюджет/размеры потребуется оценить отдельно.
Вызов страницы не выполняет handoff/ack, не продлевает pin и не подключает цены.
Runtime, matcher, replay, cleaner и нагрузка вне результата.

Проверка: `go test -race -count=1 ./internal/api/snapshotrpc` — реальный mTLS
loopback, права, страницы/ошибки, точность, сроки и отмена. Реальный PostgreSQL:
`GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py` — disposable
стенд, две связи/пула, потеря ответа после commit, конкурентные повторы, expiry,
abandon, foreign owner, SQL-отмена. Рабочая БД не изменяется.

## Диагностика срока и часов (SUB-024)

Целевая SQL/race-проверка на существующем disposable runner:
`python3 scripts/check_postgres.py --snapshot-only`. Она запускает snapshot/pin
storage, восстановление потерянного RPC-ответа после commit, SQL-lock cancellation,
настоящий expiry и модель рассинхронизации часов. Полный runner остаётся доступен.

PostgreSQL задаёт абсолютный `expires_at` через `clock_timestamp()`, а RPC
проверяет его через часы Go. Перед и после SQL/RPC-сценариев fixture сохраняет
SQL-время между двумя замерами Go. Весь возможный интервал смещения должен
помещаться в ±50 ms: половину прежнего минимального тестового TTL 100 ms.
Это предусловие короткого теста, не продуктовая допустимая погрешность.
Нарушение даёт FAIL с временами; оно не превращается в SKIP или продление TTL.
Проверить синхронизацию часов хоста/Docker VM и задержку SQL-замера.

Журнал `before/after SQL BuildRequest` и `after SQL Page / before RPC expiry`
позволяет отличить SQL `ErrSnapshotLost` от отказа после успешного Page.
Дополнительных SQL-запросов между Build и Page нет. Управляемая модель −2 min
на границе Store/RPC показывает успешные реальные SQL-операции и отказ RPC;
системные часы и durable строки тест не меняет. Эта модель доказывает ограничение
часовых доменов, но не устанавливает причину исторического сбоя 29 октября:
его SQL/Go времена не были сохранены. Факты и команды — в
[отчёте SUB-024](../../../docs/measurements/SUB-024/diagnosis.md).
