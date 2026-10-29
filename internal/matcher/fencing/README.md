# SQL fencing matcher — MAT-017

Миграция `migrations/matcher/005_fencing.sql` применяется runner владельца matcher.
`fencing.New(pool)` использует matcher_runtime и только собственную БД.
Полномочия SQL не подтверждают Kafka-владение: MAT-018/010 проверяет актуальное
назначение группы до выдачи token и прекращает работу/готовность при его потере.

| Метод | Контракт |
| --- | --- |
| `Current(ctx, Identity)` | Последний durable Grant и active; отсутствие — `pgx.ErrNoRows`. Нужен для `ExpectedToken`, наблюдение не разрешает последующую запись. |
| `Assign(ctx, Request)` | CAS предыдущего token (0 только при первом назначении). Один стабильный RequestID и неизменный полный запрос на все повторы. Возвращает Grant с token 1..MaxInt64. |
| `Revoke(ctx, Grant)` | Условный отзыв именно этого Grant. Повтор успешен; старый Grant не отзывает преемника. |
| `Lock` + `Price` / `Projection` | Проверка внутри той же READ COMMITTED транзакции, что и запись. Встроена в stores; не заменяется проверкой перед Begin. |

`Identity{Environment, Group, Topic, Partition}` — фиксированная ценовая
партиция. `Scope` навсегда связывается с одной Identity; нельзя менять его на
PID, token или projection generation. Так следующий владелец получает прежние
Pending/Get, canonical исходы и checkpoint. Другой scope для той же Identity
и та же scope для другой Identity отклоняются с `ErrAuthority`.
`ProjectionID` — полная replica-local view подписок; C не шардируется по ценовым
партициям. У разных реплик разные ID. Все назначения реплики регистрируются
через Assign с одним ProjectionID **до начала обработки**, затем передаются
полным набором в `projection.NewFenced(pool, id, grants)`.

Assign сохраняет token, grant, членство проекции и исход запроса одной
транзакцией. При неизвестном результате commit повторяется **тот же** Request,
включая ExpectedToken. Нельзя читать Current и выдавать новый request ID из-за
потери ответа. Поздний повтор возвращает исходный Grant, даже если уже отозван
или заменён: последующая запись всё равно проверяет актуальность.
`ErrExhausted` запрещает переполнение/сброс MaxInt64. Ledger запросов, scope и
старое членство проекции не удаляются при рестарте или отзыве.

`pricework.NewFenced(pool, grant)` проверяет текущий Grant для Accept/Record/
Complete, включая точные terminal повторы и Prefix. Новый Input обязан
ссылаться на Grant.ProjectionID; Position, если есть, обязан совпадать с
Topic/Partition. Latest имеет Position=nil: без фиктивного offset. Повтор
**уже принятого точного Input**, Record и Complete могут продолжать работу со
старой ProjectionID: это immutable аудит принятия. Пересборку актуальной view,
Latest и кандидатов выполняет MAT-005/010 перед оставшимися запросами API.
C, subscription-changes NextOffset, prices Prefix, ревизия цены и token —
независимые шкалы. Соответствие пары фиксированному partition hash доказывает
транспортный caller; наличие SQL Grant само этого не доказывает.

`projection.NewFenced` копирует набор Grants. Initialize/ApplyReplay/Handle
требуют точного совпадения полного набора с durable членством и актуальности
каждого token. Отзыв **любой** партиции запрещает прежнему writer все изменения
полной проекции, включая дубли, не дробя C. При смене набора caller останавливает
writer, получает назначения и создаёт новый store; для меньшего набора — новое
ProjectionID (историческое членство не удаляется). Повторное назначение той же
партиции с тем же ProjectionID заменяет её token в членстве; прежний store
остаётся fenced. Caller сериализует один writer/ack этой view.

Старые `New` продолжают обслуживать только изолированные незащищённые
scope/id. С момента Assign они не могут менять зарегистрированный scope или
projection ID, даже после revoke и даже при использовании старого экземпляра
store. Нельзя обойти защищённую ProjectionID через Accept в другом legacy
scope. Чтение Get/Pending/Checkpoint/Recover/Walk не выдаёт полномочий и остаётся
доступным для восстановления/диагностики. Изолированная загрузка immutable
snapshot в snapshotload не привязывается к ценовому token/offset; копирование
его в изменяемую защищённую проекцию уже проверяет Grants.

Все изменяющие stores сначала берут совместимую `FOR SHARE` блокировку
единственной строки matcher_fence_gate. Assign/Revoke берут `FOR UPDATE` той же
строки; блокировка удерживается до commit/rollback. Поэтому параллельные
записи совместимы, но изменение полномочий ждёт завершения ранее разрешённых
транзакций; после его commit старый writer уже не сможет зафиксироваться.
Проверка членства/актуальности идёт отдельными SQL-запросами после блокировки
в READ COMMITTED, чтобы увидеть новое назначение даже после ожидания gate.
Это короткая транзакционная SQL-сериализация без TTL/lease или отдельного
распределённого lock-сервиса. Она охватывает и legacy writers, поэтому
регистрация защиты также сериализована с уже начатыми legacy записями.

Gate общий для БД matcher: смена назначения может ждать любую текущую
изменяющую транзакцию matcher. Нагрузочные характеристики здесь не измерены.
Не удерживать транзакцию во время Kafka/RPC/ack. Ack вызывается только после
успешного commit; ErrAuthority/SQL/commit error не разрешают ack. Между commit
и callback назначение может измениться: MAT-018 дополнительно проверяет
актуальность Kafka-назначения непосредственно при broker commit.

Проверки: `GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py` —
готовый disposable PostgreSQL runner с race detector, независимыми pool,
идемпотентностью/CAS, отзывом, исчерпанием, rollback/deferred commit, сохранением
Pending/canonical результатов и fencing всех путей записи без ack.
Runtime, Kafka group и восстановление consumer после отказа — MAT-018/010/011.
