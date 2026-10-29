# Проекция подписок matcher — MAT-001

`New(pool, projectionID)` использует только собственную БД matcher. Схема —
`migrations/matcher/001_subscription_projection.sql`, runtime её не создаёт.
Идентификатор проекции стабилен при рестарте; будущее поколение snapshot может
использовать отдельный ID. Этот пакет не создаёт snapshot и не переключает активное поколение.
[MAT-007](../snapshotload/README.md) сохраняет полный снимок в отдельных
staging-таблицах; загрузка сама по себе не меняет этот рабочий checkpoint.

`Handle(ctx, Batch, ack)` принимает **одну целую транзакцию** API-журнала:
исходные JSON-конверты SubscriptionChanged в порядке position, с общим
transaction_end и последней position=C. Проверяются контракт, последовательность
записей и непрерывность относительно durable C (для новой проекции C=0).
Ошибка схемы/контракта, пропуск или незавершённая транзакция возвращают ошибку
без ack. Caller останавливает партицию; пропуск закрывается SQL-replay будущего
MAT-008, потеря pin — новым bootstrap. Повторы целых транзакций до C безопасны.

Одна SQL-транзакция блокирует checkpoint проекции, применяет полные after-image
только с большей subscription_version и сохраняет C плюс NextOffset. Старые и
равные версии не меняют состояние, но обработанная позиция продвигается.
Terminal delivered/cancelled остаются версионированными tombstone: физического
удаления нет, старый active не возрождает подписку. Сохраняются точные цены,
исходное время и workflow, включая active после исчезновения условия. Наличие
записи не означает ценовую готовность или допуск отправки.

`Batch.Topic/Partition` фиксируются при первом commit для этого projectionID;
менять их молча нельзя. NextOffset — **следующий Kafka offset**, отдельный от C.
Caller обязан подавать непрерывный обработанный префикс одной партиции через
read_committed, без auto-commit, с последовательными Handle/ack. Физические
Kafka offsets могут иметь пропуски из-за aborted transactions; они не проверяются
как позиции API-журнала. При повторной публикации C не изменяется, NextOffset
может продвинуться. При повторе старого offset подтверждается текущий durable
NextOffset. Нельзя подавать сюда произвольные выборки Kafka или ценовые события.

Ack вызывается **после commit**, без SQL-блокировок. Ошибка/неопределённость commit
или ошибка ack возвращаются вызывающему: тот повторяет тот же batch. SQL-checkpoint
авторитетен; ошибка ack не откатывает проекцию. `Checkpoint` читает C/NextOffset
(нулевые до первого commit), `Get` — полный after-image, включая terminal;
отсутствие записи даёт pgx.ErrNoRows. Параллельные SQL-повторы сериализованы строкой
checkpoint; распределённое владение и fencing относятся к MAT-010/011.

Kafka polling/сборка batch, bootstrap/replay/live, перестроение индекса,
подключение последних цен и runtime выполняются отдельными тикетами.

Проверка: `python3 scripts/check_postgres.py` — реальный disposable PostgreSQL,
два runtime pool, race detector; обычный `go test ./internal/matcher/projection`
проверяет валидацию, storage-тесты без POSTGRES_TEST пропускаются. Fixture
`infra/postgres/fixtures/go_matcher/9223372036854775806_projection_failure.sql`
вызывает ошибку UPDATE checkpoint после записи after-image только для тестовой
проекции: проверяется rollback обеих частей, без production fault hooks.

## Snapshot и SQL-replay — MAT-014

Миграция `003_snapshot_projection.sql` добавляет origin snapshot и отсутствие
Kafka identity до первого реального `Handle`. `Initialize(ctx, snapshotID)`
атомарно копирует **только полное** поколение MAT-007 в отдельный `projectionID`
и устанавливает исходную C. Клиентская память ограничена страницей 1000 строк;
вся инициализация остаётся одной SQL-транзакцией. Пустой снимок допустим.
Повтор с тем же origin ничего не переписывает, в том числе после replay;
другой origin или существующая Kafka-проекция дают `ErrProjectionExists`.
Никакой указатель рабочего поколения не переключается. Проверка expiry/pin
и handoff остаются у MAT-008: complete означает лишь завершённую загрузку.

`ApplyReplay(ctx, events)` принимает одну целую CON-002 транзакцию из SUB-022.
Декодирование и проверка порядка общие с Kafka-путём. Новая транзакция начинается
на durable C+1 и заканчивается на transaction_end; пропуск, пересечение C,
незавершённая транзакция и неверный контракт отклоняются. Целые повторы до C
безопасны. Версии, tombstones и C обновляются атомарно под блокировкой checkpoint.
Сохраняется полный after-image источника; меньшая/равная версия его не заменяет.
SQL-путь требует предварительного Initialize, не создаёт topic/partition и не
меняет NextOffset. После неоднозначного commit повторить те же events через
любой pool, прочитать `Checkpoint`, и только потом подтверждать API. Сам пакет
не вызывает AcknowledgeReplay и не объявляет readiness.

Первый реальный Kafka batch устанавливает topic/partition даже при C-дубле;
он может продвинуть только свой обработанный NextOffset. Последующий SQL-replay
сохраняет транспортную позицию. Проверять непрерывность **реального** Kafka
префикса обязан caller MAT-015/008, как и раньше в MAT-001.

`ReadImage(ctx, subscriptionID)` возвращает `Image` с ровно одним полем:
`Snapshot` — исходный полный protobuf MAT-007, либо `Change` — последний полный
CON-002 after-image после более новой версии. У снимка сохраняются owner,
optional percent, flags, workflow, blocked, исходные decimal-строки и наносекунды.
Snapshot не получает выдуманных journal positions. При более новой версии
источником становится CON-002: отсутствующие в нём snapshot audit-поля не
синтезируются и старый blocked не переносится в новое состояние. Исходный
immutable снимок остаётся в MAT-007. `Get` сохраняет интерфейс MAT-001 для
журнальных записей, а для snapshot-представления возвращает `ErrSnapshotImage`.

`Walk(ctx, visit)` обходит все after-images одной проекции, включая tombstones,
по ID в одном REPEATABLE READ view; возвращает checkpoint того же view. Callback
получает `Image`, должен учитывать context; ошибка callback прекращает обход.
Конкурентный replay не смешивает строки/С разных моментов; координация изменений
после обхода и переключение индекса выполняются MAT-008. Ошибка/отмена обхода
означает неполный результат: возвращённый checkpoint нельзя использовать для
активации. Несуществующие строки/generation дают `pgx.ErrNoRows`, неполное
поколение — `ErrIncomplete`, ошибки последовательности — `ErrSequence`,
контракт/SQL/context передаются caller. Checkpoint отсутствующей проекции
остаётся нулевым для совместимости MAT-001; Initialize/Walk проверяют существование.

Проверка тем же `scripts/check_postgres.py`: MAT-007 pages → Initialize →
SQL-replay → реальный Kafka batch, два pools, конкурентные повторы, uint64 C
выше int64, полные protobuf/точные цены, rollback после after-image, пустой
снимок, изоляция проекций и согласованность Walk при конкурентном commit.

## Чтение restart-состояния — MAT-009

`Recover(ctx)` возвращает `Recovery{SnapshotID, Checkpoint, Topic, Partition}` из
одного SQL view checkpoint и origin поколения. Не создаёт и не сбрасывает строки.
Нет полного snapshot-origin, C ниже исходной границы либо повреждена транспортная
идентичность — `ErrRecovery`, требуется новый bootstrap. Прочие SQL/context ошибки
передаются caller. До первого Kafka Handle Topic пуст и NextOffset=0; иначе это
точная SQL-позиция для `changefeed.RegisterAt`. C и NextOffset не смешиваются.
Срок загрузки снимка не проверяется: действительность replay-pin устанавливает
`live.NewResume` через API. Идентификатор поколения выбирает внешний владелец;
распределённое владение и активный указатель runtime этим методом не создаются.

## Владелец подписки — SUB-025

`ReadImage` возвращает durable owner из `Snapshot.OwnerId` либо `Change.OwnerID`
(payload v2), включая новые подписки после C и обновления snapshot-подписок.
`image.OwnerID() (string, error)` унифицирует обе формы для MAT-004. После чтения
проверить ошибку, взять canonical subscription ID/версию из той же Image и
передать оба ID в SUB-014. Это не обходит каноническую проверку API. Новый
after-image заменяет snapshot целиком, owner из прежнего snapshot не наследуется.

`ErrOwnerMissing` запрещает использование старой проекции без owner: её
возвращают Get/ReadImage/Walk и оба входных пути для v1 (в том числе дублей).
Не вызывать RequestDelivery/ack и не объявлять readiness. NewResume возвращает
также live.ErrBootstrap; нужен свежий snapshot с новым projection ID. При Walk
ошибка означает неполный обход. Snapshot и v2 проверяются до построения live
индекса; отсутствующий owner не подменяется владельцем pin/сертификата.

Миграция таблиц не требуется; политика v1/v2 и порядок безопасного обновления
писателей/relay/bootstrap описаны в [контракте](../../contracts/subscriptionchanged/README.md#владелец-и-совместимость--sub-025).
Тесты проверяют реальные API journal bytes через SQL ApplyReplay и Kafka Handle,
повторное открытие pool, snapshot → новая версия → tombstone, legacy отказ без
продвижения C/offset и bootstrap в отдельное поколение.

## SQL-полномочия — MAT-017

`NewFenced(pool, id, []fencing.Grant)` требует полный набор ценовых назначений
реплики. Initialize/ApplyReplay/Handle, включая дубли, проверяют все tokens
в той же транзакции, что after-images/C/checkpoint. Отзыв любой партиции
останавливает прежний writer полной view; C не шардируется. Старый New не
обходит уже защищённый ID. Snapshotload сохраняет изолированный immutable
снимок без ценового offset; защищено его Initialize в изменяемую view.
Протокол выдачи/отзыва и смены членства — [fencing](../fencing/README.md).
