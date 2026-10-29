# Проекция подписок matcher — MAT-001

`New(pool, projectionID)` использует только собственную БД matcher. Схема —
`migrations/matcher/001_subscription_projection.sql`, runtime её не создаёт.
Идентификатор проекции стабилен при рестарте; будущее поколение snapshot может
использовать отдельный ID. Этот пакет не создаёт snapshot и не переключает поколения.
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
