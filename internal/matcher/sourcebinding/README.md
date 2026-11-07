# Source binding matcher — OPS-061

Kafka identity — `kafkasource.Identity{ClusterID, TopicID, Topic}` из OPS-060.
Logical consumer (projection ID либо стабильный prices scope), SQL generation
(snapshot/ProjectionID), fencing token и Kafka source — разные идентичности.
Partition/offset хранятся отдельно; C никогда не преобразуется в Kafka offset.
Ожидаемые IDs приходят из доверенного provisioning, а provenance старых позиций
— из SQL. Go sourcebound API не заменяет transport Verify/CheckFetches.

## Транзакции

Projection Handle под прежним fencing gate и row lock связывает только пустую
transport позицию, затем фиксирует binding, after-images, C и offset одним commit.
Checkpoint/Recover читают origin+cursor+binding одним SELECT; Walk использует
одну read-only RepeatableRead view. Snapshot/replay без Kafka сохраняет nil Source.

Prices Accept под fencing gate и transaction advisory lock стабильного scope
создаёт binding и Input/candidates одной транзакцией. Legacy INSERT work и
checkpoint используют тот же scope lock, поэтому не могут проскочить первое
связывание. Scope с любым прежним work/checkpoint отклоняется; существующие
Pending/исходы не удаляются. Get читает binding и Input/исходы в одной MVCC view.
Record/Complete проверяют immutable Position.Source, актуальный grant и прежнюю
ProjectionID как audit. Complete фиксирует состояние и CAS Prefix атомарно;
ReadCheckpoint объединяет source и cursor одним SELECT. ReadScope (OPS-063)
проверяет весь prices scope: ledger, любой исторический work и checkpoints одной
SQL-view до выбора bootstrap, даже без cursor. Scope binding до cursor не разрешает
cleaner удалять что-либо. Все SQL locks отпускаются до RPC/ack.
Advisory hash collision только сериализует чужие scope, не смешивает данные.

## SQL граница и совместимость

Миграция `migrations/matcher/007_source_binding.sql` добавляет nullable identity к projection и отдельный
immutable prices scope ledger. Старые строки/Pending остаются без backfill.
Ledger лишён INSERT/UPDATE/DELETE у matcher_runtime, checkpoints — DELETE.
INVOKER triggers запрещают runtime менять bound transport или устанавливать/
снимать identity. Старый буквальный INSERT/UPDATE checkpoints не продвигает bound
позицию; guard остаётся действовать после revoke/takeover. В unbound состоянии
прежний SQL работает, но не является подтверждением для cleaner.

Четыре узкие SECURITY DEFINER функции matcher_projection_source/advance и
matcher_prices_source/advance работают внутри caller transaction, сверяют
immutable source, отказывают legacy и сохраняют монотонность/CAS. Владелец —
matcher_migration; search_path фиксирован pg_catalog,app, таблицы квалифицированы,
PUBLIC EXECUTE отозван. Runtime не владеет таблицами/функциями, не может менять
triggers или SET ROLE matcher_migration. Нет доверия к GUC/application_name.
Fencing проверяется stores внутри той же транзакции по существующему протоколу
MAT-017; SQL функции не являются альтернативным API назначения грантов.
Произвольное выполнение нового SQL от имени доверенной runtime роли не является
протоколом приложения. Транспортные IDs также не защищают от клона broker storage
с теми же IDs, как описано в OPS-060.

`sourcebinding.ErrUnbound` отличает legacy от отсутствующей позиции;
`kafkasource.ErrIdentity` — invalid ID, ErrMismatch — чужой source. SQL P0610/P0611
сохраняются в wrapped error. Ошибки/неизвестный commit не разрешают ack; повтор
использует исходные Input/Batch/Prefix и durable чтение, без присвоения новых IDs.

## Безопасный запуск и проверка

Остановить старые writers, применить `python3 scripts/migrate.py --owner matcher`
на согласованном стенде, затем запустить
sourcebound runtime OPS-062 с доверенными IDs. Отсутствие transport cursor у
новой snapshot projection нормально. Legacy cursor/Pending вызывает явный отказ;
не сбрасывать его на latest, не подставлять config/metadata, не менять scope ради
обхода проверки, не удалять старое состояние. Автоматический перенос не реализован.
Runtime подключён в OPS-062; явные переменные и process-проверки —
[cmd/matcher](../../../cmd/matcher/README.md).

`GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py` поднимает
готовый disposable PostgreSQL, применяет миграции/fixture и запускает race tests.
Проверяются два pool, source conflict при равных offsets, snapshot/replay,
legacy, takeover/Pending/canonical outcomes, старый буквальный SQL и его гонка,
rollback на записи и deferred commit. Kafka здесь не требуется: identity/fetch
уже проверены OPS-060. Рабочая БД не мигрируется. Нагрузочная стоимость gate,
advisory locks и SQL-функций не измерялась.
