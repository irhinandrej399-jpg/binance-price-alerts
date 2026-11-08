# Durable приём AlertTriggered — NTF-001

`New(Config{Brokers, Topic, Group, Binding}, NewStore(notificationsPool))`, `Step(ctx)` /
`Run(ctx)` и `Close()` принимают запросы из префиксного delivery-requests.
Consumer group распределяет 16 партиций; группа стабильна для среды. Методы
consumer вызываются последовательно; отмена контекста завершает ожидание.
Runtime и запуск процесса подключаются отдельно.

Перед чтением каждого назначения `AdjustFetchOffsetsFn` читает source-bound SQL
прогресс каждой партиции через `ReadProgress` и восстанавливает абсолютные позиции.
Пустой intake требует отдельного явного `Bootstrap`; consumer его не выполняет.
Broker committed offset не заменяет SQL, включая позицию впереди SQL. Потеря необходимой
истории, offset вне retained начала / read_committed LSO и fetch errors явно
останавливают consumer. Автоматического reset/skip нет. Один логический intake
на topic: SQL-ключ `(topic, partition)` не зависит от replica/group member.
При переименовании topic или сбросе SQL требуется отдельный операторский план;
checkpoint не разрешает удалять историю, пока не подключён безопасный cleaner.

Kafka настроен на read_committed, DisableAutoCommit, NoResetOffset и
BlockRebalanceOnPoll. Step читает одну запись, проверяет AlertTriggered/ключ,
затем блокирует строку checkpoint и сохраняет job плюс следующий offset одной
транзакцией notifications (бюджет 5 с). SQL-позиция сравнивается с cursor
предыдущей принятой записи; abort/control records могут оставлять численные
пропуски offsets. Непрерывность доказывает последовательный fetch без skip,
а не равенство соседних чисел. CAS не позволяет устаревшему reader перепрыгнуть
уже изменившийся SQL cursor; повтор ниже durable позиции ничего не меняет.

Только успешный SQL commit разрешает явный CommitOffsetsSync (бюджет 3 с).
Любая ошибка Step завершает работу экземпляра; следует закрыть его и создать
новый с теми же binding/topic/group/БД. Невалидная запись останавливает весь экземпляр,
включая её партицию, с ErrContract и координатами; автоматического ack после
повторов нет. Частичный успех/неизвестный исход SQL commit безопасно перечитывается
из SQL; потерянный Kafka ack не меняет принятое задание.

Миграция `notifications/001_delivery_jobs.sql`: trigger_id — NOT NULL primary key,
сохраняются subscription/user/event ID, исходные байты и начальное state=pending.
`ON CONFLICT(trigger_id) DO NOTHING` сохраняет первоначальное задание и его
текущее состояние даже при новом event_id/версии workflow после wait_condition.
Событие не разрешает попытку Telegram. Слот, подписка и outbox API здесь не
изменяются; роль notifications не имеет CONNECT к остальным БД. Worker leases,
попытки, проверки цены, результаты и Telegram — последующие задачи; их schema
расширяется отдельными миграциями. Поведение при rebalance безопасно за счёт
порядка fetch/SQL CAS/уникальности. Rebalance двух members проверен в OPS-065
(см. ниже); нагрузка не измерялась.

Память ограничена одной доставленной записью, fetch/decompression до 1 MiB,
ответом до 2 MiB; приём записи — до 8 KiB (бюджет producer SingleRecord), ключ до 256. Сохраняются точные
JSON-цены/uint64 версии без float. Нагрузочная производительность не измерялась.

Проверка реальных PostgreSQL/Kafka с race detector и удалением disposable volumes:

```sh
KAFKA_RUN_POSTGRES=1 GOCACHE=/tmp/binance-go-build bash infra/kafka/verify-go.sh
```

Runner миграций применяет notifications и test-only checkpoint/deferred commit
failures. Без POSTGRES_TEST SQL-тесты пропускаются; Kafka-тесты требуют также
KAFKA_INTEGRATION и конфигурацию тестового брокера. Обычный `go test` не заменяет
совместный opt-in прогон.

Остановка владельца вызывает Close после возврата Run/Step: AllowRebalance,
LeaveGroupContext с отдельным пределом 2 с, затем отмена собственного client
context и Close/join локального клиента (включая JoinGroup/SyncGroup).
Ответ брокера о leave не является условием сохранности принятой работы: при
истечении срока membership может оставаться до session expiry; restart
продолжает работу по SQL checkpoint. Общий runtime deadline — cmd/notifications.

## Source-bound SQL intake — OPS-064

После миграции `notifications/011_intake_source.sql` доступен SQL-контракт:

```go
b := intake.Binding{
    ConsumerID: "local-notifications", // стабильный logical intake этой БД
    Generation: "<новый уникальный ID из operator inventory>",
    Source: source, // kafkasource.Identity: ClusterID, ненулевой TopicID, Topic
}
err := store.Bootstrap(ctx, b, partitions) // только явное provisioning пустой БД
p, err := store.ReadProgress(ctx, b, partition)
err = store.AcceptBound(ctx, b, partition, expected, offset, key, originalBytes)
```

В одной БД notifications — один logical intake, один source и одно поколение
bootstrap. Это продолжение прежнего ограничения одного intake; consumer/group
member/replica не являются владельцами нового namespace. `ConsumerID` сохраняется
между рестартами/переназначениями. `Generation` создаёт оператор при **каждом новом
пустом SQL bootstrap**, хранит вне этой БД и явно регистрирует в ожиданиях cleaner.
Повторный bootstrap существующей БД использует прежнюю generation. Уничтоженная
БД не может доказать, что оператор повторно использовал старый ID; конфигурация
и внешний inventory обязательны. Копии БД с сохранёнными IDs неразличимы.

Перед provisioning оператор сверяет ожидаемые Kafka ClusterID/TopicID/Topic и
полный набор партиций. Bootstrap сам не читает metadata, не принимает latest
или broker committed position. Все партиции получают next=0 атомарно с binding.
Порядок массива несущественен; пустые/повторяющиеся/отрицательные partitions
отвергаются. Повтор с теми же IDs/source/набором идемпотентен, продвинутые offsets
сохраняются. Расширение набора, смена source/generation/consumer требуют отдельного
операторского плана; данный API их не выполняет. До запуска transport необходимо
проверить retained диапазон нулевых/восстановленных позиций (OPS-065).

Миграция только добавляет immutable singleton registry, функции и guards;
не переписывает jobs/bytes/claims/attempts/исходы/reports/checkpoints. Любые
существующие jobs **или checkpoints, даже next=0**, без registry — legacy.
Bootstrap и bound Accept отказывают с `ErrUnbound`; источник никогда не берётся
из нынешней metadata/config. Автоматического переноса/backfill/reset нет.

`ReadProgress` — один SELECT/MVCC snapshot persisted consumer/generation/source,
partition и next. Нет строки/партиции — `ErrMissing`, legacy — `ErrUnbound`,
несовпадение — `kafkasource.ErrMismatch`. Нулевой returned struct при ошибке
не означает прогресс 0 и не разрешает очистку. SQL cancellation/connection errors
передаются вызывающему; caller задаёт deadline. Успешный ответ доказывает только
этот intake/partition, а не полноту реестра обязательных consumers cleaner.

`AcceptBound` проверяет binding до duplicate-ветки, держит row lock checkpoint
и сохраняет исходные bytes/уникальный trigger_id и next одной SQL-транзакцией.
CAS/offset gaps имеют прежний контракт; повтор не сбрасывает работу worker.
Ошибка или неопределённый commit запрещает ack; следующий экземпляр перечитывает
SQL. `ErrCheckpoint` означает конфликт префикса; malformed request — ErrContract;
пустая consumer/generation — ErrConfig, неверный source — ErrIdentity.
SQLSTATE P0640/1/2/3 соответствуют Unbound/Missing/Mismatch/Checkpoint.

Registry закрыт для runtime INSERT/UPDATE/DELETE. SECURITY DEFINER функции с
фиксированным search_path и owner notifications_migration открыты только runtime
(EXECUTE), PUBLIC отозван. Runtime не является членом migration role. INVOKER
triggers запрещают прямые INSERT jobs и INSERT/UPDATE checkpoints после binding,
включая другой topic/партицию и job `ON CONFLICT`; GUC/application_name не дают
обхода. Поля входного job также защищены от UPDATE; worker обновляет свои
state/lease/scheduling/blocked поля и таблицы исходов через прежние права.
Это не закрывает независимый исторический долг прав NTF-017.

Bootstrap и legacy SQL writers сериализованы advisory lock `(64064,1)`.
Они требуют READ COMMITTED, иначе SQLSTATE 25000: snapshot более строгой изоляции
не должен скрывать только что принятый binding. Source-bound Accept читает
immutable registry и блокирует только свою partition. После bootstrap старый
`Checkpoint` отвергается SQL guard, старый `Accept` также отказывает до duplicate
с ErrIdentity. Пока registry отсутствует, legacy путь совместим. Transport OPS-065 использует только bound API. Старые вызовы cmd/notifications
и process fixture не передают Binding и теперь явно отвергаются; их переключение —
OPS-066. Экспортера нет.

Проверка: `GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py`.
Только этот SQL-контракт: та же команда с `--binding-only`.
Первой фазой runner запускает `intake/testdata/binding` на disposable notifications
БД с SQL fixtures: legacy seed **до миграции 011**, snapshot всех четырёх исходов/
reports, fault/rollback/deferred commit, concurrent bootstrap/old writer/CAS,
MVCC read и cancellation. Test-only reset очищает только этот disposable intake
перед общими legacy-тестами; никогда не применять fixtures к рабочей БД.
Без POSTGRES_TEST — явный skip. Kafka/runtime/process recovery здесь не проверяются.

## Source-bound consumer — OPS-065

```go
consumer, err := intake.New(intake.Config{
    Brokers: brokers, Topic: b.Source.Topic, Group: stableEnvironmentGroup,
    Binding: b, // ConsumerID / Generation / Source из operator inventory
}, store)
// provisioning отдельно: store.Bootstrap(ctx, b, partitions)
err = consumer.Run(ctx)
consumer.Close() // после возврата Run/Step, до pool.Close
```

`Binding.Validate` обязателен; пустые consumer/generation, нулевой source и
несовпадение Config.Topic/Source.Topic отвергаются. В Config также есть `Dialer`
для доверенного transport и узких wire tests. Новый group member/реплика и
рестарт используют прежние ConsumerID/Generation/Source, а не создают IDs заново.
Текущая metadata никогда не становится provenance старого SQL cursor.

Assignment сверяет metadata без auto-create перед чтением SQL и после проверки
retained start/read_committed LSO. Он не вызывает Checkpoint/Bootstrap и не
создаёт отсутствующую partition. `ErrMissing`, `ErrUnbound`, mismatch или потеря
истории завершают экземпляр без reset/skip. Returned позиции происходят только
из persisted `ReadProgress`; broker offsets игнорируются. Source/generation
сверяются также атомарно внутри `AcceptBound`, в том числе для duplicate/CAS.

Каждый Step проверяет metadata до poll и после него. Poll ограничен одной
секундой, так что Run проверяет metadata и на пустом потоке. Затем исходные
fetch responses проходят `Source.CheckFetches` **до** Records/decode/Accept;
KeepControlRecords сохраняет commit/abort markers для проверки ID, после чего
они пропускаются без SQL/ack. Step может вернуть nil после idle poll/marker.
Read_committed, одна доставленная запись, прежние fetch/decompression/SQL/ack
лимиты и Close/join сохранены. Любая ошибка Step/assignment остаётся terminal;
владелец вызывает Close после выхода Run/Step. Проверки требуют доступной metadata,
даже если buffered data уже есть; нагрузочная цена этого режима не измерена.

Транспорт проверяет Kafka ClusterID/TopicID/имя, SQL доказывает provenance
сохранённой позиции. Граница доверия и ограничение клонов storage с теми же IDs —
[kafkasource](../../platform/kafkasource/README.md). Автопереноса source/legacy нет.

Переход вызывающего кода (OPS-066): добавить обязательные операторские
ConsumerID/Generation/ClusterID/TopicID, передать Config.Binding, выполнить
явный bootstrap **только нового пустого intake** и обновить process fixture.
Нынешний cmd компилируется, но intake.New отвергает его конфигурацию без Binding;
прежние process smoke до OPS-066 не подтверждают запуск нового транспорта.

Проверки: существующий `KAFKA_RUN_POSTGRES=1 bash infra/kafka/verify-go.sh`
с реальным PostgreSQL/Kafka и race detector: durable commit/потерянный ack/restart,
broker offset впереди SQL, rebalance двух members с тем же binding, неизвестный/
legacy intake, потеря retained истории, delete/recreate уникального disposable
topic с перекрывающимися offsets. `check_postgres.py` запускает transport в отдельной фазе до общих legacy writers
(внутренний opt-in `INTAKE_TRANSPORT_TEST=1`). Fixtures последовательно используют
существующий test-only `test_reset_delivery_source` до/после своего intake;
к рабочей БД этот fixture неприменим. Unit/wire проверки source data/control,
нулевого ID, metadata failures, idle и отмены работают без внешнего брокера.
