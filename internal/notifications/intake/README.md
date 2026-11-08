# Durable приём AlertTriggered — NTF-001

`New(Config{Brokers, Topic, Group}, NewStore(notificationsPool))`, `Step(ctx)` /
`Run(ctx)` и `Close()` принимают запросы из префиксного delivery-requests.
Consumer group распределяет 16 партиций; группа стабильна для среды. Методы
consumer вызываются последовательно; отмена контекста завершает ожидание.
Runtime и запуск процесса подключаются отдельно.

Перед чтением каждого назначения `AdjustFetchOffsetsFn` регистрирует SQL
checkpoint каждой партиции и восстанавливает абсолютные позиции. Первый приём
начинается с нуля; broker committed offset не заменяет SQL. Потеря необходимой
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
новый с теми же topic/group/БД. Невалидная запись останавливает весь экземпляр,
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
порядка fetch/SQL CAS/уникальности; отдельный fault/load прогон реплик не заявлен.

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
с ErrIdentity. Пока registry отсутствует, legacy путь совместим. Transport и
cmd/notifications ещё используют его; переключение — OPS-065/066. Экспортера нет.

Проверка: `GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py`.
Только этот SQL-контракт: та же команда с `--binding-only`.
Первой фазой runner запускает `intake/testdata/binding` на disposable notifications
БД с SQL fixtures: legacy seed **до миграции 011**, snapshot всех четырёх исходов/
reports, fault/rollback/deferred commit, concurrent bootstrap/old writer/CAS,
MVCC read и cancellation. Test-only reset очищает только этот disposable intake
перед общими legacy-тестами; никогда не применять fixtures к рабочей БД.
Без POSTGRES_TEST — явный skip. Kafka/runtime/process recovery здесь не проверяются.
