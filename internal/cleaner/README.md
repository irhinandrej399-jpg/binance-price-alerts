# Локальная безопасная очистка Kafka — OPS-014

`Engine` выполняет одну maintenance-операцию над одной source/partition.
[Решение](../../docs/decisions/OPS-014.md): приватный постоянный локальный каталог,
файл `registry.json` и **постоянный** `registry.lock`. Все init/register/retire/
clean/inspect на этом хосте используют этот же каталог и OS flock. State
заменяется атомарно; inode lock file не заменяется. Чтение и все сетевые операции
находятся под той же блокировкой. Lock wait отменяется context; whole operation
ограничена 30 с, client/server RPC — 1 с, broker operation — 5 с.

## Контракт реестра

`Inventory{Source, Partition, Consumers}` содержит полный явно заданный набор
обязательных SQL consumers. Source — точные ClusterID, ненулевой TopicID и topic.
Consumer `{Name, Kind, ID, Generation, Endpoint}`:

| Kind | ID | Generation |
| --- | --- | --- |
| `projection` | точный matcher SQL projection_id | отсутствует |
| `prices` | стабильный matcher SQL scope | отсутствует |
| `notifications` | logical intake ConsumerID | обязательная SQL bootstrap Generation |

Endpoint — alias клиентского RPC config; не адрес БД, не replica/member и не
Kafka group. Name и exact SQL identity уникальны, в том числе среди retired
записей. Init требует непустой явный inventory, проверяет Kafka source/partition
и retained start/read_committed LSO, но **не объявляет неизвестные SQL proofs
успешными**. Неизвестные consumers можно включить изначально: clean откажет,
пока каждый из них не даст настоящий SQL cursor или не будет явно retired.

Регистрация нового consumer проверяет exact SQL proof не ниже
`max(broker log-start, durable floor)` и не выше LSO. Если нужна уже удалённая
история, оператор сначала делает отдельный явный bootstrap у владельца с новым
ID/generation, затем register; команда не делает reset/latest/bootstrap сама. У нынешнего notifications
Bootstrap начальный next=0: перенос на ненулевой start требует отдельного
операторского плана, такой migration adapter здесь не реализован.
Register того же активного descriptor идемпотентен. Retire требует весь прежний
точный descriptor и сохраняет tombstone; изменение generation/endpoint под
прежним Name не допускается. Новая generation получает новый Name. Offline,
timeout, shutdown и restart не означают retirement. Пустой активный набор
всегда запрещает clean.

Существующий state не создаётся заново ни на clean, ни на register. Missing,
truncated/invalid/oversized JSON, неизвестная schema version, дубликаты,
непригодный каталог и symlink lock/state дают отказ. Ограничения: 1024 consumer
записи (включая tombstones), state/config/input до 1 MiB, имена/IDs до 256 bytes.
Оператор обеспечивает полноту inventory и регистрацию до запуска consumer;
RPC одного consumer полноту не доказывает. Не очищать/не копировать реестр при
смене реплик. Повторный init существующего state запрещён.

## Граница удаления и сбои

1. Под flock читается state, проверяются актуальный Kafka source и retained
   диапазон; **каждый** активный consumer опрашивается через mTLS OPS-056/057.
   Client повторно проверяет весь response: consumer/kind/generation/source/
   partition и наличие неотрицательного next. Missing/offline/mismatch/ошибка
   запрещают DeleteRecords; broker committed offsets никогда не читаются.
2. Source/range проверяются повторно. Каждый SQL next должен быть не ниже
   log-start и прежнего floor и не выше LSO. Выбирается minimum SQL next.
   Target ограничен этим minimum и `start + maxOffsets` без переполнения.
   Число offsets — диапазон, не количество bytes/records: Kafka gaps допустимы.
   `target == start` даёт noop без DeleteRecords.
3. `floor = max(oldFloor, target)` и весь membership сохраняются через temp file
   mode 0600 → Sync → Close → Rename → directory Sync **перед каждой отправкой**,
   также на повторе прежнего target. Ошибка любого шага запрещает отправку.
   Полный resync важен после неизвестного результата предыдущего directory Sync.
4. Bounded DeleteRecords удаляет только `[start,target)`. Успех даёт decoded
   code 0/low watermark. Decoded ошибка даёт `rejected` с числовым broker code;
   transport/deadline/непригодный response — `unknown`. Оба сохраняют floor:
   старый запрос ещё может исполниться после смерти процесса/потери ответа.
   При неизвестном исходе следующая команда повторно проверяет SQL и log-start;
   может безопасно повторить target либо вернуть noop, если удаление уже произошло.

`Result` содержит status (`blocked/noop/deleted/rejected/unknown`), Start,
Minimum, Target, Floor, Attempted и optional LowWatermark/BrokerCode. На blocked
нули не являются progress proof; проверенная граница появляется после успешной
валидации всего набора. Чужие SQL DSN, broker error messages и PEM не выводятся.
Retire синхронизируется с тем же lock: выполняется до либо после всего clean,
никакого окна смены состава между proof и DeleteRecords нет.

## Использование и проверки

```go
reader, err := cleaner.NewRPCReader(endpoints) // только mTLS client files
// Проверить err; defer reader.Close().
engine := cleaner.New(cleaner.NewStore(authoritativeDir),
    cleaner.KafkaBroker{Client: kafkaClient}, reader)
err = engine.Init(ctx, inventory) // только новый явный реестр
result, err := engine.Clean(ctx, 1000)
```

Готовая команда и форматы JSON: [cmd/kafka-cleaner](../../cmd/kafka-cleaner/README.md).
Изоляция, transport и lifecycle тестов переиспользуют
[cleanerprocess](../testutil/cleanerprocess/README.md), без production fault hooks.

```sh
# Без opt-in реальный стенд явно SKIP; unit/RPC/file/process tests выполняются.
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./internal/cleaner ./cmd/kafka-cleaner
# Реальные PostgreSQL/Kafka/mTLS и настоящий CLI, race и disposable cleanup.
GOCACHE=/tmp/binance-go-build python3 scripts/check_cleaner_process.py --package ./internal/cleaner
```

Unit/process tests проверяют lock другого процесса, crash после durable floor,
permission/write failure до DeleteRecords (также на повторе прежнего target),
corruption/symlinks, tombstones, zero/empty/unknown/invalid proofs, limits/overflow
и несовпадение каждого поля RPC. Real test использует source-bound SQL owners,
два prices consumers с разным next, unknown consumer/generation, RPC failure,
concurrent register/retire, pre-request fault, настоящий broker rejection,
потерю успешных OffsetCommit/DeleteRecords responses и повтор. Прямые observers
сверяют retained read_committed key/value bytes, SQL Prefix и неизменный
canonical pending notifications job. Настоящий CLI стартует без parent env/DSN.

Область — macOS/Linux и local filesystem на одном хосте. Каталог mode 0700,
общий authoritative path на partition, надёжные fsync и отсутствие rollback
старой копии — операторские предусловия. Не удалять lock/state и не создавать
параллельные реестры. Topic lifecycle не меняется администрацией во время
операций: DeleteRecords адресует имя, а metadata не блокирует delete/recreate.
Это не HA/multi-host/NFS протокол. Внешняя потеря/клонирование storage с прежними
IDs не определяется. Retention/compaction остаются в конфигурации DEC-007,
SQL replay-pins и владельцы jobs не меняются. Непосредственное освобождение
всех физических bytes и измеренная скорость очистки не обещаются; capacity
квалифицируется отдельно.
