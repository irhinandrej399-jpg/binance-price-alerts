# Групповой источник цен (MAT-018)

`NewGroup` распределяет один prices topic между живыми клиентами одной группы.
Используется закреплённый franz-go v1.22.2, eager range balancing, read_committed,
DisableAutoCommit и NoResetOffset. Один PollRecords/dispatch обслуживает полный
поток и регистрации MAT-012. Direct `New` параллельно с группой для тех же
полномочий не нужен и не доказывает владение.

```go
g, err := pricefeed.NewGroup(ownerCtx, pricefeed.GroupConfig{
    Config: pricefeed.Config{Brokers: brokers, Topic: topic, Source: expectedSource, Buffer: 256},
    Group: groupID,
    PrepareSource: func(ctx context.Context, parts []int32) (map[int32]pricefeed.Position, error) {
        // MAT-010: установить SQL tokens MAT-017; получить абсолютные NEXT
        // offsets из SQL/согласованного bootstrap. Не брать их из broker commit.
        return prepareAuthorityAndCursors(ctx, parts)
    },
})
if err != nil { return err }
defer g.Close()
a, err := g.NextAssignment(ctx, nil)
if err != nil { return err }
// a.Context()/Done() отзывает полномочия текущего назначения.
// MAT-012: a.Register(pairCtx, pair, pricefeed.PairPartition(pair)), затем Latest.
u, err := a.Next(ctx) // все пары назначенных партиций, в том числе без регистраций
if err != nil { return err }
// Caller: durable business result и общий SQL Prefix, защищённые tokens MAT-017.
if err := persistDurably(a.Context(), u); err != nil { return err }
if err := a.Confirm(ctx, u.Position); err != nil { return err }
// После a.Done(): прекратить работу; ждать g.NextAssignment(ctx, a).
```

Пример показывает границы API; SQL-функции в нём реализует MAT-010. Локальный
`Assignment.ID()` — номер внутри одного объекта Group, **не SQL fencing token**.
`Starts()` возвращает копию начальных next offsets; это не текущий checkpoint.
`NextAssignment(ctx, after)` возвращает новое готовое назначение, отличное от after.
Каждый caller обязан наблюдать Done/Err во время Latest, обработки и SQL-вызовов.
Отмена одного ожидания Next/NextAssignment не уничтожает назначение.

## Подготовка, отзыв и потеря

Перед началом чтения `PrepareSource` получает точный набор партиций из group protocol.
Возвращённая map передаётся адаптеру во владение и должна содержать ровно эти
партиции с source-bound Position: Source должен совпасть с Config.Source,
Topic/Partition — с назначением, Offset — неотрицательный абсолютный next offset. Перед выдачей назначения
проверяется диапазон `[log start, LSO]`. Удалённая/будущая позиция — ErrPosition;
reset на latest/start отсутствует. Broker committed offsets намеренно игнорируются,
включая повторное назначение после успешного Confirm.

Общий бюджет подготовки (metadata, callback, проверка диапазона) — 3 с.
Callback должен учитывать context, не обращаться к Group и не оставлять
фоновую работу после отмены. Адаптер не ждёт поздний callback бесконечно:
ошибка/timeout останавливает группу с ErrPrepare и никогда не включает работу.
SQL token, выданный перед последующей ошибкой транспорта, caller отзывает/заменяет
по правилам MAT-017; выдача SQL полномочий здесь не реализована.

Eager rebalance отзывает весь прежний объект Assignment. Его контекст, все
регистрации и очереди завершаются до установки следующего назначения.
OnPartitionsRevoked даёт ErrRevoked, OnPartitionsLost — ErrRevoked + ErrLost.
Старые Next/Register/Confirm не разрешают работу. При штатном rebalance Group
остаётся живой, а NextAssignment выдаёт новую capability. При транспортном или
контрактном отказе завершается весь Group; необходим новый bootstrap.

BlockRebalanceOnPoll защищает только короткий dispatch одной записи;
AllowRebalance вызывается независимо от скорости бизнес-обработчика. Callback
назначения/отзыва не выполняет бизнес-обработку. Register и Confirm ограничивают
сетевые запросы 3 с; heartbeat выполняется клиентом отдельно. Close отменяет
подготовку, завершает reader, регистрации и Kafka client, идемпотентен.

## Поток, память и подтверждение

Topic обязан иметь ровно 16 партиций; проверка выполняется при NewGroup,
назначении и не реже раза в секунду чтения (с конечным сетевым timeout).
Другое число явно завершает источник. `Config.Partitions` в GroupConfig должен
быть пустым. `PairPartition` использует keyed partitioner закреплённого producer
с фиксированным числом 16. Регистрация чужой партиции/неверного отображения
отклоняется; событие с неверным отображением завершает группу с ErrContract.

`Assignment.Next` сохраняет исходные event_id, точную цену, revision, key и
topic/partition/offset всех read_committed data records; дубли не объединяет.
Control records и aborted записи не выдаются. Межпартиционного порядка нет.
`Register` использует тот же LSO-барьер и тот же `Registration` MAT-013, поэтому
совместим с Register → Latest → merge MAT-012 без независимого reader.

Один общий лимит (по умолчанию 256, максимум 512) учитывает полную очередь,
выданные, но ещё не подтверждённые позиции и копии в очередях регистраций.
Регистрация пары расходует дополнительный слот на событие; чтение из её очереди
освобождает этот слот. Полный поток освобождает слот только после Confirm.
Переполнение завершает источник без skip/ack, включая выдачу накопленных записей.
Внутренняя служебная регистрация полного потока входит в предел 1 024 регистраций.
Payload ≤4 KiB, fetch/decompression target 64 KiB, один concurrent fetch,
broker response ≤1 MiB и остальные ограничения MAT-013 сохраняются.

Confirm — явное утверждение caller, что SQL commit уже состоялся. Принимается
только самая ранняя выданная неподтверждённая позиция своей партиции данного
назначения. Future, foreign, повторная и подтверждённая вне порядка позиция
отклоняются с ErrConfirm. Успех синхронного broker commit сохраняет next offset
`record.offset + 1`; ошибка инвалидирует capability и требует SQL-replay.
Адаптер не вычисляет полноту кандидатов/Prefix и не доказывает SQL commit.
Это обязанности caller в MAT-010; Kafka commit не заменяет SQL checkpoint.

## Проверка

`bash infra/kafka/verify-go.sh` использует существующий disposable runner и race
detector. Group fixtures создают отдельные topics внутри этого же стенда.
Проверяются две реплики/16 партиций, production mapping, rebalance/loss,
open/abort/commit и LSO, Latest с конкурентной ценой, явный commit/SQL cursor
replay, переполнение до/после выдачи, неверный контракт, недоступные/удалённые
позиции, неверное/изменённое число партиций, ошибка/timeout Prepare, отмена/Close.
Без KAFKA_INTEGRATION=1 реальные тесты явно SKIP; недоступный opt-in broker
даёт FAIL. Проверки SQL fencing и failover бизнес-обработчика сюда не входят.

Семантика callbacks/offset adjustment/gate/commits:
[config.go franz-go v1.22.2](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/config.go),
[consumer_group.go](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/consumer_group.go).

## Source binding — OPS-060

PrepareSource возвращает identity **вместе с каждой позицией** из SQL binding
либо согласованного нового bootstrap. Нулевая/чужая Source, другой Topic/Partition
отвергаются до публикации Assignment. Config.Source проверяется по broker metadata
и каждому fetch; `Assignment.SourceIdentity()` возвращает неизменяемое значение.
`Starts()` остаётся копией чисел в рамках этой identity, а Update.Position
содержит её явно. Confirm проверяет identity доставленной позиции и новую
metadata перед broker commit; ошибка metadata инвалидирует capability.
Неверная позиция отклоняется без broker commit. После обнаруженного
отказа NextAssignment не возвращает уже инвалидированное назначение.

Поле Prepare со старым `map[int32]int64` оставлено только для компиляции
нынешней replica: адаптер **не вызывает его** и без PrepareSource не запускается.
Это не совместимый обход проверки. OPS-062 должен заменить callback, передать
SQL binding OPS-061, мигрировать fixtures/process recovery и удалить это поле.
До этого существующий cmd/matcher получает отказ; продуктовый runtime в OPS-060
не подключается и его прежний process smoke не считается пройденным.
Ожидаемые IDs нельзя автоматически подставить из config в исторический offset.
