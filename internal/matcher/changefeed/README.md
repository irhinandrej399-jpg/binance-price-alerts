# Kafka-источник изменений подписок (MAT-015)

`changefeed` читает `subscription-changes` через закреплённый franz-go v1.22.2.
Это вход для MAT-008; SQL-replay, durable checkpoint, readiness, consumer groups
и runtime подключаются отдельно.

```go
source, err := changefeed.New(ownerCtx, changefeed.Config{
    Brokers: brokers, Topic: prefix + "-subscription-changes",
})
if err != nil { return err }
defer source.Close()
reg, err := source.Register(bootstrapCtx)
if err != nil { return err }
// Только теперь BeginReplay может зафиксировать H.
// Во время SQL-replay и live обязательно наблюдать reg.Done()/reg.Err().
for {
    tx, err := reg.Next(waitCtx)
    if err != nil { return err }
    // Сверить tx.First/tx.C с durable C; пропуск закрыть SQL-replay.
    // Полный пакет и tx.NextOffset сохранить одной SQL-транзакцией.
    if err := persist(tx); err != nil { return err }
    if err := reg.Confirm(tx.NextOffset); err != nil { return err }
}
```

## Регистрация и целые пакеты

На Source разрешена одна успешная регистрация. Register проверяет metadata:
ровно одна партиция 0; получает её read_committed last stable offset (LSO),
устанавливает абсолютный direct cursor и запускает reader до возврата.
`Start()` — неизменяемая включённая позиция `(topic, 0, offset)`. Публикация,
завершившаяся раньше LSO, покрывается последующим SQL-replay; открытая на этой
границе транзакция будет прочитана после commit. High watermark вместо LSO
использовать нельзя. Потеря регистрации требует нового Source/bootstrap,
не тихого reset на latest.

Выдаются только committed записи и целые пакеты SUB-007: одна API-транзакция
на одну Kafka-транзакцию. Включено KeepControlRecords; данные проверяются до
помещения в очередь, пакет выдаётся после commit marker. Проверяются одинаковые
producer ID/epoch записей, последовательные position, общий transaction_end,
достижение C и отсутствие второй API-транзакции до marker. У commit marker
epoch может совпадать с записями либо быть на единицу выше: последнее необходимо
для [Kafka KIP-890](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/235834631/KIP-890+Transactions+Server-Side+Defense).
Abort markers без видимых записей игнорируются; open/aborted payload не выдаётся.
Повреждённая граница инвалидирует всю регистрацию, частичный пакет не выходит.

Transaction содержит `First`, `C`, `Records` и `NextOffset` **после commit marker**.
Record сохраняет собственную Position, исходные JSON и Key, Envelope/event_id и
полный типизированный Change. C — uint64 журнала, он не вычисляется из Kafka offset.
Целые дубли с прежними C/event_id выдаются со своими новыми Kafka-позициями;
дедупликация принадлежит durable координатору. Пропуск между пакетами также
виден caller по First/C: источник не заполняет его и не объявляет актуальность.
Начальный First сравнивается с SQL-прогрессом; один position/end сам по себе не
доказывает отсутствие пропущенного префикса до границы регистрации.

`Next`/`TryNext` выдают не более одного неподтверждённого пакета. Confirm принимает
только его точный NextOffset, после durable сохранения caller, и освобождает
учтённые слоты. `Confirmed()` — локальная транспортная отметка, исходно Start.
Клиент не присоединяется к группе, не имеет auto-commit и не отправляет broker
OffsetCommit. Авторитетный checkpoint хранится в SQL; группы/их commits и
fencing — отдельные MAT-010/011. Confirm не является SQL-ack replay-pin.

## Лимиты и жизненный цикл

- Payload ≤4 KiB, key ≤256 bytes; default/max Buffer — 1 024 records. Предел
  общий для сборки, очереди и выданного неподтверждённого пакета: ≤4 MiB wire
  payload плюс ключи/структуры. В очереди не удерживается декодированная копия
  полного after-image; она создаётся только для одной выдачи. После Confirm
  caller освобождает свои ссылки на пакет. Меньший Buffer пригоден для fixtures.
- Fetch/partition target и decompressed batch ≤4 MiB, broker response ≤8 MiB,
  один concurrent fetch, PollRecords по одной записи. Это ограниченный профиль
  прикладных буферов DEC-008; RSS, суммарная память клиента/процесса и нагрузочная
  вместимость не квалифицированы. Ограничение oversized batch/response не
  позволяет бесконечному compressed payload обойти лимит чтения.
- SUB-007 не ограничивает размер SQL-пакета и не режет его при публикации.
  Пакет сверх этого Kafka-профиля вызывает видимый ErrOverflow/ErrContinuity,
  без выдачи/подтверждения его части. MAT-008 закрывает хвост paginated SQL-replay;
  новый Kafka-вход регистрируется до нового H. Admission/квалификация — OPS-015.
- Ошибки envelope/schema, ключа, границы, позиции, чтения и переполнение
  инвалидируют регистрацию, освобождают очередь и закрывают Done. Ошибка имеет
  приоритет над накопленной работой; ошибки не содержат исходных payload/адресов.
- Проверка LSO при пустом poll раз в секунду обнаруживает отказ даже без новых
  событий; позиционные запросы ограничены 3 с. NoResetOffset запрещает reset
  при OffsetOutOfRange. Отмена ownerCtx/контекста Register инвалидирует вход.
  Отмена контекста Next прерывает только ожидание; для отказа от регистрации
  вызвать Close. Registration.Close освобождает вход; Source.Close закрывает
  клиент и ждёт reader, повторный вызов безопасен.
- Внешний владелец обязан вызвать Source.Revoke до утраты/передачи назначения.
  Адаптер не реализует rebalance и не доказывает право записи в SQL.

## Воспроизведение

```sh
GOCACHE=/tmp/binance-go-build bash infra/kafka/verify-go.sh
```

Существующий SET-009 runner запускает race-тесты на disposable Kafka и отдельном
`KAFKA_CHANGEFEED_TEST_PREFIX` topic, изолированном от relay. Проверяются LSO/open
на регистрации, конкурентная публикация, commit/abort, целые пакеты/дубли/пропуск,
исходные байты/uint64/позиции, недопустимые контракты/границы, лимиты и 1 024×4 KiB,
отмена/отзыв, пустой поток при потере брокера через готовый fault proxy,
неверное число партиций и реальный OffsetOutOfRange без reset. Отдельный запуск
с закрытым opt-in broker обязан FAIL; runner проверяет этот ожидаемый отказ.
EXIT удаляет стенд/volume. Без KAFKA_INTEGRATION=1 реальные проверки явно SKIP;
при opt-in отсутствие параметров и недоступность брокера — FAIL.
