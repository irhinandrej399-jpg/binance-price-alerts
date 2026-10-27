# Публикация PriceObserved — MKT-013

`New(ctx, kafka.Config, topic)` создаёт один транзакционный producer и получает
новый epoch, отсекая прежнего владельца. `TransactionalID` задаёт вызывающий
relay-шард: стабильный по среде/владельцу/шарду, не ID события или запуска.
`topic` — префиксный prices topic среды (16 фиксированных партиций локального
профиля). Все операции, включая `Close`, выполняются последовательно.

`Publish(ctx, payload)` принимает полные неизменные JSON-байты из SQL-outbox
(`storage.Claim`). Проверяет контракт `priceobserved.Decode`, выбирает пару как
Kafka key и передаёт исходные байты без повторной сериализации. Сохраняются
точная цена, event_id, source ID, stream_id/generation/sequence, времена,
состояние потока и неизвестные дополнительные поля. Невалидное событие не
публикуется. Вызывающий код не должен менять payload во время вызова.

```go
p, err := publish.New(ctx, kafka.Config{
    Brokers: []string{"127.0.0.1:9092"},
    TransactionalID: "local-market-data-relay-shard-0",
}, "local-prices")
if err != nil { return err }
defer p.Close()
return p.Publish(ctx, persistedPayload)
```

New/Publish требуют конечного deadline. Настройки и жизненный цикл наследуются
от [Kafka helper](../../platform/kafka/README.md): acks all, idempotence, retries,
не более 5 in-flight. `nil` означает подтверждённый Kafka commit одного события.
Ошибка не разрешает SQL-отметку публикации. `errors.Is` сохраняет ErrFenced,
ErrCommitUnknown, ErrRetired, ErrPublish и ошибки context из пакета kafka.
ErrFenced требует остановить прежнего владельца. После transaction error нужно
закрыть producer и восстановить владение новым producer с тем же stable ID;
повторять исходные байты. Ошибка валидации не обращается к брокеру.

Потерянный commit ack допускает дубль исходного события: атомарности SQL/Kafka
нет. Вызов не меняет SQL, не управляет lease, не буферизует работу и не выполняет
длительные повторы. Порядок вызовов внутри пары и неизменность числа партиций —
обязанности relay/runtime; runtime здесь не подключён.

Проверки:

- `GOTOOLCHAIN=local go test -v ./internal/marketdata/publish` — unit-контракт;
  без opt-in реальные Kafka-сценарии явно SKIP.
- `bash infra/kafka/verify-go.sh` — disposable broker, race-проверки helper и
  прикладного producer: commit/read_committed, fencing, отказ брокера и потеря
  EndTxn response с повтором исходных байтов новым producer. Две копии имеют
  исходные ID, ключ и партицию; стенд удаляется runner после проверки.
