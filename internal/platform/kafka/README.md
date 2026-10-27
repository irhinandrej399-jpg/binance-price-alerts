# Транзакционный producer Kafka (SET-009)

Клиент закреплён: `github.com/twmb/franz-go v1.22.2`, `kmsg v1.15.0`.
Pure Go, без librdkafka/CGO. Выбор основан на поддержке транзакций,
явной инициализации producer ID/epoch и ошибках fencing/EndTxn.

## Интерфейс для MKT-013 и других relay

```go
ctx, cancel := context.WithTimeout(parent, 10*time.Second)
defer cancel()
p, err := kafka.New(ctx, kafka.Config{
    Brokers: []string{"127.0.0.1:9092"},
    TransactionalID: "local-market-data-relay-shard-0",
})
if err != nil { return err }
defer p.Close()
// Record.Value уже сериализован вызывающим кодом. ID внутри payload устойчив.
err = p.Publish(ctx, kafka.Record{Topic: topic, Key: pairKey, Value: payload})
```

`New` вызывает `ProducerID(ctx)`: получает новый epoch и отсекает прежнего
владельца **до** публикации. `transactional.id` обязателен и стабилен по
среде/владельцу/relay-шарду; PID, UUID запуска и event_id для него не подходят.
Независимым шардам нужны разные ID. При init-ошибке клиент закрывается.
Producer используется последовательно; `Publish` и `Close` не конкурентны.
Непустой batch фиксируется одной Kafka-транзакцией. Пустой batch отклоняется,
чтобы отсутствие фактической транзакции не выдать за commit сообщения.

Настройки: `RequiredAcks(AllISRAcks())`, идемпотентность включена по умолчанию,
`RecordRetries(math.MaxInt)`, `RequestRetries(20)`. Franz-go с идемпотентностью
сам ограничивает in-flight до 5 (до 1 на старых Kafka); его опция
`MaxProduceRequestsInflightPerBroker` предназначена для отключённой идемпотентности.
Время ограничено обязательным deadline контекста New/Publish, delivery timeout
10 с, request retry timeout 10 с, dial 3 с, produce request 5 с + overhead 2 с,
transaction timeout брокера 30 с. Между попытками relay должен выполнять свой
bounded backoff. Helper не содержит фонового прикладного retry loop.
`Close` освобождает соединения и буферы; он не является подтверждением commit
и не обещает abort уже обработанной транзакции. Неразрешённую транзакцию
разрешает следующий `New` с тем же ID либо transaction timeout.

Результаты:

- Только `nil` от `Publish` подтверждает commit и разрешает будущую SQL-отметку.
- `ErrFenced` означает потерю владения: прекратить старый relay. Нельзя автоматически
  перехватывать владение обратно в цикле; новый producer создаёт назначенный владелец.
- `ErrCommitUnknown` — ошибка после начала попытки EndTxn; консервативная
  классификация, **не доказательство abort**. Сохранить outbox и исходный event_id.
- `ErrPublish` — ошибка до попытки EndTxn/при инициализации; подтверждения commit нет.
- После любой ошибки транзакции producer становится `ErrRetired`: закрыть,
  восстановить владение, вызвать `New` с прежним ID и повторить исходные байты/ключ.
- Deadline/cancel при отказе доступны через `errors.Is`; сетевые адреса,
  broker messages и credentials в возвращаемые ошибки и логи helper не попадают.

Между PostgreSQL и Kafka **нет exactly-once**: потеря ответа после commit или
падение до SQL-отметки создаёт допустимый дубль с исходной идентичностью.
Дедупликация потребителей и SQL-outbox относятся к прикладным тикетам.

## Реальная проверка одной командой

Из корня, Docker должен работать, Go 1.26.3:

```sh
bash infra/kafka/verify-go.sh
```

Уникальный проект `binance-go-kafka-<run>`, временный loopback port,
отдельный volume и префикс `go-check-<run>`. Используются закреплённый образ,
ресурсы и init-topics SET-005. `EXIT`/INT/TERM удаляют **только этот** проект,
сеть и volume даже при ошибке. SIGKILL самого runner/отказ Docker не позволяют
гарантировать cleanup; имя созданного проекта видно в выводе Compose.
Готовность ограничена 180 с, Go-прогон 120 с, отдельные операции — контекстами.
`KAFKA_GO_CHECK_PORT` позволяет явно назначить свободный порт.

Прикладной producer PriceObserved — [marketdata/publish](../../marketdata/publish/README.md).
Та же команда запускает его race-сценарии на отдельном prices topic стенда.

Проверки основы на неприкладном probe: commit, abort, read_committed/read_uncommitted,
fencing активного старого epoch, потерянный EndTxn response, повтор тем же ID,
недоступный брокер. В конце явно включённый opt-in с закрытым портом обязан
провалить интеграционный тест; его `FAIL` в выводе ожидаем и проверяется runner.
Без `KAFKA_INTEGRATION=1` обычный `go test -v` явно пишет SKIP. С opt-in отсутствие
параметров или недоступность брокера — ошибка, не SKIP.

`internal/testutil/kafkafault.Proxy` подставляется через Config.Dialer.
Перехватывает framed Kafka TCP, сопоставляет correlation ID и API key 26,
декодирует ответ EndTxn выбранной версии (включая flexible header) и после Arm
скрывает все EndTxn ответы, включая повторы. Запросы доходят до реального broker;
тест требует полученный код успеха 0, `ErrCommitUnknown` у producer и видимость
первого commit независимому read_committed consumer. Затем новый producer с
тем же transactional.id повторяет исходный probe; проверяются две копии.
Это средство только для single-broker plaintext disposable-стенда; оно не
маршрутизирует несколько brokers и не расшифровывает TLS.

Исходные контракты клиента: [версия v1.22.2](https://github.com/twmb/franz-go/tree/v1.22.2),
[настройки](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/config.go),
[транзакции и EndTransaction](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/txn.go).
