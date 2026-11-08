# Управляемые Kafka faults

Только disposable single-broker plaintext Kafka. `New(target)` подставляется
в `kgo.Dialer(proxy.DialContext)` и направляет **все** advertised addresses в target.
`Close()` закрывает соединения и присоединяет relay goroutines. Production clients
и broker lifecycle не меняются. Старые `Arm()`/`Observed` сохраняют подавление
всех EndTxn responses, включая retries; `Observed` содержит настоящий broker code.

```go
proxy := kafkafault.New(broker)
defer proxy.Close()
ctx, cancel := context.WithTimeout(parent, 10*time.Second)
defer cancel()
fault, err := proxy.Intercept(ctx, 21, kafkafault.BeforeRequest)
// Проверить err; запустить DeleteRecords через client с proxy.DialContext.
event := <-fault.Events // ожидать через select с ctx.Done()
// Здесь broker ещё не получил DeleteRecords; event.Response == nil.
fault.Fail()           // закрыть соединения выбранных операций, оставить fault armed
fault.Release()        // снять fault; теперь caller может повторить исходный request
```

`Intercept(ctx, key, boundary)` допускает один активный fault:

| API key | BeforeRequest | AfterResponse |
| --- | --- | --- |
| 8 / OffsetCommit | нет | да |
| 21 / DeleteRecords | да | да |

Fault выбирает все requests этого API на всех соединениях proxy, включая повторы;
он не фильтрует group/topic/partition. Для изоляции используйте отдельный proxy
на caller. Выбор происходит при чтении request; уже отправленный до Intercept
request не перехватывается. Events содержат correlation ID и полный decoded
kmsg request/response нужной версии. В AfterResponse frame уже пришёл от настоящего
broker, но ещё не доставлен caller. Проверяйте **все нужные topic/partition error
codes**, для удаления также LowWatermark; факт наличия ответа не означает успеха.
Для OffsetCommit v10 topics идентифицируются TopicID. Объекты Event read-only.
Никаких синтетических успешных ответов proxy не создаёт.

`Fail()` идемпотентен и сохраняет fault до Release: следующие выбранные запросы
снова доходят до той же границы и завершаются сетевой ошибкой. В AfterResponse
это потеря настоящего ответа; в BeforeRequest запрос не пересылается.
`Release()` идемпотентно снимает fault и пропускает удержанные операции, если
Fail ещё не был вызван. После Fail удержанные операции не оживают: нужен повтор
caller. Освобождение не ждёт обработки broker. Канал Events (32 элемента) не
закрывается; при заполнении возникает backpressure, поэтому observer должен
читать его или отменить context/закрыть proxy. После Fail новые наблюдения
записываются, пока есть место; переполнение не задерживает отказ.

Отмена/deadline context Intercept закрывает удержанные соединения, включая
ожидание broker response, и освобождает gates. Истёкший fault не выбирает новые
запросы. Context DialContext ограничивает только dial, как net.Dialer.
Close освобождает gates и соединения даже без чтения Events. Предел frame —
16 MiB, одновременно ожидаемых responses — 128 на соединение; malformed
selected frames, неизвестный/дублирующий активный correlation ID отвергаются закрытием.
Payload/credentials в ошибки не включаются.

Граница удерживает соответствующее направление TCP до Release/Fail/отмены;
это может задержать соседние pipelined операции на том же соединении. Fail
закрывает всё затронутое соединение. Соседние API не декодируются заново и их
frames не переписываются; отдельные соединения продолжают работать.
Это не Kafka TLS/multi-broker proxy и не алгоритм безопасной очистки.

## Проверка

```sh
# SKIP реальной Kafka без opt-in; TCP protocol/lifecycle tests выполняются.
GOTOOLCHAIN=local go test -race -v ./internal/testutil/kafkafault
# Настоящая Kafka, уникальный Compose project/port/volume/topics, race и cleanup.
bash infra/kafka/verify-go.sh
```

Runner SET-009 уже включает этот пакет. OPS-068 создаёт собственный topic,
независимо читает committed group offset и log-start, удерживает/теряет реальные
OffsetCommit/DeleteRecords ответы, повторяет ту же границу и читает оставшиеся
read_committed key/value/header bytes. Общий runner также проверяет старый
EndTxn lost-response и отрицательный opt-in с закрытым портом: ожидаемый FAIL
обязан произойти. Недоступный opt-in broker не считается SKIP.
Никакой SQL progress или безопасный минимум cleaner здесь не вычисляется.
