# Управляемый Binance fixture (E2E-003)

`binancefake.New()` открывает отдельный случайный loopback HTTP/WS порт.
Родитель E2E владеет fake, а дочернему market-data передаёт `f.URL()` для
REST-каталога и `f.StreamURL("BTCUSDT")` для raw trade. Обязательно `defer f.Close()`:
Close закрывает listener и все hijacked sockets, включая ещё не полученные
через Next, и присоединяет handlers. Docker и публичный Binance не нужны.

```go
f := binancefake.New()
defer f.Close()
// Запустить сервис с REST origin f.URL() и WS endpoint f.StreamURL("BTCUSDT").
conn, err := f.Next(ctx) // ctx с deadline; дожидается handshake
// Проверить err и conn.Symbol.
err = conn.Trade(ctx, binancefake.Trade{
    ID: 12345, Price: "100.000000000000000001",
    TradeTime: 1672515782001, EventTime: 1672515782136,
})
// Дождаться наблюдаемого результата сервиса перед следующим действием.
err = conn.Ping(ctx, "probe") // ждёт pong с тем же payload
err = conn.Disconnect()      // TCP drop, без close frame
// При reconnect получить новый conn через f.Next(ctx).
```

`GET /api/v3/exchangeInfo` возвращает встроенный `testdata/exchange_info.json`:
доступны BTCUSDT/SOLUSDT; ETHBTC, BREAK ETHUSDT и non-spot BNBUSDT служат
проверке фильтрации готового catalog.Client. Raw endpoints:
`/ws/btcusdt@trade`, `/ws/solusdt@trade`. Неизвестный путь даёт 404,
не-GET — 405. Каждое соединение независимо; Next возвращает его Symbol.
Изменять Symbol нельзя. При нескольких одновременных handshake порядок Next
не определён — сопоставляйте Symbol, а не порядок соединений.

Trade отправляет raw `e/s/t/p/T/E`; ID — uint64 без float64, цена — исходная
строка, T/E — заданные Unix milliseconds. Никаких автоматических сделок,
ID, времён или replay после reconnect нет: сценарий явно повторяет нужный
Trade. Fake не назначает durable revision и не имитирует её вместо market-data.
Проверки корректности цены остаются у настоящего декодера; fake позволяет
задать некорректную цену для отказного сценария.

Ping запускается сценарием, без ожидания 20 секунд; это проверка frame/payload,
а не симуляция времени Binance. Нужен читающий клиент. Используйте deadline;
после timeout закрывайте этот socket, чтобы поздний pong не попал в следующую
проверку с таким же payload. `conn.Close(ctx, 1001, "rotate")` задаёт graceful
close code/reason, `Disconnect()` — abnormal close. Ожидания Next/Ping отменяются
context; каждая запись ограничена меньшим из deadline и 1 секунды. Записи
сериализуются; сценарий вызывает действия последовательно. Ping payload
ограничен 125 байтами, close code/reason должны быть допустимыми для WebSocket.

Проверка через настоящие catalog.Client, stream.Read и trade.Decode:

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race -count=10 ./internal/testutil/binancefake
```

Проверяются каталог, две точные цены и максимальные ID, независимые T/E,
ping/pong, TCP drop, reconnect с повтором fixture, close 1001,
отмена Next/timeout Ping, неизвестные пути и cleanup незабранных sockets.
Это Go helper для стенда с настоящими процессами; отдельный CLI/control HTTP,
combined streams/SUBSCRIBE, rate limits, fake Telegram, полный E2E четырёх
процессов и нагрузочный генератор в этот результат не входят.
