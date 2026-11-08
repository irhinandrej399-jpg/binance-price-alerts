# Управляемый Binance fixture (E2E-003 / E2E-008)

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
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./internal/testutil/binancefake
```

Проверяются каталог, две точные цены и максимальные ID, независимые T/E,
ping/pong, TCP drop, reconnect с повтором fixture, close 1001,
отмена Next/timeout Ping, неизвестные пути и cleanup незабранных sockets.
Это Go helper для стенда с настоящими процессами; отдельный CLI/control HTTP,
combined-stream wrappers, rate limits, fake Telegram, полный E2E четырёх
процессов и нагрузочный генератор в этот результат не входят.

## Динамический каталог и bare /ws (E2E-008)

Для reconciler используйте `f.WSURL()` (bare `/ws`). `f.SetCatalog([]catalog.Pair{...})`
атомарно заменяет REST-ответ: каждая переданная пара получает TRADING и spot=true,
настоящий catalog.Client продолжает фильтрацию по USDT. Срез копируется в JSON;
`SetCatalog(nil)` задаёт пустой каталог. Исходный fixture и raw endpoints сохраняются
до явной замены. После Close SetCatalog возвращает ErrClosed.

На bare-соединении `conn.Symbol == ""`. Fake принимает SUBSCRIBE/UNSUBSCRIBE
с непустыми params имён `lowercase@trade`, отвечает `{"result":null,"id":...}`
с исходным JSON ID без float64. Повторная подписка/отписка идемпотентна;
некорректная или неподдерживаемая команда закрывает соединение. Это узкий fixture,
не полная эмуляция Binance protocol/errors/rate limits.

`conn.Streams()` возвращает независимый отсортированный снимок набора этого
соединения. Дожидайтесь успешного Reconcile/ACK перед проверкой набора или сделкой.
`conn.TradePair(ctx, "ETHUSDT", trade)` отправляет raw trade только в подписанную
пару; иначе возвращает ErrNotSubscribed без записи. На raw-соединениях прежний
`Trade` сохранён. Записи, ACK и изменение набора сериализуются; Streams безопасен
при параллельных командах, SetCatalog — при параллельном Fetch.

Замена каталога сама не меняет подписки: родитель запускает refresh/reconcile
в приложении и наблюдает результат. После Disconnect новый Next возвращает
новое соединение с пустым набором до SUBSCRIBE. Автоматических цен, replay,
поколений или подтверждения live нет. Ping/Close/отмена и cleanup работают для
обоих видов соединений, включая bare socket без вызова Next.

Тесты через catalog.Client, stream.Reconciler и trade.Decode проверяют две
точные различные цены, переходы 2 → 3 → 2 → 0 пар, подавление сделки удалённой
пары, reconnect, exact ACK большого ID, конкурентный каталог/Streams и cleanup.
SQL/outbox и процессный smoke остаются в MKT-019.

## Собственный listener и адрес клиента (E2E-009)

`New()` сохраняет прежний loopback-only запуск. Для изолированного сетевого
сценария доступен `NewWithListener(listener net.Listener, origin string) (*Fake, error)`.
Handlers, ручные действия и журнал те же; отдельного control API нет.

```go
listener, err := net.Listen("tcp", bindAddress) // bind задаёт родитель сценария
if err != nil { return err }
f, err := binancefake.NewWithListener(listener, clientOrigin)
if err != nil { return err } // listener уже закрыт конструктором
defer f.Close()
// clientOrigin, например http://fixture.test:18080, должен быть достижим клиенту.
// f.URL() возвращает ровно clientOrigin, а не listener.Addr().
```

Владение ненулевым listener передаётся **при входе**, в том числе при ошибке
валидации. Родитель передаёт уже открытый исправный listener и после передачи
сам его не закрывает и не использует. Ошибки bind/listen возвращает `net.Listen`
до создания fake; nil listener или неверный origin дают ошибку конструктора
без запуска goroutine, ненулевой listener закрывается. Конструктор запускает
обычный HTTP Serve асинхронно, не выполняет сетевую проверку готовности или
достижимости origin; это обязанность сценария. Закрытый или неисправный listener
передавать нельзя. Close закрывает listener и активные HTTP-соединения,
освобождает ожидающие запросы и присоединяет handlers; повторный Close безопасен.

Origin — только HTTP origin с явным портом 1–65535, без userinfo/path/query/fragment;
IP wildcard `0.0.0.0`, `::` и `*` отклоняются. DNS-имя допустимо: разрешение
имени и маршрут обеспечивает родитель. Bind и origin могут отличаться:
wildcard bind не является URL для клиента. Если хостовый родитель использует
отдельный маршрут, он обращается по собственному достижимому адресу; переданный
origin предназначен выбранной группе клиентов.

Для OPS-074 родитель должен выбрать bind интерфейс/порт и отдельный адрес,
доступный из Compose-сети, настроить DNS/маршрутизацию при необходимости и передать
origin процессу. Подойдут адрес интерфейса тестового bridge или явный сетевой
alias с настроенным маршрутом; ни `host.docker.internal`, ни host networking
не являются обязательным контрактом. Реальный маршрут из контейнера и cleanup
проверяются в OPS-074. Здесь тесты используют переданный loopback listener;
подмена его Addr на wildcard проверяет независимость URL без открытия LAN-порта.

Non-loopback bind разрешён только явным тестовым сценарием в изолированной сети.
Не публикуйте эти handlers наружу и не передавайте реальные credentials.
`WSURL()` и `StreamURL()` строятся из переданного origin. Close также закрывает
все hijacked WS, включая незабранные Next, и присоединяет их handlers.
Каталог, ACK, TradePair и Next остаются управляемыми сценарием.
