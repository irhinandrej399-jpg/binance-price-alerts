# Одно WebSocket-соединение — MKT-009 / MKT-010

`stream.Read(ctx, endpoint, handle)` открывает один WS/WSS URL, читает полные
текстовые сообщения последовательно и передаёт исходные bytes в `handle(ctx, data)`.
Для выбранного MKT-002 raw trade URL:
`wss://stream.binance.com:9443/ws/btcusdt@trade` (symbol в нижнем регистре).
В `Read` подписка задаётся URL. Для динамических команд доступен
`ReadWithCommands(ctx, endpoint, config, commands, handle)`: `commands` — канал
готовых JSON `[]byte`, отправляемых как text frames. Например:

```go
commands := make(chan []byte, 1)
commands <- []byte(`{"method":"SUBSCRIBE","params":["ethusdt@trade"],"id":1}`)
close(commands)
err := stream.ReadWithCommands(ctx, endpoint, stream.DefaultConfig(), commands, handle)
```

Содержимое JSON, ID, соответствие ACK и изменение набора streams — ответственность
вызывающего кода. Ответы команд передаются в тот же `handle` как raw text;
их нужно отличать от trade перед вызовом декодера. Nil/закрытый канал отключает
команды; цены и pong продолжают обрабатываться. Не изменять bytes после передачи
в канал. Размер/буфер канала команд выбирает вызывающий код; транспорт держит
не более одной ожидающей команды.

Обработчик может вызвать `trade.Decode(data, metadata)`. Он сам задаёт исходное
время приёма, единицы времени и метаданные готового декодера. Транспорт JSON
не разбирает, combined wrapper не извлекает, ревизии и live/gap не назначает.
Raw URL использует миллисекунды по умолчанию; при `timeUnit=MICROSECOND` вызывающий
код обязан передать декодеру `priceobserved.Microseconds`.

Ping в цикле чтения помещает pong с исходным payload в ограниченную очередь
без ожидания лимитера или сети. Отдельная writer goroutine отправляет команды и
pong через общий бюджет. Уже ожидающий pong получает следующий слот раньше
команды; pong сохраняют порядок. Собственные ping и unsolicited pong не отправляются.
При переполнении очереди возвращается `ErrControlQueueFull` и соединение закрывается:
обязательный pong не теряется молча, чтение не ждёт освобождения очереди.

`DefaultConfig()` задаёт `Messages: 5`, `Window: time.Second`,
`WriteTimeout: time.Second`, `ControlQueue: 16`. Интервал между завершением
отправки и началом следующей — ceil(Window/Messages): по умолчанию минимум 200 мс.
Первая отправка немедленная; после простоя/задержки бюджет не накапливается.
В любом полуоткрытом окне Window отправляется не более Messages сообщений.
Это общий бюджет JSON и pong, не ограничение получаемых ценовых событий.
Валидация до dial: Messages 1..5, Window >= 1 с, положительные WriteTimeout и
ControlQueue. Более медленный профиль разрешён; его нужно выбирать так, чтобы
задержка pong вместе с очередью и записью укладывалась в минуту Binance.
WriteTimeout ограничивает отдельную сетевую запись; ожидание слота прерывается context.
Ошибка writer сохраняется через `%w`, закрывает сокет и прерывает чтение.

Handshake ограничен 10 секундами (либо более ранним deadline context).
Полное сообщение ограничено 1 MiB; binary message возвращает `ErrMessageType`.
Это транспортные границы, не лимиты бизнес-данных или admission control.

Ошибки dial/read/handle возвращаются с `%w`; `errors.Is/As` сохраняют причину.
Даже нормальное закрытие peer возвращает `*websocket.CloseError` с кодом и текстом;
обрыв TCP даёт abnormal closure. `websocket.IsCloseError` не снимает нашу обёртку:
использовать `errors.As`. Каждое завершение закрывает сокет, reconnect отсутствует.
Отмена/deadline context закрывают установленное соединение и прерывают сетевое
чтение с `context.Canceled`/`context.DeadlineExceeded`. При гонке отмены и ошибки
чтения приоритет у context. Уже отменённый context не начинает dial.

Обработчик вызывается синхронно, обеспечивает backpressure и должен быстро
возвращаться либо учитывать переданный context. Заблокированный обработчик
задерживает обработку ping и не может быть принудительно остановлен транспортом.
Отдельных goroutine для чтения или обработчика нет; writer работает отдельно и
ожидается при любом выходе. context.AfterFunc закрывает сокет при отмене и
снимается при выходе. Во время HTTP handshake отмена без заданного deadline может
ожидать его таймаута; отмена установленного чтения проверяется отдельно.

Зависимость: `github.com/gorilla/websocket v1.5.3`. Контракты сверены 27 октября
2026 по [Binance WS streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)
и [Gorilla WebSocket](https://pkg.go.dev/github.com/gorilla/websocket@v1.5.3).
Локальные httptest WS-серверы проверяют fixture MKT-002 → decoder, pong с NUL и
произвольными bytes, нормальное и аварийное закрытие, ошибку handshake/обработчика,
отмену/deadline во время чтения и освобождение соединения, binary и размер.

MKT-010 проверяет fake clock: всплеск SUBSCRIBE, общий бюджет с pong, приоритет,
FIFO, закрытие канала команд, округление интервала, отсутствие catch-up, отмену и
ошибку записи. Локальный WS server проверяет чтение цены во время ожидания pong,
приоритет pong перед второй командой, переполнение очереди и отмену с ожидающей командой.
Лимиты повторно сверены 27 октября 2026 по официальной Binance-документации выше.

Проверка: `GOTOOLCHAIN=local go test -race ./internal/marketdata/stream`.
Live Binance, runtime wiring, reconnect (MKT-011), ротация (MKT-012) и durable приём
сюда не входят.
