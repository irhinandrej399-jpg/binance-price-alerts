# Одно WebSocket-соединение — MKT-009

`stream.Read(ctx, endpoint, handle)` открывает один WS/WSS URL, читает полные
текстовые сообщения последовательно и передаёт исходные bytes в `handle(ctx, data)`.
Для выбранного MKT-002 raw trade URL:
`wss://stream.binance.com:9443/ws/btcusdt@trade` (symbol в нижнем регистре).
Подписка задаётся URL; JSON-команды SUBSCRIBE не отправляются.

Обработчик может вызвать `trade.Decode(data, metadata)`. Он сам задаёт исходное
время приёма, единицы времени и метаданные готового декодера. Транспорт JSON
не разбирает, combined wrapper не извлекает, ревизии и live/gap не назначает.
Raw URL использует миллисекунды по умолчанию; при `timeUnit=MICROSECOND` вызывающий
код обязан передать декодеру `priceobserved.Microseconds`.

Ping обрабатывается в цикле чтения: один pong с тем же payload, срок записи
control frame — 1 секунда. Собственные ping и unsolicited pong не отправляются.
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
Отдельных goroutine для чтения или обработчика нет; context.AfterFunc закрывает
сокет при отмене и снимается при выходе. Во время HTTP handshake отмена без
заданного deadline может ожидать его таймаута; отмена установленного чтения
проверяется отдельно.

Зависимость: `github.com/gorilla/websocket v1.5.3`. Контракты сверены 27 октября
2026 по [Binance WS streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)
и [Gorilla WebSocket](https://pkg.go.dev/github.com/gorilla/websocket@v1.5.3).
Локальные httptest WS-серверы проверяют fixture MKT-002 → decoder, pong с NUL и
произвольными bytes, нормальное и аварийное закрытие, ошибку handshake/обработчика,
отмену/deadline во время чтения и освобождение соединения, binary и размер.

Проверка: `GOTOOLCHAIN=local go test -race ./internal/marketdata/stream`.
Live Binance, runtime wiring, лимитер команд (MKT-010), reconnect (MKT-011),
ротация (MKT-012) и durable приём сюда не входят.
