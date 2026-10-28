# WebSocket transport — MKT-009 / MKT-010 / MKT-011 / MKT-012

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
Live Binance, runtime wiring и durable приём
сюда не входят. Reconnect MKT-011 описан ниже.

## Reconnect — MKT-011

`Reconnect(ctx, endpoint, config, retry, initialCommands, handle, disconnected)`
повторно открывает соединение после транспортного сбоя (включая close 1000,
отказ handshake, ошибку записи и переполнение pong). Каждый раз используется
тот же endpoint целиком и новый канал с полной копией `initialCommands` через
планировщик MKT-010. Для URL streams достаточно `initialCommands == nil`.
Для команд SUBSCRIBE передайте готовые JSON с прежним набором потоков.
Bytes копируются один раз на входе; во время копирования вход менять нельзя.
Динамическое изменение плана потоков — отдельный MKT-016. Команды/ACK остаются
raw JSON; успешный dial или отправка команды ещё не подтверждают подписку.

`DefaultRetryConfig()` задаёт InitialBackoff=1 с, MaxBackoff=30 с.
Требуется 0 < InitialBackoff <= MaxBackoff. Верхняя граница удваивается после
каждого сбоя до MaxBackoff без переполнения. Независимый equal jitter выбирает
задержку от max(1 нс, backoff/2) до backoff включительно: по умолчанию 0,5–1 с,
1–2 с, 2–4 с, …, 15–30 с. Backoff не сбрасывается при успешном handshake или
получении сообщения: быстро обрывающийся сервер не вызывает частые повторы.
Количество попыток не ограничено; их завершает context. Ожидание отменяется,
перед каждой новой попыткой context проверяется снова; немедленные ошибки dial
тоже обязательно проходят через задержку.

Обязательный `disconnected(err, delay)` вызывается синхронно после каждого сбоя,
после закрытия старого соединения/writer и до начала задержки. Исходная причина
доступна через errors.Is/As. Callback позволяет сообщить о возможном разрыве и
выбранной задержке; назначение generation/live/gap и сохранение предупреждения
остаётся вызывающему коду. Reconnect не доказывает непрерывность данных и не
восстанавливает сделки из внешней истории. Observer должен быстро возвращаться
или учитывать внешний context; транспорт не прерывает заблокированный callback.
Ошибка `handle` завершает Reconnect с исходной обёрнутой причиной без повторов и
без callback разрыва. Невалидный config или nil callbacks отвергаются до dial.
Ошибки транспорта/протокола повторяются до отмены: observer должен обеспечивать
их видимость. Для неверного endpoint оператор должен исправить конфигурацию.

Отмена контекста завершает установленное чтение и backoff, новые попытки
не начинаются. Ограничение отмены HTTP handshake из MKT-009 сохраняется.
Локальный WS server проверяет EOF → задержка → прежние URL и два потока SUBSCRIBE,
ошибку handshake → восстановление, доставку цены, остановку и terminal-ошибку
обработчика. Fake clock проверяет обе границы jitter, экспоненту и потолок,
переполнение duration, отсутствие попыток до срока и отмену ожидания.
Runtime wiring, ACK tracking и backfill здесь не реализованы. Ротация — ниже.


## Плановая ротация — MKT-012

`Reconnect` заменяет установленное соединение через `RotationAge = 23*time.Hour`
после успешного handshake (час запаса до внешнего ограничения 24 часа), а также
при Binance `serverShutdown`: raw `{"e":"serverShutdown"}` или combined
`{"stream":"!serverShutdown","data":{"e":"serverShutdown"}}`.
Ключ `e` распознаётся с учётом регистра; `E` не является типом события.
Эти сообщения поглощаются внутри reconnect, остальные bytes без изменений
доходят до handler. `Read`/`ReadWithCommands` остаются одноразовым транспортом.
Внешний контракт повторно сверен 27 октября 2026 по
[официальному Binance WS](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#server-shutdown).

Причина ротации доступна observer через `errors.Is(err, stream.ErrRotation)`.
Она проходит существующий bounded backoff/jitter и replay приватной копии
initialCommands с неизменным URL. Отдельного пути dial или параллельного соединения
нет. Совпавшие shutdown и возраст заканчивают одну попытку и вызывают одну замену;
таймер останавливается, его goroutine ожидается до возврата из попытки. У нового
соединения собственный таймер; старый deadline на него не действует. Context
приложения завершает весь цикл без следующего dial. Handler должен учитывать
переданный context также при отмене из-за возраста соединения.

Ротация сообщает возможный gap и не подтверждает восстановление истории/live.
Вызывающий код сохраняет committed previous для `trade.CheckNext` между
соединениями; транспорт не владеет границей ID. WS-тесты проверяют ID 10 до
ротации, повтор 10 и новый 11 после неё: результат фильтра — 10,11.
Durable фиксация и runtime wiring остаются MKT-017 и последующим задачам.
Fake clock и локальный WS проверяют возраст, raw/combined shutdown, оба сигнала
одновременно, один replay двух streams, отсутствие раннего срабатывания и cleanup.

## План каталога → соединения — MKT-016

`NewReconciler(lifetimeCtx, ReconcileConfig, handle, disconnected)` создаёт
одного владельца feed. `Reconcile(callCtx, catalog.Snapshot)` принимает только
успешный полный снимок каталога и вызывает `streamplan.Build`; пользовательские
подписки не участвуют. Пустой успешный снимок удаляет все потоки и соединения;
ошибку Fetch нельзя заменять пустым снимком.

```go
r, err := stream.NewReconciler(ctx, stream.ReconcileConfig{
    Endpoint: "wss://stream.binance.com:9443/ws",
    Limit: streamplan.MaxStreamsPerConnection,
    Transport: stream.DefaultConfig(),
    Retry: stream.DefaultRetryConfig(),
    AckTimeout: 10 * time.Second,
}, handle, func(group int, err error, delay time.Duration) {
    // Сообщить о возможном gap; сохранять причины и учитывать ctx.
})
if err != nil { return err }
defer r.Close()
return r.Reconcile(ctx, successfulCatalog)
```

Endpoint обязан иметь путь `/ws` без URL-подписок; разрешён только query
`timeUnit=MICROSECOND` либо `microsecond`. Прочая конфигурация, callbacks и лимит
проверяются до dial. Настройки лимитера и retry совпадают с Read/Reconnect.
AckTimeout положительный, включает очередь лимитера и ответ сервера; пример
10 секунд — локальный бюджет, не внешний лимит Binance. Слишком маленькое
значение вызывает видимые повторы. Одновременно ожидается одна команда на группу.

Группа определяется индексом Build. Сначала **во всех** существующих группах
применяется UNSUBSCRIBE для удаляемых/переносимых потоков, затем SUBSCRIBE для
добавленных. Переход к добавлениям ждёт подтверждения всех удалений. Временные
пропуски возможны; пара не подписывается одновременно на двух группах владельца.
Удалённые пустые группы останавливаются с ожиданием cleanup. Повтор того же плана
не отправляет команды и не открывает новые соединения при исправном транспорте.
Индексы групп не являются постоянным shard ID.

Команда имеет числовой ID, возрастающий внутри соединения. Только JSON с точным
`id` и `result:null`, без `code`, подтверждает применение. ACK и ошибки команд
поглощаются и не передаются trade handler. Неверный/пропущенный ACK, отказ команды,
таймаут и транспортный сбой закрывают текущую попытку. По готовому
`retryConnections` (тот же backoff/jitter, что Reconnect) новая попытка подписывает
актуальный набор группы заново. Успешная отправка и dial не считаются ACK;
подтверждения старого соединения не действуют на новом. Ротация использует
существующие watchRotation/RotationAge/serverShutdown, общий writer/лимитер
ReadWithCommands и тот же retry-путь. `disconnected(group, err, delay)` вызывается
после cleanup попытки, сообщает также ошибки ACK и возможный gap.

Вызовы Reconcile сериализованы. Отмена callCtx прекращает ожидание, оставляя
безопасный промежуточный набор; повторный Reconcile доводит его до последнего
переданного снимка. Lifetime context/Close останавливает все группы; Close ждёт
сокеты, writer, command worker и retry loop. Ограничение отмены handshake до
10 секунд сохраняется. Ошибка прикладного handler завершает всего владельца
с исходной причиной; она не повторяется как сетевой сбой. Handle и observer могут
вызываться параллельно разными группами: нужны concurrency-safe callbacks,
возвращающиеся быстро или учитывающие lifetime/переданный context. Из callback
нельзя синхронно вызывать Reconcile/Close этого владельца: они ждут callback.

ACK подтверждает подписку, но не live, непрерывность сделок или durable приём.
Состояние `trade.CheckNext` должно сохраняться по паре через переносы и reconnect.
Runtime wiring, периодическое обновление каталога, backfill, общий IP-лимит dial
и многорепличное владение feed сюда не входят. Реальный Binance не вызывался.
Формат SUBSCRIBE/UNSUBSCRIBE и `result:null` повторно сверен 28 октября 2026 по
[официальной спецификации Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#live-subscribingunsubscribing-to-streams).

WS-тесты `TestReconcile*` проверяют no-op, дубли каталога, точную разницу команд,
перенос между группами без двойной подписки, пустой каталог, отказ/потерю/таймаут
ACK, неверный ID/result, shutdown, replay актуального набора после потерянного ACK
удаления, отмену/продолжение, terminal handler и Close с ожидающим ACK.
