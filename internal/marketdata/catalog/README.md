# Каталог Binance Spot — MKT-001

`New(httpClient, baseURL, now)` создаёт клиент; `Fetch(ctx)` выполняет один
публичный `GET /api/v3/exchangeInfo` без credentials. Например, origin:
`https://data-api.binance.vision`. HTTP-клиент и часы обязательны; вызывающий
код задаёт timeout HTTP-клиента либо deadline контекста. Origin допускает
HTTP для локальных fixture-серверов, путь/query/credentials не допускаются.

Результат `Snapshot` содержит `Pairs` (`Symbol`, `BaseAsset`, `QuoteAsset`)
в порядке ответа Binance и `UpdatedAt`: местное UTC-время после успешного
чтения, декодирования и фильтрации всего ответа. Это время обновления каталога,
не `serverTime` Binance и не время сделки. Пустой массив symbols — успешный
пустой каталог с timestamp.

Отбор: `quoteAsset == USDT`, `status == TRADING` и
`isSpotTradingAllowed == true`. Символ используется дословно, а не собирается
из названий активов или проверяется по суффиксу. Старое поле `permissions`
не требуется. Неизвестные поля ответа игнорируются.

Ошибка HTTP, API error envelope, ошибка транспорта/чтения/JSON, отсутствие
массива symbols или пустой symbol/baseAsset у подходящей пары дают ошибку и
нулевой Snapshot. Отмена контекста сохраняется через wrapping для `errors.Is`.
Ответ ограничен 16 MiB; при превышении возвращается ошибка без частичного
каталога. Логику сохранения последнего успешного снимка определяет вызывающий код.

Нет refresh-loop, persistence, retry/backoff, WebSocket или подключения к
`cmd/market-data`; это отдельные результаты. При 429 вызывающий код должен
применять backoff перед следующим запросом.

Поля и публичный endpoint сверены 27 октября 2026 с
[официальным REST-контрактом](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#exchange-information).
Fixtures синтетические, содержат только нужные поля и не требуют живого Binance.

Проверка: `GOTOOLCHAIN=local go test -race ./internal/marketdata/catalog`.

Внутренний gRPC поверх Fetch и клиент API готовы в [MKT-018](../catalogrpc/README.md); фильтрация остаётся в этом пакете.
