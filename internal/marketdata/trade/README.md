# Binance raw trade → PriceObserved — MKT-002

`trade.Decode(payload, Metadata)` принимает одно raw-событие `trade` и возвращает
валидный `priceobserved.Observed`. Источник — Binance Spot `<symbol>@trade`;
стабильный `StreamID = binance-spot-trade`. Raw trade выбран потому, что передаёт
каждую отдельную сделку, без агрегации нескольких сделок одного taker order.
`aggTrade`, combined wrapper и управляющие сообщения не принимаются.

Контракт сверён 27 октября 2026 по [официальной документации Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#trade-streams).

| Вход | Выход |
| --- | --- |
| `s` | `Pair`, нормализованная ASCII-пара по CON-003 |
| `p`, строка | положительный точный доменный Price, без float/округления |
| `t`, JSON uint64 | `SourceID`, десятичная строка без потери цифр; 0 допустим |
| `T`, JSON int64 | `TradeTime`, время сделки в UTC |
| `E`, JSON int64 | `SourceEventTime`, независимое время события в UTC |
| Metadata.ReceivedAt | исходное время приёма в UTC |
| Metadata.Generation/Sequence | переданная ревизия с фиксированным StreamID |
| Metadata.StreamState/GapDetected | переданное состояние потока и исторический gap |

`e` обязателен и равен `trade`; все перечисленные поля источника обязательны,
null запрещён. Ключи регистрозависимы: отсутствие `T` не заменяется `t`, а `E` — `e`.
Дополнительные поля (объём, maker flags и будущие расширения) игнорируются.
Цена и метаданные проходят валидацию CON-003. Неверный ввод не даёт нулевую цену
или частичную котировку: возвращаются zero Observed и ошибка.
`errors.Is(err, ErrInvalidTrade)` отличает повреждение данных/метаданных,
`ErrUnsupportedEvent` — неподдерживаемый непустой строковый тип события.

Metadata.Unit обязателен: `priceobserved.Milliseconds` (стандарт Binance) либо
`Microseconds` (при соответствующем параметре URL будущего WebSocket-клиента).
Единицы не угадываются по величине; нормализация использует готовый CON-003.
Поздний приём не меняет время сделки. Возраст и clock skew проверяются при
использовании котировки по DEC-002, декодер не вводит TTL.

Вызывающий код отвечает за исходное время приёма, durable ревизию, монотонность,
повторы и состояние потока. Декодирование само по себе не подтверждает live,
не очищает gap и не означает durable приём. Проверка пары по каталогу USDT,
сетевой клиент, latest-price, outbox и event envelope здесь не реализованы.
Не-ASCII символы Binance не представимы в текущем контракте CON-003 и отклоняются.

Проверка: `GOTOOLCHAIN=local go test ./internal/marketdata/trade`.
Fixtures синтетические, повторяют форму опубликованного raw trade; precise fixture
проверяет предельный uint64, точность Price и микросекунды без живого соединения.
