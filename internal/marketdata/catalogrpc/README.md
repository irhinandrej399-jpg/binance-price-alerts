# Внутренний каталог — MKT-018

`proto/marketdata/v1/catalog.proto` задаёт unary `marketdata.v1.Catalog/Fetch`.
`NewServer(source)` принимает `Source.Fetch(context.Context) (catalog.Snapshot, error)`;
готовый `catalog.Client` реализует этот интерфейс. Регистрация —
`marketdatav1.RegisterCatalogServer(server, catalogrpc.NewServer(source))`.
Сервер передаёт полный успешный снимок: точные symbol/base/quote, порядок пар и
исходный UpdatedAt в UTC RFC3339Nano с наносекундами. Фильтрация остаётся в
[каталоге Binance](../catalog/README.md); новой фильтрации или нормализации нет.

`ServerOptions(cert, key, clientCA, apiURI)` использует serviceauth (mTLS,
TLS 1.3) и разрешает Fetch только точному URI доверенного API. Matcher,
notifications и пользовательский Bearer такого права не получают. Все options
устанавливаются вместе. При регистрации Catalog и Quotes на одном сервере
нужно собрать **одну** общую политику через `serviceauth.ServerOptions`:
API — `Catalog_Fetch_FullMethodName` и `Quotes_Latest_FullMethodName`, matcher —
только `Quotes_Latest_FullMethodName`. Нельзя складывать два набора ServerOptions,
переопределять TLS или устанавливать независимые запрещающие interceptors.

API создаёт соединение с `serviceauth.ClientCredentials` и доверенным DNS SAN
сервера (при необходимости `grpc.WithAuthority`), затем
`client := catalogrpc.NewClient(conn)` и `snapshot, err := client.Fetch(ctx)`.
Клиент возвращает `catalog.Snapshot`; соединение создаёт и закрывает вызывающий
код. Прямого доступа API к Binance и SQL market-data нет. Проверка пары после
успешного Fetch — точное `pair.Symbol == requestedSymbol` среди snapshot.Pairs,
без сборки symbol из активов, смены регистра или проверки суффикса. При ошибке
Fetch проверка наличия пары невозможна; её нельзя трактовать как неизвестную пару.

Бюджет клиента и источника на сервере — **1 секунда** каждый, более ранний
caller deadline сохраняется. Context передаётся источнику, который обязан
реагировать на отмену; `catalog.Client` передаёт его HTTP-запросу. Истечение
бюджета возвращает DeadlineExceeded, отмена — Canceled; частичный снимок не
возвращается. Прикладных повторов, wait-for-ready, кэша и refresh-loop нет.
Клиент принимает Protobuf-ответ до 16 MiB (источник ограничивает JSON 16 MiB),
превышение даёт ResourceExhausted, а не обрезанный успешный каталог.

Успешный пустой каталог имеет пустой Pairs и исходный UpdatedAt. Ошибка источника
даёт Unavailable с безопасным сообщением `catalog source unavailable`, без
внутреннего текста ошибки. Недоступный транспорт также даёт Unavailable;
чужая доверенная сервисная идентичность — PermissionDenied. Без клиентского
сертификата mTLS отклоняет handshake до handler (обычно клиент видит Unavailable).
Bearer в metadata не заменяет сертификат. Некорректный timestamp ответа даёт
Internal (`invalid catalog response`). Любая ошибка клиента возвращает нулевой
Snapshot, без частичного каталога. Runtime, PKI и внешний HTTP endpoint не входят
в этот результат.

Проверки:

```sh
python3 scripts/generate_proto.py --check
GOTOOLCHAIN=local go test -race ./internal/marketdata/catalogrpc ./internal/marketdata/catalog ./internal/platform/serviceauth ./internal/testutil/grpctest -count=1 -timeout=60s
```

Сетевые тесты используют mTLS на реальном loopback; композиция с Binance Fetch
проверяется локальным HTTP fixture MKT-001. Живой Binance не требуется.
