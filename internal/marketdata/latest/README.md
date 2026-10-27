# Последняя котировка: MKT-006

Unary RPC `/marketdata.v1.Quotes/Latest`, схема —
[`latest.proto`](../../../proto/marketdata/v1/latest.proto), generated Go —
`internal/gen/marketdata/v1`. Пара задаётся канонически `[A-Z0-9]+`;
несуществующая пара и пара без сделки дают `missing`. Каталог этим RPC не проверяется.

`NewServer(storage.New(pool), time.Now)` читает один согласованный durable
снимок через `storage.Repository.Latest`. Цена — точная десятичная строка;
source ID, stream ID и generation/sequence — исходные значения снимка.
Повторное чтение ничего не записывает. Цена/ревизия не собираются разными запросами.
Только market-data подключается к своей БД; API использует gRPC-клиент.

В ответе `trade_time`, `received_at`, `checked_at` — UTC RFC3339Nano.
Возраст вычисляется при каждом запросе по времени сделки, без округления;
`Age` содержит секунды и наносекунды (0..999999999), без ограничения time.Duration.
До 5 секунд включительно — `fresh`, больше — `stale` и WARNING_STALE,
при этом цена остаётся доступной. Возраст не определяет здоровье соединения.
`unconfirmed` даёт отдельный WARNING_STREAM_UNCONFIRMED при любом возрасте.
Исторический `gap_detected` даёт WARNING_GAP_DETECTED и после подтверждения live.
Клиент отображает предупреждения пользователю согласно DEC-002.

`missing` и `invalid_time` — успешные доменные ответы без quote/age;
использовать их как цену нельзя. Будущее, отсутствующее или выходящее за
RFC3339 время сделки даёт `invalid_time`. Исходные времена при чтении не меняются.
До начала обслуживания после рестарта runtime обязан выполнить BeginGeneration
для каждой пары по [контракту storage](../storage/README.md). Это оставляет
сохранённую котировку доступной, но переводит поток в unconfirmed;
открытие соединения само по себе не подтверждает актуальность. Подключение ingest
и запуск RPC в cmd остаются задачами runtime, в MKT-006 они не добавлены.

Подключение будущим runtime:

```go
opts, err := latest.ServerOptions(serverCertPEM, serverKeyPEM, clientCAPEM, apiURI, matcherURI)
// Проверить err; оба URI SAN берутся из доверенной конфигурации окружения.
server := grpc.NewServer(opts...)
pb.RegisterQuotesServer(server, latest.NewServer(storage.New(pool), time.Now))
// Serve и остановка принадлежат runtime.
```

ServerOptions устанавливает mTLS и выдаёт право на Latest только `apiURI` и
`matcherURI` по DEC-009: API читает цену при создании/допуске доставки, matcher —
при подключении и обработке. Notifications получает допуск через API и здесь прав не имеет.
Пример тестовой идентичности — `spiffe://binance.test/api`; рабочий URI, CA,
сертификаты и DNS-имя сервера задаются окружением, секретов в репозитории нет.
Полный набор options нельзя переопределять; см.
[serviceauth](../../platform/serviceauth/README.md).

Соединение клиента создаётся с `serviceauth.ClientCredentials` и
`grpc.WithAuthority(expectedServerDNSName)`; `latest.NewClient(conn).Latest(ctx, pair)`
ограничивает RPC одной секундой, сохраняя более ранний deadline вызывающего кода.
Обработчик также ограничивает вложенное чтение одной секундой и передаёт context
в PostgreSQL. Это начальный бюджет операции, не измеренный SLA; он входит в бюджет
будущего вложенного вызова API. При сетевом timeout серверный context может получить
Canceled раньше локального DeadlineExceeded. Отмена всегда прекращает чтение.
Прикладные повторы не добавляются; безопасное повторное чтение возвращает текущий
снимок, который мог уже измениться.

| Результат | gRPC |
| --- | --- |
| Неверная форма пары | InvalidArgument |
| Нет цены / некорректное время сделки | OK + missing / invalid_time |
| Нет права у доверенного сервиса | PermissionDenied до handler |
| Ошибка TLS / сервер недоступен | Unavailable |
| Недоступна БД / ошибка чтения | Unavailable без исходного текста ошибки |
| Истёк deadline / отменён context | DeadlineExceeded / Canceled |

Проверки:

```sh
go test -race ./internal/marketdata/latest -count=1 -timeout=30s
python3 scripts/check_postgres.py
python3 scripts/generate_proto.py --check
```

Первый прогон проверяет доменные границы и реальный loopback mTLS; PostgreSQL-сценарий
без opt-in пропускается. Второй поднимает disposable БД и выполняет общий race-прогон,
в том числе RPC с реальным Latest, отказом старых/повторных source ID, новым поколением
после пересоздания repository и сохранением времени/ревизии. Физический рестарт
процесса/сервера БД, рабочий PKI, runtime и нагрузка этим не проверяются.
