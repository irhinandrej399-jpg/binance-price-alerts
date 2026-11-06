# Disposable market-data process environment (OPS-049)

```sh
GOCACHE=/tmp/binance-go-build PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_marketdata_process.py
```

Требуются Go из `.go-version`, Docker Desktop и локальный
`infra/postgres/.env` по PostgreSQL README. Это тот же runner OPS-047
(`check_api_process.main(owner="market_data")`), Compose/init/migrate и
`apiprocess.With/Config`, без второго process lifecycle. Уникальные проекты
`binance-process-{pg,kafka}-<random>`, случайные loopback порты и topic prefix
изолируют прогон от рабочего стенда. Применяются только `migrations/market_data`.
Штатный init topics проверяет конфигурацию, включая `<prefix>-prices` с 16
партициями; создаваемые companion topics из общего init также disposable.

Runner предоставляет Go-тестам только `MARKET_DATA_DATABASE_URL` владельца
`market_data_runtime`, `POSTGRES_TEST=1`, `MARKET_DATA_PROCESS_INTEGRATION=1`,
`KAFKA_TEST_BROKER`, `KAFKA_TEST_PREFIX`. Все унаследованные app settings/DSN
убираются; пароль извлекается только для выбранного владельца, не попадает
в argv или вывод. DSN нельзя печатать. Child получает полный явный `Config.Env`,
без наследования окружения родителя. Реальная проверка child подтверждает
имя БД/роли, CONNECT своей БД и отсутствие CONNECT трёх чужих БД.

До выбранного пакета runner запускает готовые lifecycle tests apiprocess:
readiness/exit/start failure, timeout/kill, cancellation и очистку PID registry.
Все команды и ожидания ограничены deadlines. Cleanup использует точные Compose
labels и проверяет отсутствие containers, volumes и networks обоих проектов.
`API_PROCESS_PID_DIR` — прежнее имя общего registry; переименовывать его не нужно.

## Интерфейс для OPS-048 / OPS-009

Когда появятся тесты настоящего executable:

```sh
GOCACHE=/tmp/binance-go-build PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_marketdata_process.py --package ./cmd/market-data
```

Тест собирает executable отдельно и запускает через `apiprocess.With(ctx, cfg, check)`.
В `cfg.Env` передаются DSN из `MARKET_DATA_DATABASE_URL`, broker, prices topic,
stable transactional ID и явные настройки runtime по его будущему контракту.
Harness сам выделяет `HTTP_ADDR`, ожидает `/readyz`, ограничивает stop/kill/wait
и удаляет PID marker. `cfg.Args` не должен содержать secrets. Callback обязан
соблюдать ctx/deadlines. Контракт lifecycle — [apiprocess](../apiprocess/README.md).

`Seed(ctx, pool, raw)` принимает **fresh disposable market_data** pool и raw
`internal/marketdata/trade/testdata/trade.json`. Через готовые Decode,
BeginGeneration/Accept сохраняет одну pending работу и возвращает
`Fixture{Generation, EventID, Key, Value}`. Key/value читаются из canonical
outbox, а не пересобираются. Seed не claim-ит работу, поэтому её может сразу
забрать новый publisher после рестарта. При частичной ошибке выбросить dataset;
helper не реализует rollback нескольких отдельных durable операций.

`NewRawFeed(raw)` предоставляет `.URL` вида
`ws://127.0.0.1:<port>/ws/btcusdt@trade` и `.Close()` (обязательно defer).
По шаблону stream tests каждый WS получает один исходный raw payload;
соединение остаётся открытым до peer close или Feed.Close. Повторные соединения
получают ту же сделку. Close закрывает также hijacked WebSockets. Это endpoint
одного BTCUSDT потока: нет каталога, управляющих сообщений, полного Binance
или моделирования разрывов/ротации. Для E2E-003 нужен отдельный fake.

Тестовый child в этом пакете проверяет только HTTP probe, credentials и WS.
Он не сохраняет цены и не публикует. Родитель через Seed создаёт работу,
новый Repository восстанавливает её через Claim, готовый publish producer
отправляет исходные bytes и MarkPublished фиксирует результат. Независимый
read_committed consumer сравнивает исходные bytes и event_id. Настоящий
runtime и восстановление после его сбоев остаются в OPS-048/009.

## Отказные и локальные проверки

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race -count=1 -v ./internal/testutil/marketprocess ./internal/testutil/apiprocess
```

Без integration opt-in PostgreSQL/Kafka проверка явно SKIP; WS child и
lifecycle проверяются без Docker. С opt-in отсутствие POSTGRES_TEST, DSN,
БД или Kafka — FAIL, не SKIP. Отрицательная проверка без credentials:

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local MARKET_DATA_PROCESS_INTEGRATION=1 go test -count=1 -run '^TestDisposableFixture$' ./internal/testutil/marketprocess
```

Ожидается exit 1. Проверка аварийного выхода Go-владельца (также exit 1):

```sh
GOCACHE=/tmp/binance-go-build PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_marketdata_process.py --package ./internal/testutil/apiprocess/testdata/ownerexit
```

Готовый ownerexit fixture OPS-047 обходит defers через os.Exit(19).
Обязательны `Cleanup verified: registered child group` и подтверждения очистки
обоих проектов. Это не graceful SIGTERM продукта. SIGKILL внешнего Python
runner обходит его finally и требует ручной очистки по точным labels, как и
в OPS-047. Нагрузка/HA/full E2E здесь не проверяются.
