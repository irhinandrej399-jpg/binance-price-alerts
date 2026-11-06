# Disposable API process harness (OPS-047)

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_api_process.py
```

Требуются Go из `.go-version`, Docker Desktop и локальный `infra/postgres/.env`
по инструкции PostgreSQL. Runner использует существующие Compose/init и
`scripts/migrate.py`: уникальные проекты `binance-process-pg-<random>` и
`binance-process-kafka-<random>`, случайные loopback порты, префиксные topics.
Применяются только штатные миграции API; test-only SQL functions не нужны.
Рабочие проекты и их volumes не адресуются. API runtime password считывается
в память через container env, не передаётся в argv и не печатается.
Другие owner DSNs не считываются. Каждый этап имеет timeout; runner ведёт приватный PID registry child groups,
чтобы убрать их также после panic/timeout go test без defers. finally пытается
удалить оба стенда и проверяет отсутствие containers/volumes/networks по
точному Compose label. Cleanup failure завершает runner ошибкой.

Для тестов OPS-046 можно передать `--package ./cmd/api` (когда там появятся
process tests). Runner предоставляет тестам `DATABASE_URL`, `POSTGRES_TEST=1`,
`API_PROCESS_INTEGRATION=1`, `KAFKA_TEST_BROKER`, `KAFKA_TEST_PREFIX`.
Topics: `<prefix>-subscription-changes`, `<prefix>-delivery-requests`;
каждому relay нужен отдельный стабильный transactional ID с этим префиксом.
Env содержит только DSN API. Секреты нельзя логировать или передавать в Args.

## Запуск executable

```go
err := apiprocess.With(ctx, apiprocess.Config{
    Executable: builtAPI, // готовый executable, не go run
    Env: map[string]string{
        "DATABASE_URL": os.Getenv("DATABASE_URL"),
        // Добавить Kafka/topics/transactional IDs по контракту cmd/api.
    },
    StartupTimeout: 5 * time.Second,
    StopTimeout: time.Second,
}, func(p *apiprocess.Process) error {
    // p.URL — http://127.0.0.1:<port>; p.PID(), p.Done(), p.Stop().
    // HTTP requests/checks используют ctx и конечные deadlines.
    return verify(ctx, p.URL)
})
```

Harness задаёт `HTTP_ADDR=127.0.0.1:<port>`, ждёт HTTP 200 на `/readyz`,
параллельно наблюдает exit. Env — полный явный набор, окружение родителя
не наследуется. stdout/stderr child отбрасываются, чтобы ошибка child не
вывела credentials. Ошибки старта безопасные, exit code сохраняется.
После callback (также error/panic/Goexit), ошибки старта/readiness или отмены
context выполняется TERM → ограниченное ожидание → KILL → ограниченный Wait.
Сигналы адресуются собственной Unix process group; child reaped до возврата,
либо возвращается явный ErrStop. Callback сам обязан учитывать ctx; произвольную
зависшую Go-функцию harness не прерывает. Отмена ctx отдельно останавливает child.

Это Unix helper (macOS/Linux). Выбор свободного порта и bind child разделены;
при коллизии bind child должен завершиться, а запуск — повторяться с новым портом.
Внешний SIGKILL владельца не позволяет выполнить Go defers/Python finally;
после такого завершения disposable проекты удаляются вручную по их точным labels.
Тестовый child — только `TestChild` в test binary, HTTP probe и соединение с API-БД;
он не является реализацией cmd/api. Проверки graceful drain принадлежат OPS-004.

## Fixture

`Seed(ctx, apiPool)` предназначен только для fresh disposable API-БД.
Возвращает `Fixture{OwnerID, Token, Active, Delivery, DeliveryEventID,
DeliveryKey, DeliveryValue}`; структуру нельзя выводить из-за bearer token.
Аккаунт и действующая session создаются через account/session; две подписки
через `LockQuota/Create` с реальной квотой. Привязка Telegram задаётся тестовым
SQL как в существующих quota fixtures. Active остаётся active для HTTP-отмены.
Для второй подписки `RequestDelivery` создаёт canonical pending workflow,
неизменяемый delivery outbox и journal. Исходные delivery key/value/event_id
читаются из outbox, не пересобираются. Частичный Seed при ошибке не переиспользуют:
весь disposable dataset удаляется runner, включая append-only journal.

Реальный прогон проверяет session.Verify, active state, два reserved slots,
workflow/event identity, подключение child к API PostgreSQL. Готовый delivery
relay вызывается родительским тестом; независимый read_committed consumer
сравнивает key/value/event_id с fixture. Это проверка пригодности harness и
fixture. HTTP-отмена и оба relay внутри настоящего cmd/api остаются OPS-046.

Без Docker lifecycle проверяется отдельно:

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race -count=1 -v ./internal/testutil/apiprocess
```

Integration test явно SKIP без opt-in; с opt-in отсутствие DB/Kafka — FAIL.

Отказная проверка cleanup после аварийного выхода владельца (ожидается exit 1):

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_api_process.py --package ./internal/testutil/apiprocess/testdata/ownerexit
```

Владелец завершается с кодом 19 после readiness, обходя defers. Runner обязан
подтвердить удаление зарегистрированной child group и обоих стендов. Эта
намеренно падающая проверка находится в testdata и не входит в `go test ./...`.
