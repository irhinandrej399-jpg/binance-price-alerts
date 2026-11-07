# Disposable matcher process fixture (OPS-052)

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py
# После появления process tests OPS-053:
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py --package ./cmd/matcher
```

Требуются локальные Go из `.go-version`, Docker Desktop и PostgreSQL `.env`
по `infra/postgres/README.md`. Runner переиспользует `check_api_process.py`,
Compose/init/migrate, уникальные проекты `binance-process-{pg,kafka}-<random>`,
случайные loopback порты и topics `<prefix>-subscription-changes` (1 partition),
`<prefix>-prices` (16), `<prefix>-delivery-requests` (16). Применяет только
миграции API и matcher. Родителю доступны `DATABASE_URL` (только API fixture),
`MATCHER_DATABASE_URL`, `POSTGRES_TEST=1`, `MATCHER_PROCESS_INTEGRATION=1`,
`KAFKA_TEST_BROKER`, `KAFKA_TEST_PREFIX`. Пароли читаются в память, не в argv/лог.
Родитель не передаёт своё окружение child.

`New(t, apiPool, broker, prefix)` создаёт protected loopback RPC на готовом
`grpctest`: настоящие Snapshot/Replay/Delivery API adapters и управляемый
Latest через production wire adapter. Delivery проверяет Latest через mTLS.
CA и серверные ключи остаются в родителе; ephemeral matcher certificate/key/CA
пишутся в `t.TempDir()` с mode 0600. URI matcher `spiffe://process.test/matcher`,
DNS `market.test`, TLS 1.3 и явная method policy. Это только тестовые credentials.
Не печатать Fixture, Env, DSN, PEM или Args с секретами.

`f.ChildEnv(matcherDSN)` возвращает полный allowlist:

- `MATCHER_DATABASE_URL` — только своя БД;
- `KAFKA_BROKERS`, `SUBSCRIPTION_CHANGES_TOPIC`, `PRICES_TOPIC`;
- `API_RPC_ADDR`, `LATEST_RPC_ADDR`, `RPC_SERVER_NAME`;
- `RPC_CA_FILE`, `RPC_CERT_FILE`, `RPC_KEY_FILE`.

Эти env — интерфейс fixture, будущий cmd может явно преобразовать их в свою
конфигурацию. Запуск executable — через существующие `apiprocess.With/Config`:
HTTP_ADDR, deadline startup, TERM/KILL/Wait, PID registry и cleanup не дублируются.
Тестовый child подтверждает свою роль/БД и запрет CONNECT во все три чужие БД,
Snapshot/Replay, Latest, Kafka и 16 prices partitions до HTTP 200. Чужие DSN и
parent secret проверяются внутри child. Невалидный DSN даёт наблюдаемый early exit.
Тестовый child не запускает replica и не проверяет продуктовый graceful shutdown.

`f.Seed(ctx)` создаёт аккаунт, binding и три active подписки через API quota/Create,
затем один journal.Append с сохранёнными after-image для целой транзакции. Возвращает Dataset с OwnerID/Pair/IDs. Частичный Seed
не переиспользуют: удаляют весь disposable dataset. `PublishChanges(ctx)` явно
вызывает готовый API relay до пустого хвоста; фоновой подмены/пересборки bytes нет.
`Changes(ctx, pos)` открывает настоящий changefeed: регистрация LSO до replay H,
при resume — точный SQL cursor через RegisterAt; Close присоединяет Source.

`Bootstrap(ctx)` снимает полный read_committed LSO vector всех 16 partitions.
Сохранить его **до** PublishPrice и использовать при первом старте. Не переснимать
на restart: SQL Prefix имеет приоритет; отсутствие SQL cursor требует явного
bootstrap, никаких reset на latest. `SetPrice(pair,value,sequence)` сохраняет
неизменные времена/ревизию для Latest и возвращает quote для `PublishPrice(ctx,q)`.
PublishPrice кодирует канонический PriceObserved и коммитит транзакцию с правильной
pair partition, возвращает record с точным offset. В тесте новый SetPrice во время
publish должен быть только явно выбранным сценарием, а не автоматическим refresh.

`PauseDelivery(subscriptionID)` возвращает `DeliveryPause{Committed, Release}`.
Сигнал Committed приходит после durable API workflow/outbox, перед отправкой
ответа. Release идемпотентен и безопасен после отмены/смерти child. Задержка
ограничена 20 с и context клиента; cleanup всегда освобождает gate. Родитель может
читать matcher через отдельный pool и `pricework.New(reader, grant.Scope)`:
Pending/Get/Checkpoint, projection и fencing через готовые интерфейсы. API outbox
и trigger_id читаются родительским API pool; broker offsets — отдельным kgo client.

Integration сравнивает целый Kafka changefeed-пакет с исходными API journal bytes.
Затем квалифицирует fixture настоящей replica в родителе: полная view над
реальным Kafka changefeed, пауза после первого durable результата/второго API commit,
отсутствие преждевременного broker ack, отмена/join, восстановление Pending и
transport Prefix, тот же canonical trigger_id/outbox. Новая ложная Latest не создаёт
исторический workflow для оставшегося кандидата. Сам cmd/matcher — OPS-053,
сигнал ОС и graceful drain — OPS-010, четыре сервиса/E2E и нагрузка сюда не входят.

Без opt-in интеграция явно SKIP; при opt-in отсутствующие DB/Kafka — FAIL.
Готовые lifecycle tests apiprocess запускаются runner отдельно. Проверка аварийного
выхода владельца переиспользует существующую fixture (ожидаемый exit 1):

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py --package ./internal/testutil/apiprocess/testdata/ownerexit
```

Go-владелец выходит с кодом 19 без defers. Python finally убирает зарегистрированную
child group и оба стенда, затем проверяет отсутствие containers/volumes/networks по
точным Compose labels. Рабочие стенды не адресуются. Каждый этап ограничен timeout;
ошибка cleanup — FAIL. SIGKILL самого Python не позволяет выполнить finally;
в этом случае точные disposable projects убирает оператор, как в apiprocess.
