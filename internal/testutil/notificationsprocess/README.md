# Disposable notifications process fixture (OPS-054)

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_notifications_process.py
# Настоящий executable (OPS-055):
GOCACHE=/tmp/binance-go-build python3 scripts/check_notifications_process.py --package ./cmd/notifications
```

Нужны Go из `.go-version`, Docker Desktop и PostgreSQL `.env` по
`infra/postgres/README.md`. Runner расширяет `check_api_process.py` владельцем
notifications: прежние Compose/init/migrate, уникальные проекты
`binance-process-{pg,kafka}-<random>`, случайные loopback порты, topics с уникальным
prefix и 16 delivery partitions. Применяются **только API и notifications**
миграции. Родитель получает `DATABASE_URL` (API fixture),
`NOTIFICATIONS_DATABASE_URL`, `POSTGRES_TEST=1`,
`NOTIFICATIONS_PROCESS_INTEGRATION=1`, `KAFKA_TEST_BROKER`, `KAFKA_TEST_PREFIX`.
Пароли читаются в память, не выводятся и не передаются в argv.

`New(t, apiPool, broker, prefix)` использует готовый grpctest lifecycle:
настоящие API `admissionrpc` (включая PrepareRecipient) и `resultrpc`,
production Latest wire adapter с управляемым источником. API читает Latest
по отдельному mTLS-соединению с URI `spiffe://process.test/api`.
У child URI `spiffe://process.test/notifications` разрешены ровно три метода
Admission/PrepareRecipient/Result. Delivery, Snapshot, Replay и Latest ему
не разрешены. TLS 1.3, DNS `market.test`; CA/серверные ключи остаются в памяти
родителя. Только временные client certificate/key/CA пишутся с mode 0600
в `t.TempDir()`. Не логировать Fixture, Env, DSN, PEM или секреты.

`f.ChildEnv(notificationsDSN)` — полный allowlist, без копирования parent env:

- `NOTIFICATIONS_DATABASE_URL` — только своя БД;
- `KAFKA_BROKERS`, `DELIVERY_REQUESTS_TOPIC`;
- `API_RPC_ADDR`, `RPC_SERVER_NAME`, `RPC_CA_FILE`, `RPC_CERT_FILE`, `RPC_KEY_FILE`;
- `TELEGRAM_BASE_URL`, `TELEGRAM_BOT_TOKEN` — loopback и заведомо fake token с числовым bot ID.

Env — интерфейс fixture. Тест cmd/notifications добавляет обязательные budgets,
stable group и RPC_IDENTITY из README executable.
Executable запускается через `apiprocess.With/Config`: HTTP_ADDR, startup probe,
deadlines, TERM/KILL/Wait, process groups, PID registry и cleanup не дублируются.
Минимальный child проверяет роль/свою БД, отсутствие foreign DSN/parent secret,
запрет CONNECT к трём чужим БД, реальные PrepareRecipient/Result validation,
запрет matcher RPC, Kafka/16 partitions и управляемый Telegram HTTP до HTTP 200.
Это connectivity probe, **не продуктовый worker и не graceful SIGTERM**.

`f.Seed(ctx)` создаёт уникальные account/binding и одну подписку через API
quota.Create, затем canonical workflow/outbox через готовый RequestDelivery.
Возвращает `Dataset{Record, Key, Body}` с исходными outbox bytes. Ошибка seed
требует удалить disposable dataset. `f.Publish(ctx)` явно дренирует настоящий
API deliveryrelay; `f.SetPrice(value, sequence)` меняет Latest, сохраняя исходные
времена при чтении. Seed-котировка не даёт admission permission.

`f.Telegram.Requests` — ограниченная очередь входящих sendMessage. Каждый
`*Send` содержит ChatID/Text/ParseMode и ждёт `Reply(positiveMessageID)`, `RateLimit()` (429/retry_after=1) либо
`Disconnect()`; первая команда побеждает. Endpoint проверяет POST/path/JSON/HTML,
не делает внешних вызовов. Ожидания ограничены context, 20 с и cleanup.
Reply сообщает серверный ответ; получение его child доказывается последующим
Result RPC/SQL, а не самим фактом Reply. Отдельный `NewTelegram(t)` позволяет
проверить wire adapter без SQL/Kafka.

`f.PauseRPC(fullMethod, after)` задерживает **один следующий** matching RPC:
`after=false` до production adapter, `after=true` после успешного adapter,
до отправки ответа (для мутаций это после commit). `Pause.Entered` закрывается
на границе, `Release()` идемпотентен. Граница до Send — после admission commit;
во время Send — Telegram.Requests; после HTTP — вход/commit Result. Отмена,
RPC deadline, предел 20 с и cleanup освобождают gates. Потеря результата после
commit моделируется удержанием gate до клиентского deadline, затем Release.

`Observe(ctx, independentNotificationsPool, triggerID)` одним SQL statement
возвращает job bytes/state/fence/lease/next_attempt_at/next_check_at, counts claims/attempts/всех четырёх
outcomes и pending reports. `Checkpoint` только читает SQL Prefix;
`f.BrokerOffset` отдельным Kafka client только читает group offset;
`f.Result(ctx, dataset)` читает canonical API запись parent pool.
Observers не дают ребёнку API credentials и не меняют состояние/offsets.

Реальная интеграция сравнивает read_committed Kafka bytes с API outbox,
принимает их готовым intake, сравнивает SQL и broker Prefix, затем квалифицирует
fixture handler в родителе. Latest=99 при candidate=100 запрещает HTTP и сохраняет
workflow. Latest=101 даёт новый permit; наблюдаемы lease до Send, зарегистрированная
попытка во время HTTP, success/pending report после ответа, API delivered после
commit. После потери ответа Result новый reporter/независимый pool повторяет
сохранённый исход без Telegram, без нового attempt/outbox. Продуктовый executable
проверен отдельно в OPS-055; SIGTERM и HTTP drain — OPS-011, общий E2E fake — E2E-004.
Нагрузки, четыре процесса и настоящие Telegram credentials сюда не входят.

Без opt-in — явный SKIP. При opt-in отсутствие DB/Kafka — FAIL.
Runner отдельно запускает прежние lifecycle tests apiprocess.
Аварийный выход владельца проверяется тем же готовым тестом (ожидаемый exit 1):

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_notifications_process.py --package ./internal/testutil/apiprocess/testdata/ownerexit
```

Go owner выходит с кодом 19 без defers. Python finally удаляет зарегистрированную
child group и оба стенда, проверяет отсутствие containers/volumes/networks по
точным Compose labels. Рабочие стенды не адресуются. Ошибка cleanup — FAIL.
SIGKILL самого Python не исполняет finally; точные disposable projects тогда
удаляет оператор, как описано в apiprocess.
