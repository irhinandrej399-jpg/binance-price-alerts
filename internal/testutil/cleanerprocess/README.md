# Совместное disposable окружение cleaner — OPS-058

```sh
# Без opt-in: unit/race выполняются, реальный стенд явно SKIP.
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race -v ./internal/testutil/cleanerprocess
# Opt-in: отдельные PostgreSQL/Kafka, настоящие mTLS/SQL, race, cleanup.
GOCACHE=/tmp/binance-go-build python3 scripts/check_cleaner_process.py
# Преднамеренный exit 1; ожидаются оба Cleanup verified и child group cleanup.
GOCACHE=/tmp/binance-go-build python3 scripts/check_cleaner_process.py --package ./internal/testutil/apiprocess/testdata/ownerexit
```

Нужны Go из `.go-version`, Docker Desktop и локальный PostgreSQL `.env` по
[инструкции](../../../infra/postgres/README.md). Runner расширяет существующий
`check_api_process.py`, переиспользует Compose/init/migrate, уникальные проекты
`binance-process-{pg,kafka}-<random>`, свободные loopback ports и prefix topics.
Применяет штатные миграции API, matcher, notifications; test-only SQL functions
не устанавливаются. DSN этих трёх владельцев доступны **только родителю**:
`DATABASE_URL`, `MATCHER_DATABASE_URL`, `NOTIFICATIONS_DATABASE_URL`.
Другие credentials не извлекаются. Никакие рабочие проекты не адресуются.

Родитель поднимает два отдельных exporter owner процесса через готовый
`apiprocess.With`. Каждый запускает `ServeOwner(ctx)`: production SQL reader,
production gRPC server и production mTLS policy OPS-056/057. Это небольшие
тестовые владельцы exporter, без matcher evaluation/notifications worker.
Квалификация полного cmd уже принадлежит OPS-056/057; эта fixture проверяет
совместное окружение для будущего cleaner. Parent SQL observers используют
отдельные pools и не проходят через exporter или fault proxy.

## Интерфейсы

- `New(t, broker)` выдаёт `Fixture`. `OwnerConfig(role, ownDSN, executable, args...)`
  создаёт полный allowlist для `matcher` либо `notifications`: только
  `OWNER_DATABASE_URL`, role, RPC address и ephemeral ServerAuth/CA files.
  Executable вызывает `ServeOwner` с context SIGTERM/SIGINT (пример
  `TestOwnerChild` в integration_test.go). apiprocess задаёт HTTP_ADDR,
  ждёт `/readyz`, выполняет TERM/KILL/Wait и ведёт PID registry.
  `ServeOwner` перед readiness проверяет current_user и запрет CONNECT во все
  чужие БД. Ошибка startup безопасная, DSN/PEM не логируются.
- `f.Caller` содержит только broker и два `Endpoint`: address и пути к
  краткоживущим CA/client cert/key. `Caller.Env()` — полный allowlist для
  будущего caller child; `Endpoint.Conn(t)` — отдельный клиент mTLS.
  SQL DSN, owner keys и CA signing key туда не входят. `TestCallerChild`
  реально проверяет обе RPC и Kafka с этим окружением, отвергая чужие env.
- У каждого exporter отдельная CA, отдельная client identity
  `spiffe://cleaner.test/cleaner`, DNS `market.test`, TLS 1.3 и ровно один
  разрешённый ReadProgress. Ключи пишутся с mode 0600 в `t.TempDir()`.
  Сертификат той же CA с outsider URI отвергается обоими endpoints.
- Matcher использует запрос OPS-056 с `kind`, exact `consumer_id` (scope или
  projection), ClusterID/TopicID/Topic/partition; notifications — OPS-057 с
  exact ConsumerID/Generation/Source/partition. Значения для будущего inventory
  берутся из явно provisioned fixtures: `fencing.Grant`, `intake.Binding` и
  `kafkasource.Metadata`. Endpoint/consumer/source descriptors передаёт тест;
  **готового продуктового inventory и доказательства полноты здесь нет**.
- `f.PauseRPC(role, after)` — один следующий RPC на отдельном защищённом
  forwarding endpoint. `after=false` удерживает вызов до owner; `true` удерживает
  настоящий успешный ответ SQL adapter. `Entered` подтверждает границу,
  `Release()` пропускает, `Fail()` возвращает Unavailable без proof. Повтор
  выполняет caller явно. Deadline/cancel/20 секунд и cleanup освобождают gate.
  Gate не добавлен в production exporter и не подменяет его SQL результат.
- `NewBarrier()` / `Arrive(ctx)` / `Entered` / `Release()` координируют действия
  теста. В OPS-014 ими можно удержать регистрацию/retirement рядом с RPC или
  DeleteRecords, затем выполнить настоящую операцию будущего реестра. Barrier
  не хранит consumers, не разрешает retirement и не моделирует safe boundary.
- `Caller.Kafka(t)` создаёт отдельные kgo client и OPS-068 proxy. Для
  `proxy.Intercept(ctx, 8, AfterResponse)` или
  `proxy.Intercept(ctx, 21, BeforeRequest/AfterResponse)` используйте
  [контракт kafkafault](../kafkafault/README.md), Events/Fail/Release.
  Все реальные response codes проверяет тест. Kafka protocol здесь не реализован.
  Fixture caller ограничен Kafka API V2.8 для стабильного именования topics в
  observers OffsetCommit; TopicID по-прежнему проверяется SQL/RPC контрактом.
- `LogStart(ctx, broker, topic, partition)` и
  `ReadCommitted(ctx, broker, topic, partition, from, count)` используют прямые
  независимые Kafka clients. Последний читает ровно count (1..1024) data records;
  finite context обязателен, недостаток records — ошибка. Это чтение bytes,
  не решение об удалении. Для SQL используйте готовые
  `notificationsprocess.Observe/Progress`, `pricework.ReadCheckpoint/ReadScope`.

Все RPC имеют прежние пределы 1 с/4096 bytes, forwarding добавляет свой
ограниченный downstream call. Конечный caller deadline ограничивает gates.
Kafka helper работает только с disposable single-broker plaintext средой,
без production ACL/TLS. Не логировать Fixture/Caller/Env/DSN/PEM.

## Что квалифицирует реальный тест

Из canonical API quota/workflow/outbox взяты два разных delivery payload без
пересборки. Тест намеренно помещает их в одну fixture partition (не квалифицирует
production partitioner), независимо сверяет исходные key/value bytes, принимает
первую запись настоящим `AcceptBound`, оставляет вторую без job. Pending job и
Prefix читаются независимым SQL observer. Для matcher сохранены разные bound work/Prefix двух consumers одного topic
над настоящими PriceObserved; третий scope оставлен Pending без cursor.
RPC сравниваются с SQL, неизвестный consumer и Pending не дают proof,
отстающая partition честно отдаёт 0. Проверяются RPC delay/deadline/lost-response
и recovery, отдельный caller child без DSN и отказ outsider identity.

Настоящий OffsetCommit успешно обработан broker, его response потерян;
независимый OffsetFetch подтверждает offset, SQL job сохраняется. До отправки
DeleteRecords gate не допускает изменения log-start. Некорректный offset
даёт настоящий broker error. Затем успешный DeleteRecords response теряется,
прямой observer видит новый log-start. Явный повтор той же границы сохраняет
необработанную запись побайтно и прежние pending job/Prefix.
Это выбранная тестом граница для квалификации fixture; безопасность вычисления
минимума, обязательный реестр/registration/retirement проверяет только OPS-014.

Без `CLEANER_PROCESS_INTEGRATION=1` integration явно SKIP. При opt-in обязательны
`POSTGRES_TEST=1`, все три DSN, broker/prefix; отсутствие зависимости — FAIL.
Runner не превращает недоступный Docker/Go/БД/Kafka в skip. У каждого этапа timeout.
После успеха, callback/startup error и аварийного выхода Go owner Python finally
убирает зарегистрированные child groups, оба проекта и проверяет отсутствие
containers/volumes/networks по точным Compose labels. Cleanup failure — FAIL.
SIGKILL самого Python не исполняет finally: точные disposable проекты тогда
удаляет оператор, как в [apiprocess](../apiprocess/README.md).
