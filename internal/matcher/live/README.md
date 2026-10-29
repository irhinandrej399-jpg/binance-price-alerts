# Координатор snapshot → replay → live (MAT-008)

`New(store, replay, registerChanges, connectPrice, Config)` связывает готовые
MAT-014, SUB-022, MAT-015 и MAT-012. `Run(ctx)` обслуживает **одно новое** полное
поколение MAT-007 и блокируется до отмены/ошибки. Caller задаёт новый отдельный
projection ID в `projection.New`; один координатор — единственный writer этого
поколения. `Run` однократный. Внешний runtime/назначение партиций ещё не подключены.

```go
coordinator, err := live.New(projection.New(pool, projectionID), replayClient,
    func(ctx context.Context) (live.Registration, error) {
        return changes.Register(ctx)
    },
    func(ctx context.Context, pair string) (live.QuoteConnection, error) {
        return priceconnection.Connect(ctx, pair, partitionFor(pair),
            registerPrices, latestClient, time.Now)
    }, live.Config{
        SnapshotID: generation.SnapshotID,
        ReplayTTL: time.Hour,
        HeadInterval: time.Second,
        PageSize: 100,
    })
// Проверить err. Run запускается владельцем; ошибки требуют нового bootstrap.
err = coordinator.Run(ownerCtx)
```

## Порядок и границы

1. Регистрация `subscription-changes` **до** BeginReplay/H. Пока строится
   проекция, наблюдаются Done/Err регистрации; потеря отменяет SQL/RPC.
2. Атомарный Initialize полного снимка; BeginReplay передаёт прежний pin.
   Целые страницы SQL-пакетов применяются строго после progress API. До ack
   каждой страницы прочитан durable checkpoint matcher. Kafka C, SQL issued
   и транспортный NextOffset различаются: Kafka может быть впереди progress;
   SQL-дубли безопасно перечитываются и подтверждаются только по выданным страницам.
3. Один согласованный Walk строит индекс **active** подписок; terminal и прочие
   незавершённые состояния остаются durable, workflow не создаётся/не теряется.
   По каждой активной паре Connect выполняет MAT-012, далее один reader Next
   постоянно продвигает Current, включая обновления во время replay/активации.
4. После построения/подключения ещё раз проверяются head/pin и применяется хвост.
   Только затем публикуется ephemeral view этого поколения. Переключение в live
   не создаёт durable указателя, не реализует restart и не передаёт владение.
5. Kafka-пакеты применяются через Handle: проекция/C/NextOffset одной SQL-
   транзакцией **до Confirm**. Пропуск C закрывается SQL. Индекс обновляется
   через ReadImage durable состояния, поэтому старые версии/дубли не возрождают
   отмену. Неиспользуемые ценовые подключения закрываются.
6. Проверка SQL-head продолжается по HeadInterval даже при пустом Kafka или
   непрерывном потоке. Конечная целая H фиксируется на каждый проход; новые
   commit попадут в следующий проход. При изменении проекции readiness отозвана
   до согласования индекса/цен. Пока нет живого pin, одного complete/C недостаточно.

`HeadInterval` по умолчанию 1 с — настройка опроса, не SLA/измерение задержки.
PageSize по умолчанию 100, максимум 1000 целых транзакций. Используются прежние
лимиты SUB-022 и MAT-015; память/нагрузочная квалификация не измерялись.

## Выход для обработчика кандидатов

`Ready()` проверяет фазу, lifetime, локальный срок pin и Done всех необходимых
регистраций. `Current(pair)` возвращает согласованные C, последнюю обработанную
котировку и кандидатов индекса. После idle loss прежняя котировка не выдаётся.
`Changed()` — канал с одним coalescing wakeup: после сигнала перечитать Current;
это не очередь исторических цен/кандидатов. Первая текущая подходящая цена
доступна сразу после активации без ожидания пересечения. Исчезнувшее во время
bootstrap условие 99→101→99 не воспроизводится. Цена создания — только аудит.

MAT-004 должен использовать последнюю ревизию, запросить канонический переход
API и сохранить durable исход/явный superseded до подтверждения цены. Этот
координатор **не подтверждает цены**, не вызывает API кандидатов/Telegram и не
гарантирует доставку одного ephemeral wakeup как durable работы. Нельзя запускать
второй Next-reader поверх переданного Connection. Consumer group, SQL fencing,
восстановление checkpoint и runtime относятся к MAT-009/010/011 и последующим
задачам; Confirm MAT-015 не является broker OffsetCommit.

## Отказы и проверка

Любая ошибка SQL/RPC, потеря Kafka/цен, отмена, повреждённая страница и утрата pin
отзывают view; Run освобождает регистрации и ждёт readers. Ошибки возвращаются
без скрытых повторов и skip. `ErrPin`/RPC FailedPrecondition требуют нового
снимка/request ID. После неопределённого commit нельзя продолжать из памяти;
durable данные сохраняются изолированными, restart отдельно в MAT-009. Pin не
продлевается и не abandon-ится скрытно; явный AbandonReplay/очистка — у владельца,
иначе он удерживает журнал до expiry.

Проверки:

```sh
GOTOOLCHAIN=local go test -race ./internal/matcher/live
GOCACHE=/tmp/binance-go-build KAFKA_RUN_POSTGRES=1 bash infra/kafka/verify-go.sh
```

Общий runner уже умеет совместный disposable PostgreSQL/Kafka и удаляет volumes.
`internal/api/replayrpc/matcher_live_test.go` использует его, существующие
mTLS/grpctest fixtures, настоящий Replay.Client/Server, журнал/pin API,
MAT-007/014 storage с чтением второго pool, MAT-015 Kafka и MAT-012 Connection.
Контролируемый источник цены задаёт исчезнувший true/первое обновление; снимок
фиксирует тестовое пустое S/C на настоящем API pin, изменения пишутся в реальный
журнал во время загрузки/регистрации. Проверяются create/cancel, целая транзакция,
SQL-only хвост, Kafka-дубль/gap, checkpoint/Confirm и idle потеря цены. Только
PostgreSQL-прогон использует управляемый changefeed; без POSTGRES_TEST этот
интеграционный сценарий пропускается. Существующие тесты MAT-015 отдельно
проверяют broker/offset loss и повреждённые Kafka-пакеты.
