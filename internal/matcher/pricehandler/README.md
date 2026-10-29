# Handler текущей цены — MAT-004

`New(pricework.New(matcherPool, stableScope), coordinator, projectionStore,
 deliveryrpc.NewClient(apiConn), projectionID)` связывает готовые MAT-008/009,
SUB-025, MAT-016 и SUB-014. `Handle(ctx, Request, ack)` выполняет одну проверку.
Это прикладной handler; consumer loop и подключение в cmd остаются отдельно.

- `Pair` проверяется через `live.Current` при входе и перед каждым RPC. `Event` — необязательное исходное PriceObserved: более старое
  событие сохраняется с явным superseded на текущую цену, без исторических RPC.
  Если событие опережает view, возвращается ErrViewChanged, ack отсутствует.
  Первая Latest и coalescing Changed вызывают Handle с Event=nil: новое
  пересечение не требуется. Handler не читает Next вторым reader.
- Для новой работы полный набор Candidates замораживается вместе с C, ценой,
  ProjectionID и owner из ReadImage → Image.OwnerID(). После чтения owner снова
  проверяются C/ревизия. Изменившаяся view требует новой выборки; частичный набор
  не принимается. Legacy owner возвращает ошибку bootstrap из проекции.
- Caller назначает стабильный `Key` **до вызова** и сохраняет его для повторов.
  Это ID проверки view: другая подписка/C/версия при той же цене требует нового
  ключа. При повторе Get восстанавливает прежний вход, не пересобирает его из
  новой view. Ключ нельзя переиспользовать для другого запроса. Position и Pair
  сверяются; наличие Event также проверяет исходную ревизию. `Resume` восстанавливает запрос по ключу из SQL; `Recover` обходит Pending
  после restart. Runtime сохраняет связь повторной транспортной доставки с ключом.
- Accept полного входа commit-ится до RPC. RequestDelivery получает только
  owner/subscription/expected version; цену API читает сам. Canonical trigger_id
  и workflow version записываются через Record, собственные ID не создаются.
  Версии выше MaxInt64 отвергаются: wire SUB-014 использует int64.
- Повтор после неоднозначного ответа отправляет прежнюю команду даже после
  появления workflow/отмены в проекции. API восстанавливает прежний workflow. Если live уже удалил подключение пары,
  durable workflow/неактивный after-image допускает такой recovery без старой
  цены. Полный набор canonical исходов завершается и при исчезнувшей паре.
  Уже записанные результаты пропускаются; terminal работа повторяет только
  Complete/ack. Более новая текущая цена даёт superseded, сохраняя все исходы.
- NotFound фиксируется как not_found. Aborted/FailedPrecondition требуют нового
  Current и ReadImage: наблюдаемая большая версия даёт version_changed,
  неактивность — not_active, ложное условие — condition_false. Текст ошибки не
  разбирается. Когда view ещё не подтверждает причину, возвращается исходная
  ошибка, кандидат остаётся pending. Unavailable/DeadlineExceeded/Canceled,
  прочие RPC/SQL ошибки и некорректный OK без workflow не разрешают ack.

## Владение и подтверждение

Caller сериализует Handle/ack одной ценовой партиции. `Position` — реальная
Kafka-позиция исходного события; Latest/Changed без transport record не получают
выдуманный offset. `Prefix` — отдельно доказанный непрерывный read_committed
префикс всех пар партиции, не максимум обработанной пары. При Position без
Prefix допустимо сохранить результат с ack=nil; внешний ack без Prefix
отвергается. Prefix без Position также некорректен. Complete сохраняет исход
и Prefix одной SQL-транзакцией, затем вызывает ack. Ошибка ack требует повтора
с тем же Key/Position/Prefix. C не продвигает ценовой checkpoint.

Changed — только wakeup: после сигнала caller перечитывает нужные текущие пары и
использует новый ключ для новой view. Этот handler не обещает durable очереди
wakeup, не доказывает полноту общего префикса, не выбирает consumer group и не делает
SQL fencing. MAT-010/011 и runtime добавляют эти части; Telegram и нагрузочная
квалификация отдельно.

Проверки: `GOTOOLCHAIN=local GOCACHE=/tmp/binance-go-build go test -race
./internal/matcher/pricehandler` (SQL-тесты без opt-in SKIP),
`GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py` — настоящий
PostgreSQL с другим pool до ack, управляемые отказы RPC и test-only deferred
commit fixture MAT-016; `internal/api/deliveryrpc/pricehandler_postgres_test.go`
проверяет handler → mTLS SUB-014 → canonical SQL/outbox, ложную последнюю цену
и восстановление одного workflow при новой проверке.

## Восстановление принятой работы — MAT-005

После восстановления live-проекции и подключения свежей Latest через MAT-008/009
caller создаёт handler с **прежним scope** pricework и текущим projection ID:

```go
err := handler.Recover(ctx)
// Проверить err. Транспортный владелец отдельно восстанавливает свой префикс.
err = handler.Resume(ctx, stableKey, provenPrefix, ack)
```

`Recover` делает один конечный проход Pending → Get → Handle, без busy loop.
Вход, owner/sub/version, Pair и Position берутся из прежней SQL-работы,
сохранённые исходы пропускаются. Старые C/ProjectionID остаются аудитом принятия;
новое поколение не пересобирает набор под прежним ключом. Перед оставшимися RPC
Handle читает Current. ErrNotReady или временная ошибка API останавливают проход,
работа остаётся доступной следующему Recover; ошибка содержит ключ и сохраняет
исходную error chain. Пропущенного transient-кандидата нельзя считать завершённым.
Более новая цена durable supersede-ит старую работу, сохраняя принятые результаты;
исчезнувшее условие не воспроизводится. Неоднозначный прежний ответ при неизменной
цене повторяет canonical команду SUB-014 без нового workflow.

Порядок Pending — порядок ключей, **не Kafka offsets**. Поэтому Recover вызывает
Handle с nil Prefix/ack: принятые работы завершаются без выдуманного checkpoint.
`Resume(key, prefix, ack)` читает SQL даже для terminal ключа, отсутствующего в
Pending, сохраняет независимо доказанный Prefix и повторяет ack после commit.
Неизвестный ключ даёт pgx.ErrNoRows, новая работа под ним не создаётся. Ошибка ack
не теряет исход и не повторяет завершённые RPC. Latest остаётся без Position.

Один владелец сериализует Recover/Resume/Handle. Восстановление Pending не заменяет
первую проверку **всей текущей view**: runtime отдельно вызывает Handle с новым
ключом для текущих пар, в том числе после superseded при новой подходящей цене
или для подписок, ещё не принятых до сбоя. Выбор групп, fencing, отображение
повторной broker-доставки в stableKey, доказательство префикса, цикл повторов и
подключение в cmd остаются отдельными задачами. Ни этот метод, ни durable исход
matcher не разрешают Telegram-отправку.

SQL-тесты MAT-005: сбой после 37/100, lost response после commit 38-го workflow,
новый pool/handler/generation, недоступная view, новая ложная цена, пустые работы
разных пар и Latest без offset; terminal Resume после потери ack проверяет Prefix.
`internal/api/deliveryrpc/pricehandler_postgres_test.go` проверяет 100 уникальных
canonical workflow и ровно одну delivery_outbox строку на подписку через mTLS,
включая потерю ответа после API commit и повторный Recover.
