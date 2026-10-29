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
  сверяются; наличие Event также проверяет исходную ревизию. Отдельный runtime
  должен обеспечить durable маршрутизацию ключа/повторную доставку, а MAT-005 —
  обход Pending и актуализацию незавершённых работ после restart.
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
wakeup, не перечисляет пары/ключи после падения, не доказывает полноту общего
префикса, не выбирает consumer group и не делает SQL fencing. MAT-005/010/011 и
runtime добавляют эти части; Telegram и нагрузочная квалификация отдельно.

Проверки: `GOTOOLCHAIN=local GOCACHE=/tmp/binance-go-build go test -race
./internal/matcher/pricehandler` (SQL-тесты без opt-in SKIP),
`GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py` — настоящий
PostgreSQL с другим pool до ack, управляемые отказы RPC и test-only deferred
commit fixture MAT-016; `internal/api/deliveryrpc/pricehandler_postgres_test.go`
проверяет handler → mTLS SUB-014 → canonical SQL/outbox, ложную последнюю цену
и восстановление одного workflow при новой проверке.
