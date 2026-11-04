# Ограниченный notification worker — NTF-013

`New(Config)` → `Run(ctx)` соединяет `jobs.Store` (`Source`) и готовый
`handler.Handler` (`Handler`). Конфигурация обязательна: `Concurrency` 1..1000,
положительные `Interval`, `Timeout`, `WriteTimeout` и `RecoveryInterval`
1 с..1 ч (целые мкс). `handler.New` отдельно требует явные Policy,
RecoveryInterval и ReportLimit; runtime должен согласовать recovery interval
обоих компонентов. Production defaults и сборка процесса здесь не выбираются.

Один проход выбирает не более Concurrency заданий по keyset cursor, вызывает
их параллельно и ждёт завершения всей порции. Перед следующей выборкой всегда
есть Interval: при пустой очереди, SQL/RPC-ошибке и успешной порции. Пустая
страница возвращает cursor к началу; постоянно доступные первые задания не
закрывают последующие страницы конечной очереди. Это ограниченный волновой
цикл, без обещания пропускной способности или строгой fairness при бесконечном
поступлении возрастающих ключей. Зависший до deadline вызов задерживает волну.

`jobs.Discover(ctx, after, limit)` читает только notifications DB и SQL clock:

- доступные обычные задания с наступившими next_attempt_at/истёкшими leases;
- назначенные channel checks с наступившим next_check_at;
- не назначенные channel blocks текущего fence, включая исторические исходы
  после uncertain; Worker вызывает только ScheduleRecovery, без Handle/HTTP;
- pending reports всех четырёх видов, включая stopped/terminal/succeeded jobs.

Чтение не захватывает lease. Другая реплика может выбрать те же строки;
готовые Claim/ScheduleRecovery/reporter повторно проверяют fence, срок и ack.
Поздний успех запрещает обычную работу, но сохраняет путь pending report.
Лимит ограничивает число возвращённых строк, а не стоимость SQL scan;
нагрузка и план запроса пока не измерялись. Новых миграций нет.

Каждый вызов и discovery получает Timeout. Отмена context проверяется перед
выборкой и перед вызовом каждой работы; начатые вызовы получают отмену.
Зависимости обязаны соблюдать context — worker не может принудительно
остановить произвольный callback/HTTP. `OnError` — необязательный конкурентный
наблюдатель ошибок; он должен быстро возвращаться. Обычные ошибки не
останавливают очередь и повторяются после Interval/обхода cursor.

`handler.PendingWrite` распознаётся через интерфейс `error + Retry(ctx) error`.
Worker удерживает исходный объект и слот, повторяя **только Retry**, по одному
вызову после Interval, с отдельным WriteTimeout. До записи исхода нового
Handle этого workflow нет. Отмена даёт последнюю попытку записи с новым
ограниченным context, сохраняющим values основного context. Если она тоже
не удалась, Run возвращает ошибку с исходным объектом в error chain и хранит
его внутри Worker. Следующий Run того же экземпляра сначала завершает эти
записи, затем открывает очередь. Caller не должен выбрасывать экземпляр или
ошибку, пока процесс продолжает работу с известным исходом. После потери
процесса остаётся готовое uncertain recovery, с риском внешнего дубля.

Одновременные Run одного экземпляра запрещены. После отмены Run возвращает
context.Canceled/DeadlineExceeded, объединённый с незаписанными исходами.
Сигналы процесса, runtime wiring, SIGTERM/HTTP-проверки и полный graceful
shutdown относятся к OPS-011/дальнейшим runtime-задачам.

Проверки: `go test -race ./internal/notifications/worker` (управляемые ожидания,
параллелизм, cursor, ошибки, отмена, recovery и удержание записи),
`GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py` (реальный
PostgreSQL/race, SQL discovery и существующий handler/mTLS/HTTP-контракт).
Реальные Telegram, Kafka и нагрузка не проверяются этим циклом.
