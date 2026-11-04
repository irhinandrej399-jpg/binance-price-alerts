# Durable reporter — NTF-014

`New(notificationsPool, client)` принимает собственную БД notifications и готовый
`resultrpc.NewClient(conn)` с принадлежащим caller mTLS-соединением SUB-020.
`Reconcile(ctx, triggerID, limit)` выполняет один ограниченный проход (1..1000
отчётов) выбранного scheduler задания. После restart источником служат только
SQL pending reports; память reporter не содержит состояния восстановления.
Выбор заданий, частота/backoff, fairness и бесконечный runtime loop — NTF-012/013.
Caller должен выбирать также jobs с pending reports в stopped/terminal состояниях,
а не только jobs, доступные Claim: поздний успех и исторический отказ тоже нужны API.

Сохранение исходов остаётся готовым протоколом jobs: SaveSuccess, SaveUncertain,
SaveBlocked, SaveRetryable атомарно сохраняют свидетельство и pending report под
job/claim/attempt fence. Reporter не создаёт новый storage/RPC контракт и миграции.
Он читает четыре вида reports через immutable attempt → job, передаёт исходные
owner/subscription/workflow/attempt, message_id либо безопасную причину blocked.
Версия привязки остаётся в свидетельстве; API получает каноническую версию
конкретной попытки из своего permit. Значение из текущей job/event/привязки
не подставляется. API-ответ — подтверждение наблюдения, а не разрешение отправки.

Перед RPC query полностью закрыт. Транзакции, row locks и локальная lease через
сеть не удерживаются. Бюджет каждого RPC — SUB-020 (3 с) с учётом более раннего
caller deadline. После успешного RPC ack меняет **только** pending соответствующей
пары (attempt ID, outcome). Состояние job, lease, расписание, fence и канал reporter
не изменяет. Он не вызывает Telegram, admission, лимитеры или Claim.

Потеря ответа/ошибка RPC или ack оставляет durable обязанность. Повтор того же
исхода безопасен; concurrent reporters могут послать дубликаты и подтверждать
одну пару одновременно. Межсервисной транзакции/exactly-once нет. Успех читается
первым, но правильность не требует порядка: SUB-026 защищает blocked после
retryable/uncertain, API сохраняет приоритет успеха и отмены и отсеивает stale
отрицательные исходы. Ack одного вида не скрывает другие, включая отдельный
blocked attempts_exhausted при исчерпании retryable.

Проход продолжает остальные выбранные отчёты после ошибки одного, возвращает
число успешных RPC+ack и объединённые ошибки. Отмена завершает оставшуюся работу.
Постоянные RPC-ошибки (identity/conflict) нужно диагностировать; они не удаляются
и не превращаются в новый sendMessage. Caller задаёт backoff и устраняет такие
ошибки, чтобы они не занимали следующие ограниченные порции одного workflow.
При неоднозначном ack следующий проход перечитывает SQL: ack мог состояться.

Проверка: `GOCACHE=/tmp/binance-ntf014-build python3 scripts/check_postgres.py`.
Реальная БД notifications, контролируемый API, concurrent replay, ack failure,
поздний success и независимые исходы — report/report_test.go. Отдельные реальные
БД API/notifications, mTLS, потеря ответа после API commit и восстановление через
новый pool для всех категорий — api/resultrpc/reporter_test.go. Атомарный rollback
локальных исходов, stale/cancel/channel арбитраж также проверяются готовыми
jobs и API SQL-тестами полного runner. Реальные Telegram и cmd/runtime не входят.
