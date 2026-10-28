# SQL-replay подписок (SUB-022)

`proto/api/v1/replay.proto` — unary gRPC для matcher. API единственный читает
свою БД. Подключение использует готовые журнал SUB-021 и pins SUB-015:

```go
opts, err := replayrpc.ServerOptions(cert, key, clientCA, matcherURI)
// Проверить err и установить все options, включая mTLS и авторизацию.
server := grpc.NewServer(opts...)
apiv1.RegisterSubscriptionReplayServer(server,
    replayrpc.NewServer(subscription.NewSnapshots(pool), pool))
client := replayrpc.NewClient(conn) // caller-owned mTLS connection
```

Все четыре метода разрешены только точному matcher URI. Владелец pin берётся
из проверенного сертификата, общ для реплик consumer; входного consumer ID,
Kafka offset и пользовательского Bearer в протоколе нет. Snapshot ID/resource ID
— канонический lowercase UUID. Бюджет клиента и сервера — 3 секунды на вызов,
с сохранением более раннего deadline и отменой pgx. Автоматических повторов нет.
Это бюджет операции, не измеренный SLA. Runtime и PKI подключаются отдельно.

1. Завершить durable загрузку S/C через SUB-016. `BeginReplay(snapshot_id,
   ttl_millis)` передаёт **тот же** pin из snapshot в replay. Готовый Handoff
   держит shared session retention-lock до проверки/commit: окна очистки нет.
   TTL 1..86400000 мс; первый handoff объявляет replay expiry, повтор не продлевает
   его, даже с другим TTL. ID, progress (первоначально C), issued, head H и
   expires_at возвращаются после commit. Повтор после timeout/потери ответа
   использует тот же ID; BeginReplay также позволяет прочитать текущий head.
2. `ReadReplayPage(resource_id, after, max_transactions)` возвращает полный
   префикс строго после after. Допустимы только целые границы
   `progress <= after <= issued`. Первую страницу читать после C; следующую —
   после `next`. Лимит 1..1000 считает транзакции. Страница наблюдает целый H
   **до** чтения журнала; commit во время чтения остаётся следующему вызову.
   H может расти между вызовами; клиент отслеживает достигнутую целую границу.
   State также учитывает более новый issued от конкурентного вызова. `next`
   — конец последней выданной транзакции, для пустого хвоста равен after.
   Пустая страница всё равно проверяет живой pin; это не сигнал потери ресурса.
3. `events` содержит байты существующих versioned JSON-конвертов CON-002
   SubscriptionChanged внутри Protobuf. Для разбора — `subscriptionchanged.Decode`.
   Сохраняются event ID, порядок position, transaction_end, полный after-image,
   точные decimal-строки и времена; float/double не вводятся. Выдача сохраняет
   только фактический `next` в issued, до сетевого ответа; потеря ответа допускает
   повтор той же страницы, пока её префикс ещё не подтверждён. Ответ ограничен
   3 МиБ событий с protobuf-overhead; граница размера никогда не режет транзакцию.
   Если первая транзакция больше бюджета, ResourceExhausted не продвигает issued;
   её нельзя молча пропустить или подтвердить. Нужна явная политика обработки
   размера/новый bootstrap; увеличение бюджета требует отдельной оценки.
4. Matcher атомарно сохраняет проекцию и cursor в **своей** SQL-транзакции,
   затем вызывает `AcknowledgeReplay(resource_id, boundary)`. API проверяет
   owner, срок, фазу, монотонность, целую выданную границу. Он не может проверить
   чужой SQL-commit: это обязанность consumer, Kafka offset его не заменяет.
   Равное подтверждение повторяемо; меньшее/невыданное/частичное отклоняется.
   После неоднозначного ответа повторяется та же граница. Конкурентный ack,
   продвинувший progress за after во время чтения, отклоняет старую страницу:
   перечитать текущий progress через BeginReplay и продолжить от него.
5. `AbandonReplay` явно отзывает свой pin, идемпотентно, включая expired/unknown.
   Чужой ID даёт пустой успех без изменения чужого pin, чтобы не раскрывать его
   наличие. Остальные операции над чужим/утраченным/истёкшим/abandoned pin дают
   FailedPrecondition: отбросить частичный bootstrap, начать новый snapshot
   с новым request ID. Истёкший pin больше не удерживает журнал; heartbeat нет,
   пауза consumer сама по себе не снимает pin до объявленного срока.

| Status | Значение |
| --- | --- |
| InvalidArgument | Неверный ID/TTL/лимит, частичная, невыданная или устаревшая граница |
| FailedPrecondition | Pin утрачен: нужен новый snapshot |
| ResourceExhausted | Первая целая транзакция больше бюджета страницы; продвижения нет |
| Unavailable | API/БД недоступны или исход commit неоднозначен; повторить прежнюю операцию |
| DeadlineExceeded / Canceled | Вызов остановлен, commit мог уже состояться |
| Unauthenticated / PermissionDenied | Исправить сервисные credentials/права |

Новый storage pin-адаптер, счётчик, миграции, cleaner, брокер и проекция matcher
не добавляются. Будущий SQL-cleaner обязан сохранять необходимые boundary batches
для валидации ReadAfter и учитывать действующие pins; publish pin не снимает.
Лимиты числа транзакций и размера ответа не устанавливают лимит размера
сохранённой SQL-транзакции или памяти готового ReadAfter: тот материализует
выбранные пакеты. Admission/нагрузочные бюджеты требуют отдельной проверки.

Проверка: `GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py` —
полный disposable PostgreSQL/race прогон, mTLS loopback, повтор через другой
сервер/пул после потери handoff/ack, целые транзакции, head vs concurrent commit,
настоящие create/cancel после C, монотонность, конкуренция, expiry, foreign owner,
SQL-отмена и недоступность. Отдельно `go test -race ./internal/api/replayrpc`
проверяет сетевые права и серверные/клиентские deadlines; PostgreSQL-сценарии
без POSTGRES_TEST пропускаются. Рабочие БД не мигрируются.
