# Одна попытка доставки — NTF-012

`New(Config)` соединяет готовые `jobs.Store`, `admissionrpc.Client`, лимитеры
бота/чата, `telegram.Client`, `retry.Policy` и `report.Reporter`.
Policy, RecoveryInterval (1 с..1 ч, целые мкс) и ReportLimit (1..1000) обязательны;
production defaults и подключение процесса здесь не выбираются.

`Handle(ctx, triggerID)` выполняет один ограниченный проход без сна:

1. Перед любым claim передаёт ограниченную порцию durable reports. Ошибка
   останавливает проход; непустая порция также завершает его, чтобы сначала
   восстановить прежние исходы. Этот путь работает и для terminal/stopped jobs.
2. Делает Claim выбранного workflow. ErrUnavailable — нормальное отсутствие
   доступной работы. UnresolvedAttempt сначала сохраняется/сообщается как
   uncertain по историческому claim; в этом вызове нового claim/HTTP нет.
3. Декодирует сохранённое событие только для идентичностей/неизменяемого порога.
   PrepareRecipient → оба лимитера → AdmitDelivery с expected_recipient.
   Положительный wait сразу Park(WaitLimiter), без ожидания под lease. Ошибка
   чтения/API/лимитера откладывает работу без HTTP и не означает потерю канала.
   При missing_channel admission использует guard chat=0/version без лимитеров;
   появившийся канал даёт Aborted, отправки по этому пути нет.
4. wait_condition/attempt_busy/Aborted дают Park(WaitCondition). Следующий claim
   заново готовит адресата, проходит оба лимитера и получает новый request ID.
   blocked сохраняется через ParkBlocked; channel_unavailable также вызывает
   ScheduleRecovery. terminal останавливает проход; локальная аренда истекает
   обычным образом, каноническую terminal-подписку это не возрождает.
5. Для admitted проверяет workflow/subscription, chat/version, структуру quote
   и оба срока. Формирует HTML только по quote разрешения и регистрирует attempt
   до HTTP. Ошибка/потерянный ответ регистрации запрещает отправку. Send вызывается
   максимум один раз, с deadline=min(lease expiry, permit expiry, caller deadline).
   После регистрации ещё раз проверяются время и отмена context.
6. success → SaveSuccess, 429/5xx → SaveRetryable с durable бюджетом, постоянный
   отказ → SaveBlocked, Unknown/прочий недоказанный исход → SaveUncertain.
   Для зарегистрированного NotSent используется тот же консервативный uncertain
   протокол: API attempt нужно закрыть, успех и доказанный 429 не выдумываются.
   Затем отдельно вызывается reporter. Неизменяемый trigger_id сохраняется.

Request ID детерминирован SHA-256 от случайного claim ID (namespace v1); разные
claim имеют разные IDs. При Unavailable/DeadlineExceeded admission допускается
один немедленный replay с тем же ID и CAS в пределах lease/context. Ответ replay
не продлевает permit. Если ответ так и не получен, HTTP запрещён; следующая
проверка после парковки получает новый claim и новый ID. Старое sending может
вернуть attempt_busy до истечения своего срока. Разрешение нигде не хранится
для отложенной отправки. Sender никогда не повторяется внутри Handle.

## Ошибки сохранения и восстановление

`*PendingWrite` сохраняет известный исход в памяти. Caller должен повторять
`Retry(freshContext)` с ограниченным backoff; этот метод повторяет только прежние
SQL-операции, ScheduleRecovery и reporter, без Claim/лимитеров/Telegram. Число
повторов одного прохода и ожидание принадлежат worker. До успешной записи
не заменять Retry новым Handle того же workflow и не терять известный исход
в работающем процессе. Один PendingWrite нельзя вызывать
конкурентно. Успех/идентичности/message_id/retry cause/version и entropy сохраняются;
неподтверждённый uncertain deadline при необходимости обновляется в будущее.
Store при replay committed исхода всегда сохраняет первый план и ack.

Report уже durable после Save*. Потеря ответа API или ack восстанавливается
следующим Handle/reporter независимо от состояния job, без нового Send. При
потере процесса до durable записи внешнего ответа зарегистрированная попытка
восстанавливается как uncertain после expiry; внешний дубль возможен. Для
этого восстановления хватает исторического Lease/AttemptID из Claim.

Исчерпанный бюджет запрещает HTTP и при восстановлении. Если unresolved
попытка получила исчерпанный бюджет из-за позднего исхода прежней попытки,
handler восстанавливает исходную binding version через исторический admission
request ID этого claim, сохраняет uncertain и attempts_exhausted/report.
Исторический replay имеет приоритет над CAS: nil CAS в этом отдельном пути
используется только для восстановления свидетельства, никогда для HTTP.
Проверяются исходные workflow/subscription/attempt ID; несовпадение — явная
ошибка, отправка запрещена. Регистрация таким handler всегда следует после
успешного commit admission с его детерминированным ID. Нельзя удалять историю
admission/claims/attempts до согласованной retention-политики. После изменения
конфигурации MaxFailures уже исчерпанный claim также не отправляется; несогласованные
конфигурации реплик требуют вмешательства оператора.

`ScheduleRecovery` после durable channel block идемпотентен. Старый fence или
terminal/success не требуют назначения от старого handler. SQL-ошибки возвращаются
как PendingWrite. Scheduler обязан также находить не назначенные current-fence
channel blocks после crash и pending reports всех jobs — точные запросы/условия
в [jobs/README.md](../jobs/README.md). Выборка due jobs/reports/recovery, паузы,
параллелизм и остановка новых вызовов готовы в [worker NTF-013](../worker/README.md);
сигналы и остановка HTTP процесса проверяются отдельно в OPS-011.
Context/deadline для всего вызова задаёт caller; API RPC имеют свои готовые
бюджеты. Бесконечного loop, новых миграций и чтения чужих БД в handler нет.

Проверка: `GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py`.
Unit/race — `go test -race ./internal/notifications/handler`. Интеграция в
`internal/api/admissionrpc/handler_test.go`: реальные API/notifications БД,
mTLS admission/results, реальные лимитеры, fake HTTP disconnect → uncertain →
false → true, потеря ответа API после commit, регистрация перед crash,
429/retry_after, постоянный отказ, неизменившийся/новый канал и освобождение
lease при обоих лимитерах. Реальные Telegram, Kafka/runtime и нагрузка этим
тикетом не проверяются.
