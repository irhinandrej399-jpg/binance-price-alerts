# Локальная аренда и свидетельство успеха (NTF-009/010)

`New(notificationsPool, ttl)` создаёт конкурентно безопасный Store. TTL обязателен:
положительный, до минуты, целые микросекунды. Production использует PostgreSQL
`clock_timestamp()` после захвата строки; часы процесса не определяют владение.
Продления нет. Миграция — `notifications/004_job_leases.sql`.

`Claim(ctx, triggerID)` получает выбранное scheduler задание, если оно pending,
wait_condition, wait_limiter, durable uncertain/retryable либо leased с истёкшей арендой, и наступил
next_attempt_at. `FOR UPDATE SKIP LOCKED` не ждёт занятую строку. Неизвестное,
занятое, отложенное или остановленное задание даёт `ErrUnavailable`; caller
ограничивает частоту опроса и выбирает другие задания. Выборка готова в `Discover` / [worker](../worker/README.md); Claim не сканирует очередь.

В одной транзакции сохраняются новый случайный ID аренды, увеличенный bigint
fence, срок и строка `app.delivery_claims`. Предыдущая истёкшая аренда получает
status expired, история сохраняется. Равенство времени сроку уже запрещает
изменение. Переполнение fence откатывает транзакцию. Истечение не является
доказательством отправки, отказа или uncertain: HTTP мог вообще не начаться.
Записи истории обозначают локальные захваты работы, а не API send attempts.

`Park(ctx, lease, reason, next)` для **неотправленной** работы атомарно сохраняет
будущий deadline, состояние wait_condition/wait_limiter, released в истории и
очищает текущий lease. Condition deadline вычисляет готовая retry.Policy;
лимитер возвращает собственное время ожидания. Дробные микросекунды округляются
вверх. Ожидание проходит после освобождения аренды, без сна внутри storage.
Park проверяет сохранённые ID/fence/срок под блокировкой, а не доверяет полю
ExpiresAt caller. Повтор Park, истёкший или чужой token даёт `ErrStale`, включая
запоздалого worker, когда новая аренда ещё не выдана. Счётчик Telegram-ошибок
не изменяется. HTTP-исход нельзя парковать этим методом вместо сохранения
свидетельства и pending report.

Каждый метод возвращает успех только после commit. Потерянный ответ Claim
означает запрет отправки: дождаться восстановления/истечения аренды и выполнить
новый claim. Потерянный ответ Park требует перечитать/повторить claim по обычным
правилам; повтор старого Park не создаёт новых полномочий. Произвольная ошибка
БД не доказывает rollback; срок ограничивает зависшую работу после crash.

Claim не разрешает Telegram. После лимитеров необходим отдельный актуальный
API-допуск; HTTP допустим только пока действуют и локальная аренда, и разрешение
API. При отказе лимитера/ожидании цены надо сразу Park и выйти. После рестарта
или истечения lease старое разрешение не используется. Локальный fence не
отзывает уже выполняющийся внешний запрос и не даёт exactly-once.

NTF-010 добавляет durable свидетельство успеха известной API-попытки и pending
report (см. ниже). NTF-007 добавляет uncertain (ниже); следующие задачи расширяют протокол:
[NTF-014 reporter](../report/README.md) — готовая передача API; NTF-012 — связывание
локального claim с ID допуска API, исходами, лимитерами и HTTP. Для новых
транзакций порядок блокировок: job → claim/attempt/outcome. Мутация текущей
работы проверяет ID/fence и SQL-срок после блокировки; историческое свидетельство
позднего успеха сохраняется отдельно и не требует живого старого lease.

Проверка двух pool/20 конкурентных worker, crash без Park, точного истечения,
старого token, парковки, restart и deferred commit rollback на disposable БД:

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py
```

Обычный go test без POSTGRES_TEST пропускает SQL-тесты. Runtime, реальный Telegram,
нагрузка и внешняя дедупликация здесь не проверяются.

## Подтверждённый успех (NTF-010)

Миграция `notifications/005_delivery_success.sql` добавляет неизменяемую
корреляцию API attempt ID → claim/trigger/fence, свидетельства с положительным
`message_id` и pending report по attempt ID. Runtime имеет только SELECT/INSERT
на попытки и свидетельства. Идентичность результата — (attempt ID, success);
несколько реально успешных попыток одного workflow сохраняются раздельно.

`RegisterAttempt(ctx, lease, apiAttemptID)` вызывается после API-допуска и до
HTTP, под действующим локальным lease. Один claim допускает только одну
API-попытку; повтор той же регистрации идемпотентен, другая идентичность даёт
`ErrConflict`. Истёкший/подменённый lease даёт `ErrStale`. Метод сохраняет
корреляцию, не проверяет удалённое разрешение: caller NTF-012 проверяет его
актуальность, workflow, адресата и сроки. Повтор регистрации не даёт права на
повтор HTTP. После регистрации Park запрещён (`ErrConflict`): уже возможный
HTTP-исход должен пройти outcome-протокол, а не потеряться как ожидание цены.
При потерянном ответе регистрации отправка запрещена; восстановление требует
протокола known/uncertain attempt, не нового HTTP по старому разрешению.

`SaveSuccess(ctx, lease, apiAttemptID, messageID)` одной транзакцией сохраняет
свидетельство и pending report. Незарегистрированная или относящаяся к другому
claim/fence/workflow попытка даёт `ErrStale`. Повтор того же успеха ничего не
создаёт и не возвращает подтверждённый report в pending; другой message ID для
той же попытки даёт `ErrConflict`. Caller передаёт положительный ID из
подтверждённого ответа `telegram.Send`, не произвольный/неоднозначный исход.

Если lease ещё действует и совпадает с текущим, job становится `succeeded`,
lease очищается, claim становится completed. Это локальная остановка, а не
канонический API delivered. Поздний успех сохраняется и после expiry/takeover
либо локальной отмены; **ни одно поле чужой/истёкшей текущей аренды и её истории
не меняется**. Наличие любого свидетельства запрещает последующие Claim,
RegisterAttempt и Park независимо от состояния job и подтверждения report.
Все эти операции сериализуются на job, затем работают с claim/attempt/outcome.
Уже разрешённый/начатый внешний запрос отозвать нельзя; внешние дубли возможны.

Ошибка БД, в том числе потеря ответа commit, не доказывает rollback. Повторяется
**тот же SaveSuccess с теми же идентичностями и message_id**, а не Telegram.
После crash свидетельство и report либо существуют вместе, либо отсутствуют;
если crash случился после внешнего успеха, но до durable записи, это остаётся
неопределённостью, без обещания exactly-once. NTF-014 читает pending report
через join с attempt/job (owner/subscription/trigger) и передаёт SUB-020 до
подтверждения. Сам отправитель API-отчётов, остальные HTTP-исходы и worker здесь
не реализованы. Историю claim/attempt/success нельзя удалять до согласованной
политики retention.

SQL/race-тесты проверяют два pool, 20 конкурентных повторов двух успехов,
перезапуск/потерянный вызывающим ответ, конфликт message ID, точный bigint ID,
late success до/после takeover, сохранение отмены, чужие попытки и deferred
commit failure на report с rollback всех изменений. Потеря ответа моделируется
отбрасыванием успешного результата и восстановлением через другой pool; сетевой
разрыв PostgreSQL после COMMIT отдельно не инъецируется.

## Возможная доставка (NTF-007)

Миграция `notifications/006_delivery_uncertain.sql` добавляет immutable
`delivery_uncertainties` и отдельный `delivery_uncertain_reports`. Идентичность
отчёта — (attempt ID, uncertain), отдельно от (attempt ID, success): успех может
прийти позднее. Reporter NTF-014 должен читать обе таблицы отчётов через join с
attempt/job; исход SUB-020 — `uncertain`. Порядок доставки отчётов не должен
понижать уже подтверждённый успех. Эти таблицы не являются API delivered.

`SaveUncertain(ctx, lease, apiAttemptID, next)` вызывается для Unknown от sender
либо восстановления зарегистрированной попытки без durable исхода. Один commit
сохраняет свидетельство и pending report. При живом совпадающем lease job
переходит в `uncertain`, получает next_attempt_at, освобождает lease, claim
становится completed. Срок округляется вверх до микросекунды; первая запись
должна иметь будущий срок. Повтор известного исхода идемпотентен, сохраняет
исходное расписание и ack отчёта, даже если новый переданный срок уже прошёл.
Неизвестный/чужой attempt, claim или fence дают ErrStale. Регистрация одного
claim не разрешает повторять HTTP ни после Unknown, ни после ошибки commit.
При ошибке БД повторять тот же SaveUncertain, а не Send.

Политика возможного повтора: после Unknown использовать фиксированный
`retry.Policy.WaitCondition(now, failures).NextAttemptAt` с уже настроенным
ConditionInterval. Неопределённость сама по себе не является доказанной временной
ошибкой 429/5xx и не увеличивает их бюджет; прежний бюджет не сбрасывается.
Если бюджет уже исчерпан, этот путь не возобновляет автоматическую отправку:
worker должен сохранить uncertain и остановить работу по протоколу blocked
(NTF-008/014/012), а не передавать нулевой deadline этому методу.
Наступление срока разрешает новую **проверку**, а не HTTP: оба лимитера, новый
request ID/API-допуск с текущей ценой, новый attempt ID. При wait_condition —
Park того же workflow без отправки; старое разрешение не используется.

После crash и expiry Claim возвращает `*UnresolvedAttempt`, если старый claim
зарегистрировал attempt без durable исхода. Ошибка содержит исторические
Lease/AttemptID для SaveUncertain; это не новый lease и не разрешение HTTP.
Caller сначала восстанавливает запись uncertain с будущим сроком проверки,
затем повторяет Claim по обычному расписанию. Claim проверяет сохранённый срок
старой попытки даже когда старый lease уже истёк. Crash до регистрации попытки
не создаёт неопределённость: по контракту отправка до регистрации запрещена.
Для следующих исходов NTF-008/014 проверка unresolved должна учитывать также
их durable свидетельства; реализованы success/uncertain/blocked/retryable.

Исторический SaveUncertain не изменяет истёкший/чужой lease, отмену или успех.
Историческое свидетельство сохраняется вместе с обязанностью отчёта; после
успеха той же попытки новая неопределённость уже не создаётся. Поздний успех
после uncertain сохраняется через SaveSuccess, включая отмену/expiry/takeover;
свидетельство успеха запрещает новые отправки локальным протоколом. Уже
выполняющийся HTTP отозвать нельзя.

`Lease.PossibleDuplicate` сохраняет видимый риск для workflow при последующих
claim/ожиданиях, trigger_id остаётся прежним. `UncertainAttempts(ctx)` возвращает
число durable неопределённых попыток без двойного учёта повторных записей.
Это метрика **возможной** доставки, не число доказанных внешних дублей. Для
OPS-003: экспортировать как общий DB gauge, не суммировать копии реплик;
ретенция/удаление исторических свидетельств требует отдельного решения.
Экспорт endpoint и worker/report транспорт здесь не реализованы.

Тесты используют реальный PostgreSQL, два pool, fake clock и HTTP-сервер,
который читает запрос и обрывает ответ. Проверяются restart/потерянный вызывающим
ответ, concurrent replay, ack отчёта, recovered deadline, ожидание без нового
HTTP, поздний успех, отмена, неверные идентичности, метрика и deferred commit
rollback. Путь Park моделирует уже полученный wait_condition; полный API/worker
сценарий проверяется в NTF-012. Exactly-once и отсутствие внешних дублей не
обещаются. Рабочая БД и реальные сообщения Telegram не затрагиваются.

## Блокировка доставки (NTF-008)

`PermanentReason(error)` принимает безопасный `telegram.Error`: Forbidden и
allowlisted `InvalidChat` → `channel_unavailable`; прочий Rejected →
`permanent_failure`. Sender выставляет InvalidChat только для кода 400 с точным
`Bad Request: chat not found`; неизвестные описания не считаются доказательством
проблемы канала. Raw description, token, chat и текст не записываются. Unknown,
NotSent, 429/5xx этим методом не блокируются. `retry.Plan.Exhausted` caller
передаёт как `attempts_exhausted`; перепривязка не снимает две последние причины.

`SaveBlocked(ctx, lease, attemptID, reason, bindingVersion)` сохраняет immutable
`delivery_blocks` и `delivery_blocked_reports` одной транзакцией. Версия — из
API-разрешения этой попытки (int64 как в SUB-018), не из входного события или
последующего lookup. Caller обязан сохранить permission для восстановления;
метод проверяет локальную регистрацию/claim/fence, не удалённое разрешение.
Ключ исхода — (attempt ID, blocked). Reporter NTF-014 читает reason/version через
join с attempts/jobs, отправляет SUB-020 `blocked` и reason до ack; API берёт
каноническую версию из собственного сохранённого разрешения по attempt ID. Обязанность отчёта
сохраняется при истёкшем lease, takeover и отмене; повтор не возвращает ack в
pending, конфликт причины/версии даёт ErrConflict. Ошибка commit требует повтора
того же сохранения, а не HTTP.

Живой совпадающий lease переводит job в `blocked`, сохраняет безопасную причину
и версию, ставит next_attempt_at=infinity и очищает lease; claim completed.
Job/body/workflow сохраняются. Никакие таблицы API и пользовательский слот не
изменяются: каноническая блокировка/занятый слот — контракт SUB-020, транспорт —
NTF-014. Исторический отказ не меняет текущую job/аренду. При expiry без takeover
Claim распознаёт durable отказ прежней попытки и останавливается, не восстанавливая
его как uncertain. После takeover старый отказ не блокирует новую аренду.
Даже после SaveUncertain, уже очистившего lease_id, блок текущего fence
останавливает Claim: исчерпанный бюджет не запускает новую отправку. Для
восстановления ScheduleRecovery читает также свидетельство текущего fence, если
исторический отказ оставил поля job без изменения. Новый блок после известного успеха не создаётся; поздний success сохраняется
отдельно и запрещает дальнейшие попытки независимо от локального состояния.
SaveUncertain после durable блока той же попытки не создаёт неопределённость.

`ParkBlocked(ctx, lease, reason, bindingVersion)` применяется к ответу API
`blocked` без разрешения отправки, включая отсутствие привязки. API уже сохранил
канонический результат; локальная транзакция паркует job без регистрации
фиктивной попытки и без нового outcome report. Требуется живой lease без attempt.
Immutable `delivery_block_parks` по claim ID позволяет повторить потерянный ответ
после restart, не меняя более новое состояние. При наличии attempt — ErrConflict;
его исход должен пройти SaveBlocked/SaveSuccess/SaveUncertain.

NTF-015 назначает durable проверки восстановления канала; API разрешает новый
цикл при новой версии; бесконечные Telegram-повторы здесь остановлены. NTF-012 соединит
API-допуск, оба лимитера и HTTP; NTF-014 передаёт результаты. Миграция 007 не
добавляет цену блокировки, не удаляет job, не меняет порог/слот и не реализует
автоматическую перепривязку. Реальный Telegram не вызывается тестами.

## Доказанный временный отказ (NTF-016)

Миграция `notifications/008_delivery_retryable.sql` добавляет immutable
`delivery_retryables` и отдельный `delivery_retryable_reports`. Ключ исхода —
(attempt ID, retryable). Свидетельство хранит только безопасные kind
`rate_limited`/`server`, retry_after_seconds, binding_version и первый сохранённый
`retry.Plan` (failures/exhausted/next_attempt_at), без текста Telegram или token.
Runtime имеет только SELECT/INSERT на свидетельство; ack меняется в report.

`SaveRetryable(ctx, lease, attemptID, cause, policy, entropy, bindingVersion)`
возвращает сохранённый `retry.Plan` только после commit. Принимает типизированный
`telegram.Error` RateLimited/Server (в том числе wrapped), явно настроенную
`*retry.Policy` и binding version из API-разрешения. План рассчитывается готовым
NTF-006 **под job lock**, с SQL clock и числом durable retryable-свидетельств
этого trigger_id, до вставки нового исхода. Таким образом, два исхода не могут
рассчитать бюджет по одному прежнему значению. Это локальная SQL-операция без
HTTP/API-вызовов. Entropy используется один раз; production defaults не заданы.

Один commit сохраняет свидетельство, pending report, план и (только для живого
совпадающего lease) state=retryable/next_attempt_at, завершение claim и очистку
lease. Deadline округляется вверх до микросекунды, включая возвращённый Plan.
Повтор с теми же kind/retry_after/version возвращает первый план даже после его
срока или смены entropy/policy; ack, jitter и бюджет не меняются. Другие
kind/retry_after/version дают ErrConflict. Чужая/незарегистрированная корреляция
attempt/claim/trigger/fence даёт ErrStale. Неподдерживаемая ошибка, nil policy,
отрицательные retry_after/version отвергаются. Ошибка расчёта Policy (например,
непредставимый retry_after) возвращается явно без расписания: caller сохраняет
диагностику, останавливает работу и не превращает её в разрешение HTTP.

`Lease.Failures` — число durable доказанных временных ошибок workflow. Claim
читает его заново из append-only свидетельств; неизвестность и парковки условия/
лимитера его не увеличивают и не сбрасывают. Дубль intake не меняет свидетельства
или расписание. Ретенция этих строк без отдельного согласованного переноса
счётчика запрещена. Исторический retryable после expiry/takeover/отмены/успеха
сохраняется для API и бюджета, но не изменяет ни одно поле текущей job/claim.
Известный retryable исключает восстановление как uncertain; при восстановлении
истёкшего lease Claim соблюдает его сохранённый срок. Поздний доказанный отказ
после uncertain также удерживает расписание текущего fence, даже если lease уже
был очищен. Наличие success любого attempt запрещает дальнейшие отправки,
включая после позднего retryable. Уже выполняющийся внешний HTTP не отзывается.

При `Plan.Exhausted` та же транзакция сохраняет готовые `delivery_blocks` и
`delivery_blocked_reports` с attempts_exhausted/version. Живой lease завершается
через существующий blocked-протокол. Историческое исчерпание останавливает Claim
текущего fence без изменения истёкшей аренды. Между записью последней ошибки и
блокировкой нет отдельного вызова или crash-окна. Retryable-report и blocked-report
имеют отдельные ключи/ack; доставка API в любом порядке не должна понижать
blocked или success. SUB-026 исправил применение blocked после первого
retryable/uncertain той же попытки в SUB-020. Повтор SaveBlocked с той же причиной/
версией идемпотентен; другое доказанное постоянное утверждение той же попытки
конфликтует с созданием retryable.

Для NTF-014: читать `delivery_retryable_reports.pending` через join
retryables → attempts → jobs, передавать SUB-020 `retryable` по тому же attempt ID,
после успешного RPC подтверждать только этот report. При исчерпании также
передавать отдельный `blocked` attempts_exhausted. Успех имеет приоритет.
Для NTF-012: до HTTP RegisterAttempt, после 429/5xx SaveRetryable; при SQL-ошибке
повторять тот же SaveRetryable, а не Send (commit мог состояться). После срока
нужны оба лимитера и **новый** API-допуск по текущей цене с новым attempt ID.
Claim и Plan никогда сами по себе не разрешают Send. Reporter/worker/runtime
подключаются отдельно.

SQL/race-тесты: 429/503 через loopback HTTP, два pool/20 конкурентных replay,
restart/сохранение первого jitter и ack, durable бюджет через wait_condition,
лимитер и uncertain, expiry recovery без UnresolvedAttempt, late refusal после
takeover/cancel/success, поздний success, исчерпание живой/истёкшей аренды без
отдельной блокирующей операции и deferred commit rollback всех записей.
Lost response моделируется повтором committed операции через другой Store/pool;
сетевой разрыв PostgreSQL после COMMIT отдельно не инъецируется. Intake-тест
проверяет отсутствие сброса бюджета/плана при дубле. Команда — тот же полный
`GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py`.

Известное ограничение SQL-прав ранее созданных таблиц 005–007: default privileges
init.sh оставляют runtime UPDATE/DELETE, несмотря на append-only контракт методов.
Исправление отдельной новой миграцией — [NTF-017](../../../docs/backlog/tickets/NTF-017.md).
Новые таблицы 008 отзывают default-права явно; реальный SQL-тест подтверждает
отсутствие UPDATE/DELETE у delivery_retryables.

## Проверка восстановления канала (NTF-015)

`ScheduleRecovery(ctx, triggerID, currentFence, interval)` назначает durable
`next_check_at` тому же job. Интервал конфигурируется caller: 1 с..1 ч, целые
микросекунды; фиксированного интервала достаточно, для backoff caller увеличивает
его в этих границах. Под job lock сохраняются причина `channel_unavailable`,
версия и первый срок; повторы, несколько реплик и потерянный ответ commit не
сдвигают срок. Lease/лимитер при ожидании не удерживаются; HTTP-бюджет не меняется.
Другие причины, terminal и известный success недоступны; несовпадающий fence —
`ErrStale`, живой lease — `ErrUnavailable`.

Проверяется также immutable `delivery_blocks` текущего fence: поздний отказ после
uncertain/expiry может не менять job metadata. Scheduler должен периодически
обнаруживать **не назначенные** channel blocks, в том числе после crash между
ParkBlocked/SaveBlocked и ScheduleRecovery. Для discovery соединить jobs с
attempts по `(trigger_id,lease_fence=fence)` и blocks по attempt_id, взять
`COALESCE(block.reason,job.blocked_reason)='channel_unavailable'`,
`next_check_at IS NULL`, известные нетерминальные состояния, отсутствие живой
аренды и success. Передать текущий fence в ScheduleRecovery; гонки повторяются
следующим проходом, старый fence не получает новое назначение. Это локальное
чтение БД notifications. Worker loop/выборка — отдельно, NTF-012/013.

Назначенные проверки выбираются по `state='blocked' AND next_check_at <= now`
(частичный индекс миграции 009), без зависимости от prices. На сроке обычный
`Claim` получает новый fence/claim; первая реплика выигрывает, остальные получают
ErrUnavailable. Claim очищает текущую block metadata/срок и сбрасывает локальный next_attempt_at
в -infinity, чтобы crash после claim не оставил job недоступным; immutable свидетельства
и pending reports остаются. Это только локальная аренда: **HTTP запрещён без
нового допуска SUB-018**, даже при неизменившейся привязке. API хранит каноническую
причину/версию и решает, изменился ли канал. Каждый цикл использует новый request
ID; потерянный ответ API восстанавливается прежним request ID.

Handler NTF-012 после результата API:

- та же версия/нет привязки → ParkBlocked и ScheduleRecovery с новым fence;
- новая версия, ложное условие → Park(WaitCondition, deadline) того же job;
- новая версия, true → обычные лимитеры и новый непосредственный допуск/attempt;
- временная недоступность → Park с будущим сроком, без HTTP/сброса бюджета;
- terminal → обычная остановка handler; ScheduleRecovery её не снимает.

Порог/workflow/слот остаются в API; эта операция к ним не обращается и Telegram
не вызывает. Старые отрицательные результаты/квитанции не откатывают новый fence.

## Discovery (NTF-013)

`Discover(ctx, after, limit)` возвращает до limit (1..1000) `Work` по
возрастающему trigger_id > after. SQL clock совпадает с lease-протоколом.
Пустая страница означает возврат cursor к началу после паузы. В выборке due
jobs/checks, pending reports четырёх видов независимо от state и не назначенные
current-fence channel blocks без живой аренды/успеха. `RecoveryFence > 0`
требует ScheduleRecovery вместо Handle; это подсказка, а не владение.
Claim/ScheduleRecovery повторно проверяют гонки. Worker/ограничения, deadlines
и сохранение PendingWrite — [контракт цикла](../worker/README.md).
