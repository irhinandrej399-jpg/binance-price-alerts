# API fixture продолжения notifications — OPS-028

Это инфраструктурный setup для OPS-026, без новых endpoint, миграций и измерений.
Код: `internal/tools/apimeasure/notifications_fixture_test.go`. Он компилируется
в тот же test executable, что существующие фазовые drivers OPS-024/025.

## Вход и provenance

- `OPS028_SOURCE`: `docs/measurements/OPS-024/run-a2/identities.json`.
  SHA256 **60187b1c96bab46ddc9009a6853e43c0780099659470c60fdfd67a97fcd85106**
  закреплён в loader; чужие bytes отвергаются до SQL.
- `OPS028_MATCHER`: `docs/measurements/OPS-025/run-a3/identities.json`.
  Проверяются source_sha256 и все четыре пары candidate owner/subscription →
  workflow result trigger/version. Это связь с завершённой работой matcher,
  а не разрешение считать его frozen journal текущей API БД.
- Используются четыре исходных AlertTriggered и все пять сохранённых
  read_committed records, включая физический дубль. Event/owner/subscription/
  trigger, ключи, payload bytes и исходные partition/offset не изменяются.
  Для проверки intake читает эти сохранённые records через `Store.Accept`;
  новый Kafka broker не запускается. В OPS-026 при публикации на новый broker
  его новые offsets нужно сохранять отдельно от исходных.

## Две несовместимые границы

`reports`: каноническая строка **каждой** из четырёх подписок берётся из её
сохранённого admitted response. У первой version=4 (перед этим был wait),
у остальных version=3; state=sending, workflow version=1, fence=1.
Это набор индивидуальных срезов сразу после допуска и до исхода, а не один
исторический PostgreSQL snapshot с общим временем/C. Четыре исходных permit
импортируются с точными response bytes, request/attempt ID, fence, expires_at,
chat_id и binding_version. Их сроки остаются прошедшими. Admission replay
возвращает историческое разрешение с тем же сроком; оно не допускает HTTP.

`handler`: все четыре строки на границе workflow version=1,
subscription version=2, pending_delivery. Исходные permits здесь **отсутствуют**:
это отдельная чистая БД до их выдачи. PrepareRecipient → AdmitDelivery с CAS
использует реальный mTLS API и локальный Quotes adapter со свежим временем,
ценой 101 и текущим адресатом. Request `OPS028-new-<trigger>` создаёт новый UUID
attempt; его связь с исходными event/trigger/attempt сохраняется в evidence.
Fence=1 принадлежит этому новому dataset и не является продолжением fence
исторического permit. Допуск проверяется, HTTP/Telegram не вызывается.
Для измерения самого handler в OPS-026 этот проверочный admission не нужно
выполнять во время setup: начать с результата `importAPI(...,"handler")`.

Обе границы импортируют user с исходным ID, chat=24001/version=1 и квотой 4,
четыре подписки с quota_reserved и исходный outbox. Email/password_hash — явно
синтетические служебные значения fixture, не восстановленные credentials.
Пятый исходный ID (отменённая подписка без workflow) не является delivery work
и не импортируется; его bytes остаются в исходном evidence.
Creation fields берутся из полного Record admission, не из усечённого journal.
Финальные results/cancel/rebind/relay state, sessions и снимки не импортируются.
Journal начинается с C=0 нового dataset; source C=19 не используется как его
исторический baseline. Последующие настоящие RPC пишут новый journal обычным
production кодом. Полную историю этих БД fixture не восстанавливает.

Импорт атомарен и требует пустых API users/subscriptions/admissions/journal/
outbox; повторный setup отвергается. Неизвестная граница, ошибка SQL,
несоответствие идентичностей и source hash не дают частичного импорта.
Новых SQL-прав нет: setup через api_runtime, notifications только через
notifications_runtime в своей БД. Проверка SQL API в verifier принадлежит
владельцу fixture; reporter не получает API pool.

## Проверка и evidence

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race \
  ./internal/tools/apimeasure -run '^TestNotificationFixtureSource$' -count=1
GOCACHE=/tmp/binance-go-build python3 scripts/check_notifications_fixture.py \
  docs/measurements/OPS-028/my-verification
```

Вторая команда использует только фиксированный disposable Compose project
`binance-ops028-check`, loopback:55448, текущие миграции API/notifications без
расширений прав. Для каждой границы два раза создаётся новый volume; `finally`
удаляет контейнер/volume и проверяет их отсутствие по точной project label.
Рабочий `binance-storage` не адресуется. Каталог результата должен быть новым.

Reporter проверяет четыре категории: succeeded/retryable/uncertain/blocked.
Исходы — новые контролируемые наблюдения для проверки fixture, не утверждение
об исторических HTTP-запросах. Jobs принимаются из исходных bytes, исходный
attempt регистрируется через jobs.Store. API реально commits первый результат,
а обёртка того же Store seam, что `resultrpc/reporter_test.go`, возвращает
DeadlineExceeded. Новый reporter с новым notifications pool повторяет ту же
обязанность по mTLS, затем pending пуст. SQL проверяет state/version/binding,
квоту и точные bytes разрешений. Внешние Binance/Telegram не вызываются.

## Подключение к OPS-023 protocol driver

Из нового notifications driver в том же Go test package, в обработке фазы
`setup` (до первого измерительного snapshot), вызвать:

```go
f := fixtureSource(t) // читает OPS028_SOURCE / OPS028_MATCHER, валидирует hash/IDs
api := fixturePool(t, "DATABASE_URL", "api_runtime")
check(t, f.importAPI(ctx, api, "reports")) // или "handler" в отдельном прогоне
```

DSNs приходят из существующего `measure_joint.py` runtime environment.
Затем зарегистрировать готовые Admission/Results servers API по образцу
`fixtureClients`, а notifications отдать только его pool и mTLS clients.
Для reports использовать `f.Items` (исходные permits) и `f.Records`; для handler
только исходные work records, новые permissions получать production handler.
В `verification` вывести source hash, boundary, исходные ID и bytes/hash,
новые attempt mappings и фактические результаты. Python adapter подключается
через обычный JSONL `phase_id/status` протокол OPS-023, как `api_adapter.py`.
Сам notifications manifest/driver, физические snapshots и таблица стоимости
остаются OPS-026. Fixture не добавляет стоимость и не квалифицирует bounds.
