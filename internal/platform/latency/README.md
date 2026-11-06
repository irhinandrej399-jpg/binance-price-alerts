# Raw границы задержки (OPS-045)

Пакет предоставляет raw наблюдения границ (OPS-045) и агрегацию одного
контрольного опыта с histogram, проверкой полноты и p99 (OPS-002).
Подключение в runtime и нагрузочный runner остаются отдельными задачами.

## Подключение

Каждый процесс создаёт свой `latency.New(latency.HostClock, sink)`. Sink получает
`Observation` по значению; например, сериализует её JSON encoder в отдельный
raw-файл процесса под mutex. Файл и конфигурация recorder связываются с ID опыта
через manifest вызывающего runner. Sink и Clock обязаны быть concurrency-safe,
ограниченными по времени; sink возвращает ошибки записи/переполнения, а не
теряет данные молча. Синхронная запись и её overhead входят в измерение.
Не передавайте credentials в raw и не выводите ID в metric labels.

- Market-data: `storage.NewObserved(pool, recorder)`. `price_received` (`t0`)
  снимается на входе валидной контрольной цены в `Accept`, до `Begin`, ожидания
  SQL locks и commit. Контрольный источник должен передавать цену сразу в
  `Accept`; предварительная очередь/ожидание перед этим вызовом не измеряется.
  При будущем wiring transport именно этот вход должен быть границей приёма
  контрольного опыта; измерение более раннего WebSocket receipt этим не доказано.
- `price_accepted` снимается только после nil `Commit`, связывает уникальный
  `attempt_id` приёма с **назначенной durable** ревизией. `event_id`, pair,
  source_id и generation связывают raw с исходной контрольной ценой и outbox.
  Duplicate/stale/rollback/unknown commit не создают подтверждения приёма.
  Присутствие одного `price_received` само по себе не доказывает acceptance.
- API: `deliveryrpc.NewPostgresObserved(db, recorder)` вместо `NewPostgres`.
  Прямой вызов: `subscription.RequestDeliveryObserved(ctx, db, candidate, recorder)`.
  `workflow_committed` (`t1`) снимается сразу после nil `Commit` создания
  workflow, delivery_outbox и journal. Поля owner/subscription/trigger/event ID,
  итоговая subscription version и canonical candidate quote revision дают raw join.
  Кандидат может относиться к более поздней цене, чем эталонный t0.
- Старые constructors/`RequestDelivery` сохраняют поведение без наблюдений.
  Передавать timestamps через Protobuf или менять схему SQL не требуется.
  Чтение уже существующего workflow, включая конкурентный повтор под lock и
  ранний recovery в deliveryrpc, не создаёт новое t1.

## Шкала и preflight

`Stamp {scale, nanos}` сериализуется без Go `time.Time`. `HostClock` использует
kernel `CLOCK_MONOTONIC_RAW` и boot UUID/ID; OS и вид часов включены в scale.
Darwin берёт `kern.bootsessionuuid`, Linux — `/proc/sys/kernel/random/boot_id`.
Идентичность перепроверяется при каждом чтении. Нет идентичности/доступа/часов —
явная ошибка, без подмены на wall clock или локальный origin процесса.

Linux RAW не смещается time namespaces, в отличие от MONOTONIC/BOOTTIME:
[документация Linux](https://man7.org/linux/man-pages/man7/time_namespaces.7.html).
Apple описывает RAW как эквивалент continuous time:
[документация Apple](https://developer.apple.com/documentation/kernel/1646199-mach_continuous_time).
Семантика suspend различается между OS, поэтому контрольный опыт должен проходить
без сна хоста. Смешивать macOS-хост и Docker VM, разные boots/хосты/виды часов
запрещено. Boot ID — идентичность обычного kernel boot, не аттестация VM clone;
клонированные/восстановленные VM требуют отдельной проверки и сюда не входят.

До опыта runner собирает stamp каждого процесса и проверяет одинаковую scale.
Для локального запроса на чтение часов отметка другого процесса должна лежать
между parent-before/parent-after на той же шкале. Такой preflight двух отдельных
процессов с JSON round-trip проверяет `TestHostClockAcrossProcesses`.
`Between(t0,t1)` возвращает ошибку для пустой/чужой шкалы и обратного времени.
Поддержаны macOS и Linux; фактические часы macOS проверяются на текущем хосте,
Linux-сборка не означает проведённого Linux измерения.

## Контрольный эталон и join для OPS-002

Runner заранее хранит ожидаемое множество подписок и первую цену, после которой
их условие остаётся подходящим. Пример строки его отдельного oracle JSONL:

```json
{"run_id":"control-a","owner_id":"...","subscription_id":"...","pair":"BTCUSDT","first_event_id":"price-1","first_source_id":"1","generation":1,"first_attempt_id":"..."}
```

`first_attempt_id` заполняется из исходного `price_received` первой контрольной
цены; runner сохраняет его **один раз**. Для нового участка, где условие перестало
сохраняться, нужен новый явно заданный контрольный сценарий. Production matcher
не обязан собирать историю всех условий ради телеметрии.

1. Сопоставить oracle с `price_received` по attempt/event/pair/source/generation;
   найти `price_accepted` того же attempt и назначенную durable revision.
2. Найти единственное `workflow_committed` ожидаемых owner/subscription,
   сверить trigger/event с каноническим результатом workflow/outbox.
3. Считать `Between(first_received.At, workflow_committed.At)`.
   Поздняя revision в t1, новый Latest, RPC retry и recovery не меняют first t0.
   Повторный приём той же цены имеет новый attempt_id; он не заменяет исходный.
4. Потерянные, неоднозначные, чужие отметки, отсутствие принятия выбранной цены
   или t1 должны давать missing/error для ожидаемой подписки. Нельзя выбирать
   только полные пары и объявлять улучшенный p99. Подсчёт полноты — OPS-002.

Формат `Observation` версии 1: kind, at, attempt_id, pair, source_id, event_id,
generation, revision; для t1 также owner_id, subscription_id,
subscription_version, trigger_id. ID допустимы только в raw/эталоне.
`TestLatencyBoundariesOverTLS` использует готовые PostgreSQL/Latest/Delivery
loopback harness: две цены, API Latest второй цены, один workflow и RPC retry;
на управляемой шкале исходный интервал остаётся 200 ms.

## Ошибки и crash

Ошибки Clock/Sink не возвращаются как отказ уже выполненной бизнес-команды.
`Recorder.Failures()` даёт накопленный счётчик и последнюю ошибку; runner обязан
снять его **с каждого процесса**, а также проверить raw по ожидаемому oracle.
После опыта sink нужно flush/close с проверкой ошибок. Recorder не предоставляет
очередь, fsync, durable доставку или транзакцию с бизнес-БД.

Crash между commit и наблюдением оставляет missing t1/acceptance. Потерянный
commit-response может оставить durable workflow без t1: успешный recovery
возвращает workflow, но не восстанавливает время первого commit. Потеря процесса
может потерять и его счётчик ошибок. Поэтому нулевой Failures без полного raw join
не доказывает полноту. Нельзя реконструировать t1 из envelope.OccurredAt,
времени повторного RPC, Latest или wall clock. SQL/business результаты остаются
первичными, распределённая транзакция с sink не добавляется.

## Отчёт и histogram (OPS-002)

`Analyze(Input)` агрегирует **один** контрольный опыт. `Input.Expected` — заранее
зафиксированный oracle выше (без `run_id`: один файл относится к одному run).
Для каждой подписки допускается один непрерывно подходящий участок. Следующий
участок/опыт получает отдельный Input; объединять файлы разных опытов нельзя.
Идентификаторы в raw/JSON отчёте нужны для диагностики, они не экспортируются
в metric labels.

Вход дополнительно содержит:

- `workflows`: независимо прочитанные канонические результаты API/outbox **всего
  контрольного scope**, включая неожиданные подписки. Поля `Workflow` — owner,
  subscription, trigger, event ID outbox, subscription version **при создании**,
  pair/source/revision исходной candidate quote из payload. Поздний текущий
  subscription version не заменяет версию payload. Нельзя строить oracle по
  успешным workflow или копировать canonical данные из raw t1.
- `processes`: заранее определённое полное множество участвующих процессов.
  Каждая строка содержит name, накопленный `Recorder.Failures()` и `complete`.
  Только успешное получение счётчика и flush/close sink позволяет complete=true.
  При crash, неизвестном результате flush или потере процесса оставить false.
  Проверку списка процессов и preflight сопоставимости шкал выполняет runner;
  анализатор проверяет непустой список, уникальные имена, complete и failures.
- `observations`: объединённые raw v1 из sink всех процессов; порядок строк
  не важен. Каждый raw-файл/счётчик должен быть собран один раз. Дубли первых
  receipt/acceptance или commit считаются неоднозначностью, а не выбираются
  по минимальному времени. Посторонние рабочие цены допустимы; посторонние
  workflow внутри контрольного scope проваливают полноту.

Join индексируется по attempt и owner/subscription. Первая цена обязана иметь
однозначные receipt и acceptance с oracle identity и durable revision. Commit
сверяется с независимым canonical workflow, revision не может предшествовать
первой принятой цене. Проверяются одинаковые шкалы и порядок receipt ≤ acceptance
≤ commit. Новые Latest, receipt/retry и более поздняя revision не меняют первый
attempt oracle. При утрате первого t1 recovery не создаёт sample.

Результат содержит исходные samples в ns, missing по ожидаемым подпискам с
причиной, глобальные issues и inclusive cumulative buckets в секундах:
0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, **1**, 2.5, 5, 10, +Inf.
Пустой/неполный или неоднозначный oracle — ошибка входа. Missing, сбои capture,
неизвестный raw kind/version, чужие шкалы и неожиданные workflow дают
`complete=false`. Partial histogram остаётся видимым, но p50/p95/p99 отсутствуют.
При полной выборке quantiles считаются точно методом nearest-rank
(`ceil(percent*N/100)`, без интерполяции buckets); `within_one_second` требует
полноту и p99 ≤1 с. Этот флаг оценивает только задержку: отдельную burst
пропускную способность, нагрузочный fixture или полноту runtime он не доказывает.

Воспроизведение на **синтетической**, не производительной, фикстуре 200 ms:

```sh
GOTOOLCHAIN=local go run ./cmd/latency-report \
  < internal/platform/latency/testdata/control.json > /tmp/latency-report.json
GOTOOLCHAIN=local go run ./cmd/latency-report -format prometheus \
  < internal/platform/latency/testdata/control.json > /tmp/latency.prom
```

Для реального опыта runner создаёт такой же JSON Input из собственного oracle,
canonical SQL/outbox и собранных raw. CLI читает один JSON со stdin, пишет полный
отчёт либо Prometheus text. Код выхода 0 — complete и p99 ≤1 с, 1 — неполный
опыт или превышение цели (отчёт сохранён), 2 — неверный вход/ошибка вывода.
Проверять код самого собранного бинарника; `go run` оборачивает ненулевой exit.
Результат можно опубликовать как отдельный snapshot опыта через textfile
collector; cmd сервисов и live HTTP scrape этим тикетом не подключаются.

Prometheus экспортирует histogram `price_to_workflow_seconds`, gauges expected,
missing, issues, complete, within_one_second и, только для полной выборки,
p50/p95/p99_seconds с префиксом `price_to_workflow_`. Единственный label —
фиксированный числовой `le` у bucket. Не объединять per-run snapshots в один
кумулятивный live счётчик. Для проверки цели использовать complete=1 **и**
within_one_second=1; один histogram_quantile по частичным samples не даёт pass.
Queue, wait_condition и подтверждённая доставка не входят в этот histogram;
их отдельная телеметрия остаётся в OPS-003. Полный нагрузочный p99 остаётся
проверкой LOAD-003, отложенной по DEC-010.

`TestReport*` проверяют missing/ambiguity, сохранение первого t0, canonical join,
шкалы, buckets, nearest-rank/границу цели и отсутствие ID в экспорте.
`TestLatencyBoundariesOverTLS` дополнительно строит отчёт на реальных PostgreSQL
commit/outbox и mTLS RPC с управляемыми 200 ms, поздней ценой и recovery/retry.
Это проверка правильности измерителя, а не фактической производительности.
