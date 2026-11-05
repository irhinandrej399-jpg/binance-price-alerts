# OPS-026 — физические окна notifications и покрытие продолжений

5 ноября 2026. Воспроизведение, фазовая матрица и пределы:
[NOTIFICATIONS.md](../../../infra/measure/NOTIFICATIONS.md).
Исходные commit/версии Go, hashes runtime/схем/контрактов, образы/digests,
команды и времена сохранены в каждом `report.json`. Собственный SHA тикета
определяется через Git log. Полный runtime и нагрузочный профиль не запускались.

## Границы эксперимента

Источник ровно [OPS-024/run-a2](../OPS-024/run-a2/identities.json), SHA256
60187b1c96bab46ddc9009a6853e43c0780099659470c60fdfd67a97fcd85106;
matcher link — [OPS-025/run-a3](../OPS-025/run-a3/identities.json).
Четыре AlertTriggered по 858 B, пять records/4290 wire B с одним физическим
дублем. Новые Kafka coordinates сохраняются отдельно от исходных.

`reports`: четыре индивидуальных admitted cuts исходного API. Original permits
сохранены byte-for-byte и остаются истёкшими; новые контролируемые наблюдения
success/retryable/uncertain/channel_block фиксируются под исходными attempt.
После настоящего API commit seam возвращает DeadlineExceeded; новый reporter
повторяет исход и отдельно снимает pending. Никакого HTTP в этой границе нет.

`handler`: отдельная чистая БД на workflow v2, без старых permits. Настоящий
handler получает нового адресата через PrepareRecipient, проходит оба durable
лимитера, CAS admission, RegisterAttempt и один локальный HTTP. Ответы
success/429/disconnect/403 дают четыре durable исхода. Потерянный ответ result
RPC оставляет PendingWrite; Retry сохраняет прежний исход/ack без второго HTTP.
Старые и новые attempts связаны в identities/resolved-manifest.

Fixture не является восстановленным историческим snapshot источника: source
journal C=19 не переносится, в каждом новом dataset journal начинает с C=0.
API setup и Kafka republish/read_committed вынесены до baseline. В измерительных
окнах сервер API владеет своим pool; notifications получает только mTLS clients
и notifications_runtime. Внешние Telegram/Binance не вызывались.

## Покрытие цепочки

[coverage.json](coverage.json) перечисляет все классы исходного OPS-021,
соответствующие фазы API/matcher/notifications, report/raw и блокирующие пробелы.
Каждая ссылка проверяется `python3 docs/measurements/OPS-026/summarize.py`.
Это продолжения одних source ID/bytes, а не сумма трёх исторических snapshots.

| Участок | Подтверждаемая граница | Что блокирует верхнюю стоимость |
| --- | --- | --- |
| API OPS-024 | create/slot, workflow/outbox/journal, cancel, prepare/admission, четыре outcomes, обе relay/lost reply/physical duplicates | все максимальные bytes/batches, варианты переходов, конечные повторы |
| Matcher OPS-025 | empty snapshot/replay, live projection/checkpoint/duplicate, fencing, pricework | nonempty pages, полный group/handler/new workflow/price Prefix, finite candidate bound, отказные варианты |
| Notifications reports | SQL intake/first/repeat/physical duplicate, claims/attempts, четыре outcome/pending, report/replay/ack, recovery/repark/worker sweep | broker group/OffsetCommit/rebalance, expiry/takeover/rollback, exhausted retry, changed channel, максимальные данные/сканы |
| Notifications handler | PrepareRecipient, оба лимитера, новый CAS admission, HTTP 200/429/disconnect/403, durable исход и PendingWrite Retry | limiter/condition wait/contended paths, поздние исходы/cancel/rebind, полный worker/load |
| Market-data | отдельная следующая OPS-022 | исходный manifest ещё не измерен |

Служебные записи claims/checkpoints/limiter schedules/report ack/recovery
учтены отдельными окнами либо явно входят в handler. Физический дубль — ещё
одна транспортная запись, он не создаёт новое логическое обязательство.
Успешный RPC/publish/offset не освобождает физическую ёмкость.

## Чтение физических чисел

[summary.json](summary.json) сохраняет по всем окнам operation/background/
checkpoint/inclusive, кластерный WAL и app relation/fork growth отдельно по
владельцам, Kafka apparent/allocated groups. Raw и schema dumps остаются в
каталогах опытов. Inclusive нельзя повторно складывать с operation/checkpoint;
empty не вычитается. UPDATE может дать нулевой relation growth и ненулевой WAL.

API и notifications одновременно пишут внутри handler/report RPC, но relation
дельты разделены по БД. WAL общий для кластера и не является чистой стоимостью
notifications. Kafka после setup содержит только фон и внутренние записи,
а не новые notifications publishes. Исходные публикации измерены OPS-024.

`qualified=false`. PostgreSQL temp peak/device IO и Kafka fsync unsupported;
они не заменены нулём. Нет доказанных конечных bounds всей retained истории,
проверок канала и повторов. Максимальные payloads/batches/TOAST и перечисленные
write variants не измерены. Эти данные не дают таблицу верхних оценок/2×,
квоты или разрешение начать OPS-015/016. OPS-027 — отдельная известная гонка
описи Kafka временных файлов; этот collector здесь не изменялся.

## Фактические результаты

Исходный commit проекта — `15a6424` (OPS-028). Все четыре опыта завершились
exit 0: [reports-a](reports-a/report.json), [reports-b](reports-b/report.json),
[handler-a](handler-a/report.json), [handler-b](handler-b/report.json).
Первые два содержат по 38 окон, последние — по 17. Во всех разные
PostgreSQL/Kafka projects/prefix, cleanup containers/volumes/networks подтверждён.
Runtime driver во всех четырёх отчётах имеет один SHA256 и совпадает с файлом
репозитория. В первом reports-a digest verifier относится к начальной версии
до исправления регистра полей Go JSON; все raw проверены окончательным verifier.

Reports: четыре исходных attempts, четыре outcome/report, после replay pending=0,
API journal=4, slots=3. Recovery получает fence=2, повторно паркует тот же job;
HTTP calls=0. Handler: четыре новых attempts, четыре HTTP, API journal=8,
slots=3, pending=0. Для каждого PendingWrite число HTTP до/после Retry одинаково.
В обеих границах пять новых read_committed records дают четыре jobs и durable
checkpoint=9; исходные offsets остаются в evidence, новые — 0/2/4/6/8.

Выборка operation bytes за полную фазу, A / B. Значения не нормализованы и
не являются верхними оценками:

| Граница / фаза | Кластерный WAL | API app forks | Notifications app forks |
| --- | ---: | ---: | ---: |
| reports / intake_first | 1712 / 1712 | 0 / 0 | 16384 / 16384 |
| reports / intake_physical_duplicate | 203848 / 2744 | 0 / 0 | 0 / 0 |
| reports / claim_register_0 | 6944 / 7000 | 0 / 0 | 65536 / 65536 |
| reports / outcome_0 | 7504 / 7504 | 0 / 0 | 32768 / 32768 |
| reports / report_loss_0 | 10552 / 10552 | 81920 / 81920 | 0 / 0 |
| reports / report_replay_0 | 4184 / 4184 | 0 / 0 | 0 / 0 |
| reports / recovery_schedule | 7960 / 7904 | 0 / 0 | 8192 / 8192 |
| reports / worker_sweep | 9376 / 9352 | 0 / 0 | 0 / 0 |
| handler / handle_0 | 17840 / 17840 | 114688 / 114688 | 131072 / 131072 |
| handler / handle_retry_0 | 10296 / 10296 | 0 / 0 | 0 / 0 |

Сильная разница WAL physical duplicate между двумя опытами сохранена как факт,
не вычтена и не заменена меньшим значением. Relation growth в обоих окнах нулевой;
снимки не доказывают принадлежность всего кластерного WAL одной операции.
Для стоимости UPDATE нулевой рост forks не означает нулевых затрат.

Общий `verify_notifications_measurement.py` по четырём каталогам — exit 0.
`python3 docs/measurements/OPS-026/summarize.py` — exit 0, все raw-ссылки
coverage существуют. `python3 docs/measurements/OPS-026/format-check.py` —
8/8 повреждённых копий отвергнуты: unknown metric, missing phase, source hash,
body bytes, physical offset, pending ack, attempt identity, HTTP count.
Результат — [format-check.json](format-check.json), исходный raw не менялся.
Go test executable компилируется; `go vet ./internal/tools/apimeasure` — exit 0.
Это инфраструктурная проверка реальных опытов; искусственных unit-тестов и
полного функционального/race suite в measured фазах нет.
