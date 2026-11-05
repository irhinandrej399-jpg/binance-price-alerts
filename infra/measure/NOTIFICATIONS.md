# Продолжение API в notifications — OPS-026

Два manifest v1: `notifications-reports.json` и `notifications-handler.json`.
Они используют OPS-023 без изменения collectors/runtime. Общий driver —
`internal/tools/apimeasure/notifications_test.go`, wrapper —
`notifications_adapter.py`. [API fixture OPS-028](NOTIFICATIONS_FIXTURE.md)
остаётся setup до baseline; две границы требуют отдельных чистых volumes.

## Воспроизведение

Из корня проекта, Docker и локальный Go toolchain, последовательно:

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/measure_joint.py --output docs/measurements/OPS-026/reports-a --manifest infra/measure/notifications-reports.json --timeout 120 --command python3 infra/measure/notifications_adapter.py docs/measurements/OPS-026/reports-a/identities.json reports
GOCACHE=/tmp/binance-go-build python3 scripts/measure_joint.py --output docs/measurements/OPS-026/reports-b --manifest infra/measure/notifications-reports.json --timeout 120 --command python3 infra/measure/notifications_adapter.py docs/measurements/OPS-026/reports-b/identities.json reports
GOCACHE=/tmp/binance-go-build python3 scripts/measure_joint.py --output docs/measurements/OPS-026/handler-a --manifest infra/measure/notifications-handler.json --timeout 120 --command python3 infra/measure/notifications_adapter.py docs/measurements/OPS-026/handler-a/identities.json handler
GOCACHE=/tmp/binance-go-build python3 scripts/measure_joint.py --output docs/measurements/OPS-026/handler-b --manifest infra/measure/notifications-handler.json --timeout 120 --command python3 infra/measure/notifications_adapter.py docs/measurements/OPS-026/handler-b/identities.json handler
python3 scripts/verify_notifications_measurement.py docs/measurements/OPS-026/reports-a docs/measurements/OPS-026/reports-b docs/measurements/OPS-026/handler-a docs/measurements/OPS-026/handler-b
```

Каждый output должен отсутствовать. Исходные source/matcher paths закреплены
wrapper; loader проверяет hash и четыре canonical workflow OPS-025/run-a3.
Сохраняются все пять исходных delivery records (четыре события и дубль).
Setup публикует их точные bytes на новую Kafka, проверяет read_committed и
фиксирует новые offsets отдельно от исходных partition/offset. Публикация
источника не входит в measured notifications cost; её исходные окна — OPS-024.
В SQL intake используются новые offsets из последовательно прочитанного
префикса. Broker group consumer/OffsetCommit здесь не вызывается, это пробел.

## Матрица записей и окон

Все SQL operations notifications выполняются через `notifications_runtime`.
API-owned setup/servers/verifier используют только `api_runtime`, handler и
reporter получают лишь mTLS clients. Служебные диагностические чтения явно
помечены; Telegram — `httptest` loopback, token синтетический.

| Класс | Окно | Обязательные записи / граница |
| --- | --- | --- |
| Пустой контроль | empty | нет adapter-записей; фон collectors остаётся |
| Назначение SQL cursor | checkpoint | delivery_checkpoints |
| Первое применение | intake_first/intake_second/intake_tail | delivery_jobs + checkpoint одним commit; tail две записи |
| SQL replay | intake_repeat | повтор offset ниже durable cursor, job не сбрасывается |
| Физический дубль | intake_physical_duplicate | тот же event/bytes на новом offset; job конфликтует, checkpoint растёт |
| Контроль durable intake | observe_intake | SELECT job count и checkpoint, отдельное окно |
| Выборка работы | discover | production jobs.Discover, четыре подсказки, без lease |
| Аренда и регистрация | claim_register_0..3 (reports) | job lease/fence + delivery_claims, затем delivery_attempts, два commit |
| Исход и обязанность отчёта | outcome_0..3 | successes/retryables/uncertainties/blocks + отдельные *_reports + job/claim; retryable хранит budget/plan |
| Повтор сохранённого исхода | outcome_repeat_0..3 | те же lease/attempt/arguments, первое расписание/бюджет сохраняются |
| API commit без ответа | report_loss_0..3 | настоящий result RPC commits API result/subscription/quota/journal; seam возвращает DeadlineExceeded, notifications ack отсутствует |
| Replay и ack | report_replay_0..3 | новый notifications pool/reporter, тот же attempt/outcome по mTLS, отдельный pending=false |
| Пустой повтор reporter | report_empty_0..3 | подтверждённый исход не передаётся вновь |
| Назначение проверки | recovery_schedule/recovery_repeat | next_check_at/metadata, повтор не сдвигает первый срок |
| Claim наступившей проверки | recovery_claim | новый fence/claim того же job, старые outcome/reports сохранены |
| Повторная парковка | recovery_park | delivery_block_parks + blocked job, без фиктивного attempt/report |
| Ограниченная волна worker | worker_sweep | реальный Discover/ScheduleRecovery: concurrency=4, interval=1s, context=100ms; назначение потерянной проверки fence=2, без HTTP |
| Настоящий handler | handle_0..3 (handler) | Claim → PrepareRecipient → bot/chat limits → CAS AdmitDelivery → RegisterAttempt → один HTTP → Save* → report с потерей ответа |
| Повтор PendingWrite | handle_retry_0..3 | тот же outcome/reporter, без Claim/HTTP; для channel block также идемпотентный ScheduleRecovery |

Индексы outcome: 0=succeeded, 1=retryable, 2=uncertain, 3=blocked
channel_unavailable. В reports это контролируемые наблюдения над исходными
attempts; истёкшие permits не используются для отправки. В handler это
настоящие success/429/disconnect/403 локального HTTP; новые request/attempt ID
явно связаны с исходным event/trigger. Источник fixture — четыре индивидуальных
admitted cuts либо workflow v2; новый API journal C=0, не source C=19.

В handler окно API и notifications совместное по времени, но SQL relation/fork
дельты разделены по владельцам. WAL кластерный: его нельзя приписать одному
владельцу или вычесть setup повторно. API-only окна report_loss/replay содержат
также локальные чтения/ack; это измерения полной указанной операции. Setup,
background_delta, operation, checkpoint и inclusive сохранены отдельно.
Empty не вычитается. Inclusive нельзя складывать с operation/checkpoint.

## Пределы и непокрытые классы

Wire intake до 8 KiB, fetch/decompression 1 MiB, response 2 MiB. Lease до 60 s,
report/discovery limit 1..1000, worker concurrency 1..1000. В опыте 4 jobs,
5 records, report limit=10, worker concurrency=4; лимитеры 25/s и chat 1 s.
Retry policy опыта: base/max/condition=1 h, MaxFailures=3; rate_limited
retry_after=3600 s. Это параметры опыта, не production defaults и не максимумы
физического расхода. Retained attempts/history, channel checks, повторные
relay/report/intake доставки не имеют конечного общего количества; однократный
резерв не покрывает бесконечную историю. Maximal bytes/batches/TOAST не измерены.

Пробелы перечислены в `qualification.uncovered` обоих manifests: broker group
intake/ack/rebalance; limiter contention/waits; condition waits; exhausted retry
и исторические исходы/takeover; late success/cancel/rebind; изменённый канал при
recovery; rollback/crash до durable outcome/failed ack; scan/worker при нагрузке.
Это блокирующие неизвестные затраты, а не нулевые записи. Матрица покрывает все
локальные таблицы notifications; варианты записи внутри класса остаются
неизмеренными. Верхняя оценка стоимости и квоты не квалифицированы.

`verify_notifications_measurement.py` проверяет общий raw verifier OPS-023,
cleanup/версии/durability, source hash/bytes/IDs/permits, новые offsets, четыре
job/attempt/outcome и отсутствие pending после replay, новые handler permissions,
API states/slots/journal, HTTP count и неизменность count при PendingWrite Retry.
Resolved manifest связывает каждый job с исходными и текущими permits.
PostgreSQL temp peak/device IO и Kafka fsync остаются unsupported.
Сводка всей цепочки: [отчёт OPS-026](../../docs/measurements/OPS-026/README.md).
