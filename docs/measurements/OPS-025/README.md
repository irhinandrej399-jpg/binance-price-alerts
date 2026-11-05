# OPS-025 — физические окна matcher

5 ноября 2026. Исходный commit проекта: `9b02a19` (OPS-024). Итоговый SHA этого
тикета определяется через Git log. Точные исходники, схемы, миграции, версии и
digests образов, durability, команды и времена сохраняются в report каждого run.

Протокол, команды, таблицы и границы: [MATCHER.md](../../../infra/measure/MATCHER.md).
Источник — только [OPS-024/run-a2](../OPS-024/run-a2/identities.json), с SHA256 в
manifest и evidence. Продолжения разных исходных опытов не смешиваются.

## Что измеряется

26 именованных окон: empty; загрузка/повтор пустого снимка; SQL assignment/повтор;
Initialize/повтор; mTLS replay/повтор; Kafka-дубль уже сохранённого SQL события;
первая новая версия и физический дубль; два диагностических Walk; оставшиеся
create; Accept/Record/Complete pricework и их повторы; 14 новых after-images;
старая версия; takeover, запрещённая старая запись и revoke.

Setup создаёт API snapshot C=0, импортирует исходный журнал через API-owner
adapter и публикует read_committed префикс. Исходные bytes проверены после импорта
и после чтения Kafka. Это подготовка замороженного источника событий, без
восстановления canonical бизнес-строк API. Снимок реально пустой; непустые
snapshot_rows не объявляются покрытыми. Затраты setup не входят в стоимость matcher.

Matcher пишет через matcher_runtime; snapshot/replay читает через штатные mTLS
клиенты. Свой SQL читает диагностически, API SQL остаётся у API-server/setup.
19 исходных событий и один физический дубль сохраняют event/subscription/owner,
версии, trigger_id, исходные offsets и новые disposable Kafka positions.
Work сохраняет исходную candidate quote и четыре canonical результата OPS-024;
новый RequestDelivery и полный handler здесь не исполняются.

## Как читать стоимость

Relations суммируют все forks/индексы указанной БД; WAL — **кластерное** окно,
его нельзя целиком приписать matcher. В фазе replay_first есть отдельные API
pin/issued/ack записи. Market-data источник не запускается. Kafka сохраняет
apparent/allocated bytes всех файлов, включая internal topics/metadata и фон;
его рост не означает публикацию matcher. Empty не вычитается. Diagnostic Walk
имеет отдельные окна. Operation/background/checkpoint/inclusive deltas остаются
раздельными. Нулевой relation growth не означает отсутствие WAL/стоимости UPDATE.

Сводка всех чисел — [summary.json](summary.json); raw обоих collectors, schema dumps, manifests,
исходные ID/bytes и resolved bindings остаются в каталогах итоговых опытов.

## Ограничения

`qualified=false`. Конечные bounds ряда строк/кандидатов/полного snapshot и
транзакций отсутствуют. Fixture содержит один event в транзакции, четыре
кандидата, ноль snapshot rows, короткую цену 101. Не покрыты максимальные
размеры/TOAST, непустые страницы, полный coordinator/group/handler, новый
RequestDelivery, price Prefix/OffsetCommit, superseded/restart/ошибки/rollback.
Unsupported PostgreSQL temp peak/device IO и Kafka fsync сохраняются как
unsupported, не как нулевая стоимость. OPS-018 не может квалифицировать эти
данные как полную стоимость или расчёт резерва 2×. Нагрузочный fixture и полный
race suite не запускались; Telegram/Binance не вызывались.

Проверка повреждённых копий отчёта воспроизводится командой
`python3 docs/measurements/OPS-025/format-check.py`; исходный raw не меняется.

## Итоговые опыты

[run-a3](run-a3/report.json) и [run-b3](run-b3/report.json) — exit 0. Оба
полных прогона содержат все 26 окон и подтверждённый cleanup containers/volumes/networks.
Verifier проверил raw/дельты/ID/bytes и различие disposable projects/prefix.
Исходные hashes обоих отчётов совпадают с текущими исходниками.

Команда `python3 scripts/verify_matcher_measurement.py docs/measurements/OPS-025/run-a3 docs/measurements/OPS-025/run-b3` — exit 0.

В обоих опытах durable C=19, NextOffset=39, пять полных after-images
совпадают с последними версиями источника. Диагностические Walk до/после
физического дубля сохраняют C=2 и одинаковые images; NextOffset растёт 3→5.
Pricework completed с четырьмя canonical trigger_id, token=2 и active=false
после revoke. Снимок durable complete, C=0, Count=0.

[Resolved A](run-a3/resolved-manifest.json), [Resolved B](run-b3/resolved-manifest.json)
связывают окно, event/subscription/trigger, версию, C, source offset, новый offset
и wire bytes. Исходный журнал: 12862 bytes, диапазон 619–708 bytes/event.

Выборка байт за **полную фазу**, A3 / B3, без нормализации и вычитания empty:

| Фаза | Matcher relations | API relations | Cluster WAL | Kafka log |
| --- | ---: | ---: | ---: | ---: |
| empty | 0 / 0 | 0 / 0 | 238584 / 184 | 432 / 432 |
| snapshot_load | 40960 / 16384 | 0 / 0 | 186176 / 920 | 432 / 432 |
| snapshot_repeat | 0 / 0 | 0 / 0 | 480 / 480 | 432 / 360 |
| assign | 57344 / 57344 | 0 / 32768 | 2648 / 241912 | 432 / 432 |
| initialize | 16384 / 16384 | 0 / 0 | 1928 / 1984 | 432 / 360 |
| replay_first | 16384 / 16384 | 0 / 0 | 3608 / 3552 | 432 / 360 |
| replay_repeat | 0 / 0 | 0 / 0 | 568 / 568 | 432 / 432 |
| live_first | 0 / 0 | 0 / 0 | 2048 / 2048 | 432 / 432 |
| live_physical_duplicate | 0 / 0 | 0 / 0 | 912 / 912 | 432 / 432 |
| work_accept | 40960 / 40960 | 0 / 0 | 3296 / 3296 | 432 / 432 |
| work_results | 0 / 0 | 0 / 0 | 3616 / 3616 | 432 / 360 |
| work_complete | 0 / 0 | 0 / 0 | 5384 / 5384 | 432 / 432 |
| live_tail | 0 / 0 | 0 / 0 | 12760 / 12760 | 432 / 432 |
| takeover | 0 / 0 | 0 / 0 | 2960 / 2960 | 432 / 432 |
| stale_write | 0 / 0 | 0 / 0 | 976 / 976 | 360 / 432 |
| revoke | 0 / 0 | 0 / 0 | 1056 / 1056 | 432 / 432 |

[Проверка формата](format-check.json): восемь повреждений отвергнуты — missing phase, unknown numeric, одинаковые physical offsets, чужой source hash, изменённый after-image, неполный snapshot, чужой canonical trigger и изменённая диагностика дубля.

## Контрольные и неуспешные опыты

run-a — Docker запрещён sandbox до опыта. run-a1 — успешный контрольный проход
до добавления отдельных диагностических окон и точных счётчиков вызовов;
не используется как итоговый повтор. run-a2 — failed из-за исчезновения
Kafka temporary checkpoint во время описи. Оба cleanup подтверждены; точный
report сохранён для [OPS-027](../../backlog/tickets/OPS-027.md).
Эти данные не подменяют итоговую пару.

В B3/assign дополнительные 32768 B API относятся только к `pg_catalog/r/main`,
а не к бизнес-записям API. В A3/snapshot_load дополнительные 24576 B matcher
также относятся к `pg_catalog/r/main`; app heap/index дают 16384 B в обоих.
Empty WAL 238584 / 184 B сохранён без вычитания. Эти различия не являются
оценкой новой бизнес-операции или гарантированного максимума.
