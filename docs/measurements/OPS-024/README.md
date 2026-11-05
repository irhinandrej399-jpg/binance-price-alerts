# OPS-024 — физические окна API

5 ноября 2026. Исходная база проекта: `5fe131f` (OPS-023); точные изменения driver,
контрактов, схем и исходников фиксируются SHA256 в report каждого run. Собственный
SHA коммита результата определяется через Git log, не записывается в этот файл.

Команды, размерности входов, batch и ограничения: [API.md](../../../infra/measure/API.md).
Владелец SQL driver — `api_runtime`; чужой SQL доступен только штатным collectors.

## Итоговые опыты

Итоговая пара — [run-a2](run-a2/report.json) / [run-b2](run-b2/report.json), после
исправления revision fixture. Оба полных прогона — exit 0. Два новых чистых
PostgreSQL/Kafka volume набора и разные project/prefix проверены; cleanup каждого
не оставил containers/volumes/networks. Все 23 именованных окна + setup/verification
присутствуют; migrations/history/durability/images/source hashes и raw проверены
общим verifier. Текущие исходники совпадают с source hashes обоих итоговых report.

`python3 scripts/verify_api_measurement.py docs/measurements/OPS-024/run-a2 docs/measurements/OPS-024/run-b2`
— exit 0. На каждый run: 19 journal событий и 4 outbox; read_committed видит 20 и 5
сообщений соответственно, то есть один физический дубль каждой relay с исходными
bytes/event_id/key и разными offsets после code=0 брокера / ErrCommitUnknown клиента.
Cursor=19, delivery_published=4. Workflow/cancel/admission/result replay не создают
новых journal/outbox событий. Исходы: delivered, pending_delivery, pending_delivery,
delivery_blocked. Через API реально отправленных Telegram сообщений нет.

Устойчивые идентичности и исходные bytes для продолжения:
[run-a2 identities](run-a2/identities.json), [resolved manifest](run-a2/resolved-manifest.json);
независимый повтор — [run-b2 identities](run-b2/identities.json),
[resolved manifest](run-b2/resolved-manifest.json). Journal bindings перечисляют
position/phase/event/subscription/trigger/attempt и точный wire bytes/SHA256.
Продолжения OPS-025/026/022 явно not-measured, fixture не описывает их стоимость.

[Сводка JSON](summary.json) содержит выбор принятых/исследовательских runs и
агрегированные числовые дельты. Ниже значения A2 / B2 в байтах **за полную фазу**,
без нормализации на вызов и без вычитания empty/background. Relation включает все
forks/индексы API; WAL — кластерное operation-окно. Kafka log включает internal
.log и фон, полный apparent/allocated в JSON. Checkpoint/inclusive/background,
TOAST/forks/WAL/temp/IO и все Kafka files — в исходных collector reports/raw.
Нулевой прирост relation не означает нулевую стоимость записи.

| Фаза | API relations A2 / B2 | WAL A2 / B2 | Kafka log A2 / B2 |
| --- | ---: | ---: | ---: |
| empty | 0 / 0 | 0 / 238400 | 432 / 432 |
| create | 90112 / 90112 | 11104 / 196360 | 432 / 432 |
| workflow | 32768 / 32768 | 21552 / 21552 | 432 / 360 |
| workflow_replay | 32768 / 0 | 242216 / 2896 | 432 / 432 |
| cancel | 0 / 0 | 14280 / 14304 | 432 / 432 |
| cancel_replay | 0 / 0 | 3792 / 3792 | 360 / 432 |
| prepare | 0 / 0 | 4272 / 4272 | 432 / 432 |
| admission_wait | 65536 / 65536 | 17928 / 17984 | 360 / 432 |
| admission | 0 / 0 | 21992 / 21992 | 360 / 432 |
| admission_replay | 0 / 0 | 10424 / 10480 | 432 / 432 |
| succeeded | 24576 / 24576 | 15240 / 15240 | 360 / 432 |
| retryable | 0 / 0 | 16904 / 16904 | 360 / 360 |
| uncertain | 0 / 0 | 18392 / 18368 | 432 / 360 |
| blocked | 0 / 0 | 19896 / 19896 | 360 / 432 |
| results_replay | 0 / 0 | 7424 / 7424 | 432 / 432 |
| subscription_first | 0 / 0 | 8952 / 8952 | 1973 / 1973 |
| subscription_lost | 0 / 0 | 9048 / 9048 | 2625 / 2625 |
| subscription_retry | 0 / 0 | 776 / 776 | 1977 / 1905 |
| subscription_drain | 0 / 0 | 22824 / 22824 | 27870 / 27798 |
| delivery_first | 16384 / 16384 | 4872 / 4872 | 2190 / 2190 |
| delivery_lost | 0 / 0 | 680 / 680 | 2914 / 2842 |
| delivery_retry | 0 / 0 | 5144 / 5144 | 2194 / 2122 |
| delivery_drain | 0 / 0 | 6280 / 6256 | 3956 / 3884 |

В A2 workflow_replay прирост 32768 B относится к `pg_catalog/r/main`, а не
к новым бизнес-событиям; в B2 пустое окно содержит 238400 B кластерного WAL.
Оба наблюдения сохранены как фон/затраты окна, без автоматического вычитания
или приписывания этого роста новой бизнес-записи.

[Проверка формата](format-check.json): missing_phase, same_physical_offset, missing_original_bytes, unknown_numeric_metric и inconsistent_quote_revision — все пять повреждённых копий отвергнуты.

## Исследовательские опыты

[run-a](run-a/report.json) и [run-b](run-b/report.json) завершились с корректными
raw/дельтами и cleanup по общему verifier OPS-023. Первичный API verifier run-a
также прошёл; format-check исходно проверил четыре повреждённых варианта. Затем
при ревью найдено: fixture менял value/trade time при прежней price revision.
Такие данные не используются для итогового manifest продолжений.

Сохранены исходные [driver](exploratory-driver.go.txt) и
[verifier](exploratory-verifier.py.txt); их SHA256 совпадают с source_sha256 обоих
исследовательских report. Manifest каждого run содержит исходный шаблон и его hash.
Новая версия фиксирует trade/received time в setup и использует revision 1 для
candidate/создания, 2 для false, 3 для admission. Итоговый verifier проверяет
согласованность этих снимков. Повторные опыты начаты с новых чистых volumes.

## Границы вывода

Ни один run не квалифицирует верхние стоимости. Отсутствие конечных bounds для
HTTP/percent/pair/source ID/whole journal batch, неподдерживаемые device IO/temp
peak PostgreSQL и fsync Kafka, непокрытые максимальные размеры/варианты и
продолжения matcher/notifications/market-data блокируют OPS-018. Это локальный
Docker Desktop опыт; он не заменяет полный Linux-профиль DEC-008. Коэффициент
2× и квоты не вычислялись. Фоновый рост не вычитался; нулевой рост relation/log
не означает отсутствия WAL/IO или бесплатный повтор.
