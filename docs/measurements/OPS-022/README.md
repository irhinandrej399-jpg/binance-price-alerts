# OPS-022 — источник market-data и общая матрица покрытия

5 ноября 2026. Команды полного прохода, фазовая матрица и ограничения:
[MARKETDATA.md](../../../infra/measure/MARKETDATA.md). Это инфраструктурное
измерение через production adapters, без изменения бизнес-переходов и без
нагрузочного fixture DEC-008. Исходный commit — `26c083a` (OPS-026);
собственный commit результата определяется через Git log.

## Market-data

Manifest содержит 24 окна. Применены реальные BeginGeneration, SetStatus,
Accept/Latest, Claim/MarkPublished, producer Publish и serial relay.Run.
Размеры четырёх исходных событий: 531 / 4095 / 4096 / 4096 байт. Source IDs
10/12/13/14 дают revisions 1/1, 1/2, 1/3 и 2/1; исторический gap сохраняется.
Source 15 при 4097 B отклонён; повтор 10, старый 9 и stale generation не
добавили outbox. Максимальный batch существующего storage/relay — 1.

Потеря EndTxn response вводится существующим kafkafault.Proxy. Broker code=0,
клиент получает ErrCommitUnknown, SQL mark отсутствует. После естественного
истечения DB-clock lease 60 s Claim получает token=2, исходные bytes/event_id
сохраняются. Повторная публикация и SQL mark измерены раздельно.
Настоящий relay публикует две оставшиеся максимальные записи последовательно.

Итоговые identities/resolved manifests содержат четыре marked outbox rows и
пять Kafka read_committed records. Две копии `ops022-price-2` имеют те же key,
partition и bytes, разные offsets. Физический дубль не является новым логическим
обязательством. Verification читает SQL/Kafka после всех measured snapshots;
setup инициализирует producer, но не создаёт бизнес-строки.

[run-a](run-a/report.json) и [run-b](run-b/report.json) используют разные
PostgreSQL/Kafka projects, prefixes и новые volumes. Версии Go/образов/digests,
source/schema/contract hashes, durability, ack times и команды сохранены в reports.
Driver hash одинаков в обоих опытах. В source hashes обоих report сохранена
ранняя версия verifier: после старта усилены проверки null Kafka fsync count/time
и ненулевого роста prices log в publish окнах. Runtime driver и manifest не
менялись; все raw проверены окончательным verifier.

## Полная матрица

[coverage.json](coverage.json): 25 классов, каждый с 11 компонентами и явными
блокерами верхней оценки. Наследованы 17 классов API/matcher/notifications
из [OPS-026](../OPS-026/coverage.json), добавлены шесть групп market-data,
неизмеренное продолжение цены и будущие ledger/full-fixture записи.
Для каждого компонента есть report/raw/selector либо `unmeasured` с причиной.

Компоненты: SQL heap/index/TOAST, WAL/temp/PostgreSQL fsync и Kafka log/index/
segment/transaction/fsync. Main/fsm/vm/init и app/TOAST индексы видны в raw по
OID/kind/fork; TOAST не прибавляется через total_relation_size. Kafka содержит
все business/internal/metadata файлы; apparent/allocated разделены. WAL и OS IO
кластерные, их нельзя суммировать по владельцам.

[summary.json](summary.json) сводит десять чистых отчётов: по два API, matcher,
notifications reports, notifications handler и market-data. Сохранены operation,
background, checkpoint, inclusive, relation forks/temp по владельцам, кластерные
WAL insert bytes и WAL segment pool отдельно, Kafka группы/окна, unsupported.
Это ссылки и полные окна, **не сумма стоимости цепочки**. Не складывать inclusive
с operation/checkpoint; empty не вычитать, source setup не списывать повторно.
Подробные OS IO/fsync counters остаются в raw/collector report по указанным ссылкам.

API source OPS-024/run-a2 → matcher OPS-025/run-a3 → reports/handler OPS-026
сохраняет исходные ID/bytes; последние границы — отдельные API datasets с C=0.
Market-data — независимый источник со своими ID; API trigger/attempt здесь
отсутствуют. Его продолжение price Prefix/candidates → canonical API workflow
→ notifications **не измерено** и явно блокирует общую верхнюю стоимость.

## Граница результата

`qualified=false`. Kafka fsync count/time остаются null/unsupported,
PostgreSQL temp peak/device IO неподдержаны; snapshots не доказывают пиков.
Segment roll/aborted txn index, худший TOAST/incompressible input, все размеры
полей, contention/scan/rollback/fencing/write variants, конечные retained history
и publish/check/report bounds, будущий ledger и полный DEC-008 fixture остаются
блокерами. Граничные bytes получены сжимаемым correlation_id и не являются
максимальной физической стоимостью всех допустимых payloads.

OPS-018 получает проверяемую опись фактов и отрицательную квалификацию; таблица,
2× и квоты не выбирались, OPS-015/016 остаются planned. Cleaner rate по-прежнему
ноль для capacity planning. Независимые OPS-027/NTF-017 не исправлялись.

## Фактическая проверка

Оба runner завершились exit 0, по 24 окна; общий market-data verifier — exit 0.
Разные projects/prefix подтверждены, cleanup обоих collectors проверен в каждом
опыте. Driver SHA256 `fde7260ae2a1c2220cb77c4589bbfd5dbc2f30cf48f261da8af57ac296fff006`.

Выборка полных operation окон в байтах, A / B. App forks включают app heap/index;
TOAST/forks и все остальные relation группы сохранены отдельно в raw/summary.
WAL кластерный; значения не нормализованы на событие и не являются верхними:

| Фаза | WAL insert | Market-data app forks | Kafka prices log apparent |
| --- | ---: | ---: | ---: |
| empty | 0 / 0 | 0 / 0 | 0 / 0 |
| accept_small | 2040 / 2040 | 32768 / 32768 | 0 / 0 |
| accept_4095_gap | 203976 / 203976 | 0 / 0 | 0 / 0 |
| accept_4096 | 3960 / 3960 | 0 / 0 | 0 / 0 |
| reject_4097 | 2768 / 2768 | 0 / 0 | 0 / 0 |
| publish_first | 0 / 0 | 0 / 0 | 686 / 686 |
| publish_lost | 0 / 0 | 0 / 0 | 4250 / 4250 |
| reclaim | 4112 / 4112 | 0 / 0 | 0 / 0 |
| publish_retry | 56 / 56 | 0 / 0 | 4250 / 4250 |
| relay_tail | 6384 / 6384 | 0 / 0 | 8502 / 8502 |

Разница WAL между чистыми опытами сохраняется без подмены минимумом; background
и CHECKPOINT есть в отдельных окнах. Большой WAL при нулевом app growth не
доказывает принадлежность всей разницы одному Accept. Выделенный WAL segment pool
не складывается с insert bytes. Нулевая temp/TOAST дельта малого fixture не
доказывает нулевой верхний предел.

`python3 docs/measurements/OPS-022/summarize.py` — exit 0, 25 классов/10 отчётов;
`format-check.py` — exit 0: **14/14** повреждённых копий отвергнуты, включая
unknown WAL, неверные body/offset/mark/size/token, нулевой unsupported fsync,
ложную квалификацию и отсутствующий компонент. Результаты —
[format-check.json](format-check.json). Raw не изменялся.
Go build/vet driver, Python syntax, backlog check и diff check — успешны.
Полный функциональный/race/load suite не запускался: это измерительный проход,
не изменение production behavior или квалификация полного runtime.
