# Market-data source manifest (OPS-022)

Из корня проекта, Docker и Go из `.go-version`. Запускать последовательно,
без функционального/race suite или другой измерительной нагрузки:

```sh
GOTOOLCHAIN=local GOCACHE=/tmp/binance-go-build python3 scripts/measure_joint.py --output docs/measurements/OPS-022/run-a --manifest infra/measure/marketdata.json --timeout 120 --command python3 infra/measure/marketdata_adapter.py docs/measurements/OPS-022/run-a/identities.json
GOTOOLCHAIN=local GOCACHE=/tmp/binance-go-build python3 scripts/measure_joint.py --output docs/measurements/OPS-022/run-b --manifest infra/measure/marketdata.json --timeout 120 --command python3 infra/measure/marketdata_adapter.py docs/measurements/OPS-022/run-b/identities.json
python3 scripts/verify_marketdata_measurement.py docs/measurements/OPS-022/run-a docs/measurements/OPS-022/run-b
python3 docs/measurements/OPS-022/summarize.py
python3 docs/measurements/OPS-022/format-check.py
```

Каждый output должен отсутствовать. Для новых повторов заменить оба вхождения
run-a/run-b в соответствующей команде. Общий runner, диагностические credentials,
контроль background/checkpoint/FPI и cleanup — [OPS-023](README.md).
Driver получает только runtime DSNs и пишет через `market_data_runtime`.
Готовые storage/producer/relay не изменяются; внешние Binance/Telegram не вызываются.

## Фазы

| Окно | Наблюдаемая операция |
| --- | --- |
| empty | нет adapter-записей, диагностика/фон collectors остаются |
| begin / begin_again | BeginGeneration 1 → 2, unconfirmed; второй запуск сохраняет gap |
| status_live / status_gap / status_sticky | SetStatus, исторический gap не очищается; последнее окно включает проверочное Latest |
| accept_small | первая исходная сделка 10, revision 1/1, latest + price_outbox одним commit |
| repeat / old | source ID 10/9, без нового outbox; SQL lock/rollback не считаются бесплатными |
| accept_4095_gap / accept_4096 | source ID 12/13, revisions 1/2 и 1/3; разрыв source IDs сохраняется |
| stale_generation | Accept старого поколения → ErrStale |
| accept_generation2 | source ID 14, revision 2/1, 4096 байт, unconfirmed/gap |
| reject_4097 | Accept source ID 15 с размером 4097 → ErrTooLarge до durable latest/outbox |
| claim_first / publish_first / mark_first | первая запись: Claim → настоящий producer commit → SQL mark, отдельные окна |
| claim_lost / publish_lost | вторая запись; fault helper подавляет EndTxn response после broker code=0; SQL mark отсутствует |
| reclaim | ожидание истечения DB-clock lease 60 s, повторный Claim с большим token, те же bytes/ID |
| publish_retry / mark_retry | новый producer epoch с прежним transactional.id, физический дубль, затем SQL mark |
| relay_tail | настоящий relay.Run публикует две оставшиеся записи, последовательно; observer отменяет после второго committed |
| claim_empty | Claim подтверждает отсутствие неопубликованной работы |

Размеры — полный сериализованный PriceObserved, не только payload. Граничные
размеры получены валидным расширением correlation_id ASCII `x`; это сжимаемые
данные, не худший случай TOAST. Цена точная `123.000000000000000001`, одна пара
BTCUSDT, фиксированные синтетические source times. Нет конечного общего предела
всех полей, истории поколений, сохранённых outbox и числа publish-повторов.
Один размер 4096 не доказывает максимальную физическую стоимость всех 4096 bytes.

Текущий storage.Accept принимает одну запись на транзакцию, Claim возвращает
одну; production relay держит один resident payload, Publish — одна Kafka
transaction/event. Поэтому максимальный **batch равен 1**, два tail events —
две последовательные операции, а не batch=2. Цена каждого publish-повтора
отдельная, SQL mark не удаляет физические данные и не освобождает кредит.

Setup применяет схемы через runner и инициализирует producer ID до baseline;
бизнес-строк/цен в setup нет. Первые три publish окна включают New/epoch/Close
готового producer, а tail включает настоящий lifecycle Run. Изолированные
Claim/Publish/Mark нужны для разделения физических границ, они не подменяют
реализацию relay. В reclaim lease истекает естественно без fixture UPDATE;
ожидание/фон входит в полное окно и не нормализуется как чистый Claim.

Verification после всех физических after/checkpoint читает собственный outbox
и Kafka read_committed. Четыре логических события/пять физических записей;
вторая запись имеет две копии с теми же bytes/ID/key/partition и разными offsets.
Все четыре SQL mark=true; oversized/repeat/stale не добавили работы. Latest —
source 14, revision 2/1, live/gap. Идентичности независимы от OPS-024: trigger и
attempt для источника цены отсутствуют, это не неизвестная метрика стоимости.

## Полный проход и границы квалификации

Порядок исходных измерений: [API](API.md) → [matcher](MATCHER.md) →
[API-owned fixture](NOTIFICATIONS_FIXTURE.md) → [notifications](NOTIFICATIONS.md)
→ этот источник market-data. Использовать указанные там сохранённые source paths
и hashes; при создании новой цепочки передавать новые source paths и обновлять
manifest provenance, не смешивать старые ID с новыми. Fixture проверяется
до measured baseline. Контроль/collectors OPS-019/020/023 воспроизводятся по их
README; они не заменяют бизнес-фазы. Все измерения требуют новых disposable volumes.
Сводка OPS-022 проверяет сохранённую цепочку; новые источники требуют новой сводки.

[Общая coverage](../../docs/measurements/OPS-022/coverage.json) перечисляет каждый
класс и 11 компонентов с report/raw/selector либо явным блокирующим пробелом:
SQL heap/index/TOAST/forks, WAL/temp, PostgreSQL fsync отдельно; Kafka log/index/
segment/transaction и fsync отдельно. Наблюдённый нулевой рост файла означает
использование уже выделенного места, а не нулевую стоимость UPDATE/commit.
Unsupported temp peak/device IO и Kafka fsync не подставляются как нули.
Kafka segment roll/aborted txn index остаются непокрытыми upper-bound вариантами.

API → matcher → notifications относится к исходному API fixture, reports/handler
являются отдельными datasets с C=0. Этот источник цены не доказывает стоимость
market-data → matcher price Prefix/candidates → API workflow → notifications.
Она явно непокрыта. Также отсутствуют ledger, полный baseline DEC-008, конечные
bounds повторов и всех retained данных, максимальные поля/сканы/варианты переходов.

`qualified=false`; successful collector/verifier не разрешает OPS-015/016 и
не выбирает 2×, таблицу стоимости или квоты. WAL кластерный, relation deltas
по владельцам; inclusive нельзя складывать с operation/checkpoint повторно,
empty не вычитается. Полная квалификация остаётся OPS-018 с указанными блокерами.
