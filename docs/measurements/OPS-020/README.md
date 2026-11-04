# OPS-020 — физический контроль Kafka

4 ноября 2026, локальный Docker Desktop Linux/aarch64, 10 CPU / 8 321 712 128 B
RAM VM, storage driver overlayfs; брокер ограничен 2 CPU / 4 GiB.
Это функциональный измерительный контроль, не нагрузочный профиль DEC-008.
Kafka 4.3.1, закреплённый digest
`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
Базовый commit `d86f13654046`; collector добавлялся в dirty tree (зафиксировано
в каждом report). Финальный исходник и эти результаты находятся в коммите OPS-020.

[Протокол и команда воспроизведения](../../../infra/kafka/measure/README.md).
Raw: [run-a](run-a/report.json), [run-b](run-b/report.json),
[намеренный отказ](failure/report.json); полные описи рядом в `*.json.gz`.
Raw reports содержат команды, Compose, broker/topic configs, version/image,
mounts/filesystem, broker logs, подтверждения producer, read_committed и cleanup.

| Фаза | run-a apparent B | run-b apparent B | allocated B, оба |
| --- | ---: | ---: | ---: |
| initial, до topics/producers | 20 974 246 | 20 974 318 | 24 576 |
| baseline, после подготовки | 754 989 024 | 754 989 024 | 344 064 |
| empty | 754 989 240 | 754 989 168 | 344 064 |
| first | 754 990 156 | 754 990 156 | 344 064 |
| repeat, идентичные bytes/event_id | 754 991 144 | 754 991 072 | 344 064 |

На **каждую** публикацию оба опыта показывают +224 B `.log` измеряемого topic
и +548 B `__transaction_state` log. Фоновый `__cluster_metadata` log меняется:
empty +216/+144 B; first +144/+216 B; repeat +216/+144 B.
Эти дельты не вычитаются из стоимости публикации как постоянная величина.
`__consumer_offsets` уже создан/прогрет до baseline, весь его расход включён
в опись; в first/repeat новых offset commits нет. Preparation имеет собственную
стоимость и не считается бесплатной частью первой транзакции.

Большой apparent baseline объясняется preallocated index/timeindex;
allocated blocks меньше благодаря sparse files. Дельта allocated 0 показывает
запись в ранее выделенные блоки, а не нулевую стоимость. Все файлы, включая
metadata, checkpoints и транзакционные, перечислены в raw. `.txnindex` в этих
committed-only опытах не создан; aborted transactions и segment roll не проверялись.

В конце независимый read_committed consumer видит prepare (offset 0) и две
одинаковые JSON-копии `event_id=ops020-probe-1` (offsets 2 и 4), key=`probe`.
Обе публикации — реальные успешные `kafka.Producer.Publish`, без потери ответа.
Новый transactional epoch и создание topics предшествуют baseline. Потеря
EndTxn-ответа и её manifest относятся к OPS-021 с существующим fault helper.

Durability не менялась: flush.messages=1, RF/min ISR=1, acks=all и transactional
producer; retention/compaction выключены. После EndTxn возвращается nil,
затем 1 с ожидания, filesystem sync и live stat. Это не атомарный broker snapshot
и не гарантия завершения всех фоновых checkpoints. Количество/время fsync
**unsupported**, null и qualification_blocked=true. Окончательная стоимость
fsync, segment-roll upper bound, VM/host amplification и credits OPS-018
этими опытами **не квалифицированы**.

Проверки:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify_kafka_measurement.py \
  docs/measurements/OPS-020/run-a docs/measurements/OPS-020/run-b \
  docs/measurements/OPS-020/failure
```

Все суммы/дельты восстановлены из raw; transaction/offset/metadata файлы,
рост event log, идентичность дублей, порядок фаз и cleanup подтверждены.
Отдельно изменённый allocated blocks в временной копии first был отвергнут
verifier (exit 1), исходные результаты сохранены. Failure collector завершился
exit 1 на `injected failure after up`, cleanup verified=true. Успешные опыты —
exit 0, cleanup verified=true, containers/volumes/networks пусты. Первый
диагностический запуск обнаружил отсутствие find -printf в BusyBox; заменён
на find -print0 + stat -c. Его cleanup также подтверждён; он не включён в таблицу.
