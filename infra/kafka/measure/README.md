# Физический collector Kafka (OPS-020)

Опциональное broker-scoped наблюдение fsync/fdatasync добавлено в OPS-029:
[протокол, права, команды и raw](FSYNC.md). Без `--observe-fsync` сохранён
исходный unsupported режим, описанный ниже.

Из корня, Docker и Go из проекта:

```sh
python3 scripts/measure_kafka.py --output /tmp/kafka-measure-a
python3 scripts/measure_kafka.py --output /tmp/kafka-measure-b
python3 scripts/measure_kafka.py --output /tmp/kafka-measure-failure --fail-after-up
# Предыдущая команда обязана завершиться ненулевым кодом.
PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify_kafka_measurement.py /tmp/kafka-measure-a /tmp/kafka-measure-b /tmp/kafka-measure-failure
```

Новый каталог обязателен: результаты не перезаписываются. Каждый вызов создаёт
уникальные project/prefix/volume/loopback port; используется только обычный
`infra/kafka/compose.yaml`, без load override и external volumes. `finally`
удаляет проект и проверяет отсутствие всех его контейнеров (включая stopped),
сетей и volumes по Compose label. SIGINT/TERM проходят через cleanup; SIGKILL,
отказ Docker или авария хоста требуют ручной проверки project из report.json.
Все subprocess ограничены 240 с, startup 180 с, init 90 с, ответы driver 60 с,
его Kafka-вызовы — 20/30 с. Ошибка не выдаётся за полный опыт.

`internal/tools/kafkameasure` — только управляющий процесс опыта поверх
существующего `kafka.New/Publish`. Это не новый прикладной producer.
Он остаётся живым между фазами; подготовка PID/epoch не попадает в первую
измеряемую публикацию. Порядок:

1. Снимок initial после готовности нового брокера, до init topics.
2. Штатный init трёх topics; New с устойчивым для опыта transactional.id;
   отдельная транзакция `prepare` в subscription-changes; read_committed group
   читает её, явно commit offsets и закрывается. Это создаёт transaction/offset
   topics до baseline. Никаких измеряемых событий ещё нет.
3. Фиксация фактических broker/topic configs, затем baseline.
4. Empty: без Publish (пустой batch producer запрещает); снимок.
5. First: один Publish key=`probe`, фиксированный JSON с event_id;
   после подтверждённого commit снимок.
6. Repeat: тот же живой producer, отдельный Publish с идентичными key/value;
   после подтверждённого commit снимок. Это **физический дубль**, а не повтор
   сетевого запроса в рамках идемпотентной сессии.
7. Независимый direct-partition consumer от начала, read_committed:
   prepare и две копии probe с разными offsets. JSON сохраняет точные bytes
   как строки, key, partition и offset. Offset commit здесь не выполняется.

Каждый снимок: ожидание 1 с, `sync` внутри контейнера, затем последовательные
`stat size/blocks` всех файлов `/var/lib/kafka/data`, включая metadata,
`__transaction_state`, `__consumer_offsets`, log/index/timeindex/txnindex,
checkpoints/snapshots и вспомогательные файлы. block unit — 512 bytes.
`*.json.gz` содержат полную опись и границы времени; report.json — суммы по
директории/расширению и дельты к предыдущей фазе и baseline. Это live inventory,
не атомарный snapshot: фоновый metadata traffic и checkpoint могут пересечь
сбор. Состояние initial не является нулём: bootstrap уже создал metadata.

Граница измерения — успешный EndTxn через Publish и filesystem sync; sync
не подменяет Kafka durability. `flush.messages=1`, acks=all, RF/min ISR=1,
транзакции и запрет retention не меняются. Успешный EndTxn не обещает, что
все фоновые checkpoints успели завершиться. Исходный broker log сохраняется.
Чтение в конце выполняется после всех физических снимков.

Apparent size включает preallocated index/timeindex (обычно 10 MiB каждый),
allocated blocks учитывают sparse files/округление файловой системы. Нулевая
дельта allocated при росте log означает использование уже выделенного блока,
а не бесплатную запись. Прогрев относится к отдельной стоимости подготовки.
В committed-only опыте `.txnindex` может отсутствовать: aborted transactions
не создаются; отсутствие файла видно в полной описи, не считается потерей данных.
Короткий опыт не вызывает segment roll и не квалифицирует его верхнюю стоимость;
новый segment создаёт log и сопутствующие индексы, резерв должен покрывать
округление и переход границы. Пустой контроль показывает фон только своего
временного окна; его нельзя механически вычитать из first/repeat как константу.

В этом профиле нет broker-scoped счётчика количества/времени fsync. Поля явно
`null/unsupported`, `qualification_blocked=true`; filesystem sync не измеряет
fsync count/time брокера. VM/host write amplification, directory/inode overhead,
физический диск macOS, HA, нагрузочный паспорт DEC-008, segment-roll upper bound
и потерянный commit-ответ здесь не квалифицированы. Последний использует
готовый fault helper в OPS-021. Ни этот опыт, ни положительная raw-проверка
не разрешают назначать окончательные credits OPS-018 по одному сообщению.

Verifier пересчитывает суммы/дельты из всех сырых файлов, проверяет internal
файлы, порядок снимков, физический рост log обеих публикаций, идентичность
read_committed дублей и cleanup. Для failed допускается только намеренно
введённая ошибка после up с подтверждённым cleanup.
