# Совместный фазовый runner (OPS-023)

Из корня репозитория, Docker и локальный Go toolchain:

```sh
python3 scripts/measure_joint.py --output /tmp/joint-a --manifest infra/measure/control.json --command python3 infra/measure/control_adapter.py
python3 scripts/measure_joint.py --output /tmp/joint-b --manifest infra/measure/control.json --command python3 infra/measure/control_adapter.py
python3 scripts/verify_joint_measurement.py /tmp/joint-a /tmp/joint-b
```

Каждый новый output должен отсутствовать. Запуски выполнять последовательно:
параллельная нагрузка и полный functional/race suite искажают измерение.
`--command` — последний аргумент; это доверенная локальная программа. Не передавать
credentials в аргументах: команда сохраняется для воспроизведения. Adapter получает
только runtime DSNs `DATABASE_URL` (api), `MARKET_DATA_DATABASE_URL`,
`MATCHER_DATABASE_URL`, `NOTIFICATIONS_DATABASE_URL`, а также `KAFKA_BROKERS` и
уникальный `KAFKA_PREFIX`. Диагностические SQL credentials остаются у collector.
Environment и stderr adapter не сохраняются; для отчёта берутся только поля ack.
Не печатать secrets в stdout. Как и обычный локальный executable, доверенный adapter
имеет права пользователя; это протокол измерения, а не security sandbox.

## Вызываемый интерфейс и протокол v1

`measure_postgres.main(argv, session)` и `measure_kafka.main(argv, session)`
сохраняют собственные lifecycle, raw snapshot и вычисления. Без session CLI
OPS-019/020 работает по прежнему протоколу. Session предоставляет `phases`,
`setup(owner, env)`, `sync()`, `operation(phase_id)`, `verify()`.
Два collector работают в отдельных потоках; barriers разрешают операцию один раз
после обеих границ before, следующую фазу — после обоих сборов/checkpoint.
Сбой collector разрывает barriers, останавливает process group adapter и вызывает
cleanup обоих уникальных проектов. Все subprocess collector имеют timeout 240 с,
barriers — 600 с; `--timeout` (default 60 с) ограничивает каждый ack.

Runner запускает один живущий до verification процесс. Запросы stdin и ответы
stdout — по одному JSON объекту на строке (ответ <=64 KiB, возможна передача фрагментами в пределах одного deadline, без дополнительного
вывода). Запрос: `{"phase_id":"setup"}`; ответ:
`{"phase_id":"setup","status":"ok"}`. Затем именованные фазы manifest в порядке
списка, затем `verification`. Другая фаза/status, EOF, ошибочный JSON или timeout
означают провал; следующая фаза не запускается. SIGTERM/SIGINT также запускают
cleanup. SIGKILL/потеря Docker могут помешать cleanup: уникальные project labels
остаются в collector report для адресного удаления. Working/load volumes не
адресуются, имена проектов/префикс/порты генерируют сами collectors.

Setup включает миграции четырёх владельцев, создание topics и подготовку adapter.
Control adapter готовит существующий transactional producer, пишет warmup,
прогревает group offsets; это не измеряемая операция. До первой фазы PostgreSQL
делает warmup диагностических чтений и сохраняет baseline. Между предыдущим
checkpoint (для первой фазы — baseline) и очередным before сохраняет
`background_delta`; начальный CHECKPOINT виден в этом отдельном окне. Перед каждой операцией — CHECKPOINT и
before-снимок; after и отдельный checkpoint-снимок дают operation/checkpoint/
inclusive deltas без двойного суммирования. FPI после CHECKPOINT входят в operation.
Kafka делает initial/baseline, before/after каждой фазы; `background_delta` — рост
от предыдущего after до текущего before. Operation delta использует before/after,
в baseline delta также остаётся фон. Статистика двух систем снимается
последовательно внутри каждого collector, это не атомарный общий снимок.

Пустая фаза показывает диагностику/фон и не вычитается автоматически.
Kafka after следует за ack (adapter обязан дождаться EndTxn), ожиданием 1 с,
filesystem sync и live stat всех файлов, включая internal topics/metadata.
Это не измерение device fsync count/time и не доказательство нулевого overhead.
Verification выполняется после последних after/checkpoint, вне измеряемых операций;
control проверяет два read_committed сообщения (warmup + publish), а PG collector
независимо сохраняет исходное/конечное число accounts (0/10).

## Manifest/report v1

Manifest: `format_version=1`, `source_id`, `sources` (пути к файлам adapter),
`contract_versions` (имя → положительная версия), `phases` (упорядоченный список).
Фаза: уникальный ASCII `phase_id` (буквы/цифры/`_`/`-`; имена warmup/setup/baseline/initial/verification и суффиксы before/after/checkpoint зарезервированы), неотрицательный целый `size`, `unit`,
положительный `max_batch`, `links` — список объектов с ключами
`event_id/trigger_id/attempt_id`. `null` означает отсутствие такого ID в данном
control, а не неизвестную метрику. Бизнес-manifest обязан задать реальные связи.
Размеры payload/число операций с разными единицами можно задавать отдельными
фазами; размеры control заданы в `control.json` и исходном payload kafkameasure.

Report: manifest и его SHA256, source SHA256 всех internal Go-файлов, go.mod/sum
и перечисленных sources, hashes миграций/контрактов, source ID, команда adapter,
UTC epoch ns запуска и ack каждой фазы, относительные ссылки на два report/raw.
Версии и digest образов, migration history/schema dumps, durability, ограничения,
команды и physical metrics остаются в исходных collector reports. `format_version`
вложенных collectors — 1; семантика raw OPS-019/020 не изменена.

Успех — `collected-not-qualified`, `qualified=false`: unsupported temp peak/device
IO PostgreSQL и Kafka fsync блокируют квалификацию. Отсутствие фазы/ack/обязательной
числовой метрики, unknown/null вместо числа или несогласованная raw delta
отвергаются `verify_joint_measurement.py`. Допустимые null pg_stat_io по
backend/object/context сохраняются как unsupported, а не ноль. Unknown никогда
не подставляется в стоимость. Полная стоимость/2×/ledger — OPS-018, бизнес-цепочки
— OPS-024–026/022.

Отказные контроли (ожидаемый exit 1):

```sh
python3 scripts/measure_joint.py --output /tmp/joint-fail --manifest infra/measure/control.json --command python3 infra/measure/control_adapter.py fail
python3 scripts/measure_joint.py --output /tmp/joint-timeout --manifest infra/measure/control.json --timeout 60 --command python3 infra/measure/control_adapter.py timeout
```

Оба доходят до write. Timeout включает setup: слишком короткий бюджет может
закончить опыт на Go build/подготовке producer. Failed report непригоден для
стоимости; cleanup проверять в обоих вложенных report.

После отказных контролей:

```sh
python3 scripts/verify_joint_measurement.py /tmp/joint-a /tmp/joint-b --failed /tmp/joint-fail /tmp/joint-timeout
```

`--failed` проверяет именно контрольный отказ на write после setup/empty,
оба failed collector reports и подтверждённое отсутствие disposable ресурсов.
