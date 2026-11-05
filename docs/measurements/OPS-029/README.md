# OPS-029 — broker-scoped fsync/fdatasync

5 ноября 2026. Результат: observer доступен и проверен на Docker Desktop Linux
VM; это контроль механизма измерения, а не квалификация таблицы OPS-018.
[Команды, интерфейс и ограничения](../../../infra/kafka/measure/FSYNC.md).

Среда: Docker 29.5.2, Linux 6.12.76-linuxkit aarch64, VM 10 CPU / 8 321 712 128 B
RAM, overlayfs. Broker Apache Kafka 4.3.1, закреплённый compose image digest
`77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`, 2 CPU/4 GiB;
acks=all, flush.messages=1, RF/min ISR=1, retention disabled не изменены.
Strace 6.19 (package 6.19-r1), observer image
`sha256:e268780b91edb6afd48fac8563370c35a9363c3da7102a8f3d6dfe2f43e8727e`.
Это локальная среда, не полный стенд DEC-008 и не измерение device flush macOS.

## Реальные окна

Каждая ячейка — `count / сумма elapsed syscall ms`:

| Чистый опыт | Empty | Первый Publish | Отдельный физический дубль |
| --- | --- | --- | --- |
| [run-a](run-a/report.json) | 3 / 8.434 | 8 / 10.925 | 8 / 12.218 |
| [run-b](run-b/report.json) | 3 / 9.348 | 8 / 11.337 | 10 / 10.274 |

Все вызовы broker в этих окнах — fsync с return=0; fdatasync=0 при включённом
фильтре. Отдельный положительный контроль обеих операций:
[run-a](run-a/fsync-control.trace), [run-b](run-b/fsync-control.trace).
Его TID отсутствуют в broker trace: сторонний процесс исключён.
Чистые project, prefix и volumes различны; штатный producer сохранил один
event_id/одни bytes в двух физических read_committed records с разными offsets.
В каждом report сохраняются фактические broker/topic configs, версии, PID
starttime/exe/cmdline, TID/TracerPid, source hashes и все команды.

[Raw run-a](run-a/fsync.trace), [raw run-b](run-b/fsync.trace), границы
[run-a](run-a/fsync-windows.json) / [run-b](run-b/fsync-windows.json).
Setup находится в непрерывной трассе, но не в агрегатах окон.
Штатные SIGINT/detach дали strace exit 130; полные diagnostics и hashes сохранены.
`lost_events=0` основан на завершённом синхронном ptrace протоколе, без lossy
ring buffer. Незавершённая трасса не получает это значение.

Окно включает operation, ack, ожидание 1 с, filesystem sync и file stat;
наблюдаются только syscalls потоков JVM broker, включая фон. Перекрывающиеся
потоки суммируются, instrumented elapsed time включает scheduling/ptrace
overhead. Empty не вычитается; эти значения не являются верхними оценками
стоимости или задержкой накопителя. Нулевой fdatasync не означает бесплатную IO.

## Отказ и совместный runner

- [unavailable](unavailable/report.json): запуск без ptrace capabilities,
  Operation not permitted, tracer exit 1, collector exit 1. Окон нет,
  status=unknown, count/time/lost_events=null, qualification_blocked=true.
- [lost](lost/report.json): tracer убит SIGKILL после начала empty, exit 137,
  collector exit 1; у окна нет завершённой границы, значения остаются unknown.
- В обоих отказах sidecar удалён, broker containers/networks/volumes отсутствуют.
  Успешные опыты также имеют оба cleanup verified=true. Ошибка observer
  сохраняется отдельно от результата очистки; финальные отказные опыты повторены
  после исправления ошибочной маркировки этого исключения как cleanup_error.
- [joint](joint/report.json): существующий control manifest OPS-023 с
  `--observe-kafka-fsync`, collector/adapter не дублировались. Empty 7 / 15.531 ms,
  write 12 / 21.112 ms; PostgreSQL и Kafka raw/ack/дельты/миграции/cleanup
  проверены joint verifier. `qualified=false`: PostgreSQL unsupported и
  неполный профиль остаются блокерами.

## Проверки и воспроизведение

Команды из корня, каждый реальный запуск выполнен последовательно:

```sh
docker build -t binance-fsync-observer:ops029 -f infra/kafka/measure/observer.Dockerfile .
python3 scripts/measure_kafka.py --output docs/measurements/OPS-029/run-b --observe-fsync
python3 scripts/measure_kafka.py --output docs/measurements/OPS-029/run-a --observe-fsync
python3 scripts/measure_kafka.py --output docs/measurements/OPS-029/unavailable --observe-fsync --fsync-fault unavailable
python3 scripts/measure_kafka.py --output docs/measurements/OPS-029/lost --observe-fsync --fsync-fault lost
python3 scripts/measure_joint.py --output docs/measurements/OPS-029/joint --observe-kafka-fsync --manifest infra/measure/control.json --command python3 infra/measure/control_adapter.py
python3 scripts/verify_kafka_fsync_measurement.py docs/measurements/OPS-029/run-a docs/measurements/OPS-029/run-b --failed docs/measurements/OPS-029/unavailable docs/measurements/OPS-029/lost
python3 scripts/verify_joint_measurement.py docs/measurements/OPS-029/joint
python3 docs/measurements/OPS-029/format-check.py
```

Успешные runs/verifiers — exit 0; оба fault запуска — ожидаемый exit 1.
При повторе выбрать новые output, сохранённые каталоги не перезаписывать.
[Format-check](format-check.json): 11/11 повреждённых копий отвергнуты:
count/time, loss/unknown, cleanup, missing end, garbage, unfinished syscall,
crossing boundary, foreign process, file delta. В raw повреждениях hashes
пересчитаны, поэтому проверен parser, а не только checksum.

Исторические OPS-020 run-a/run-b/failure и OPS-023 run-final проходят прежние
verifiers с обновлённым кодом. Raw OPS-020/022 не переписывались.
Py_compile изменённых scripts — PASS. Script unittest suite — 27, OK, 7 opt-in
migration tests skipped; миграции реально выполнены совместным runner.
Go driver/control binaries реально собирались в runs; полный Go/race suite
не запускался, прикладной Go-код не менялся.

Preflight до ready: собственный fsync в одноразовом strace контейнере,
all-thread attach/detach живого disposable broker (109 потоков). Первый detach
без CAP_KILL выявил tkill Operation not permitted, оба preflight стенда удалены;
добавление CAP_KILL проверено повторным успешным опытом. Промежуточные
development runs сохранены в `/tmp/ops029-development-run-a*`, в итоговую пару
не включены. Это инфраструктурная проверка вместо искусственных unit-тестов.

OPS-029 закрывает механизм syscall-наблюдения. PostgreSQL temp/device IO,
finite bounds, максимальные write variants, price downstream, ledger и полный
DEC-008 fixture остаются отдельными предпосылками OPS-018. Старое
coverage qualified=false не пересчитывается задним числом; будущие manifests
должны быть измерены с явным включением observer.
