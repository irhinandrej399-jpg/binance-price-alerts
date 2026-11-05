# Kafka syscall observer (OPS-029)

Из корня, при работающем Docker и локальном Go toolchain:

```sh
docker build -t binance-fsync-observer:ops029 -f infra/kafka/measure/observer.Dockerfile .
python3 scripts/measure_kafka.py --output /tmp/fsync-a --observe-fsync
python3 scripts/measure_kafka.py --output /tmp/fsync-b --observe-fsync
python3 scripts/measure_kafka.py --output /tmp/fsync-unavailable --observe-fsync --fsync-fault unavailable
python3 scripts/measure_kafka.py --output /tmp/fsync-lost --observe-fsync --fsync-fault lost
python3 scripts/verify_kafka_fsync_measurement.py /tmp/fsync-a /tmp/fsync-b --failed /tmp/fsync-unavailable /tmp/fsync-lost
```

Оба fault запуска обязаны вернуть ненулевой код. Все output должны отсутствовать.
Observer image собирается отдельно; digest/версия/исходники записываются в отчёт.
Закреплены base image и strace package; недоступность закреплённого пакета требует
восстановить image/package environment, а не молча заменить версию.

Необходим Linux ptrace внутри Docker VM, PID namespace конкретного disposable
broker, capabilities SYS_PTRACE/KILL и seccomp=unconfined только у sidecar.
Sidecar без сети/host PID/volume mounts, с отдельным уникальным именем и label.
Рабочие контейнеры и volumes не используются. Broker image, CPU/RAM,
`flush.messages=1`, acks=all, RF/min ISR=1 и retention остаются штатными.
На платформе с запретом ptrace нужен Linux/Docker стенд, разрешающий эти права.

`scripts/kafka_fsync.py` прикрепляет `strace -f -ttt -T -e trace=fsync,fdatasync`
к namespace PID 1 после проверки JVM kafka.Kafka. Сохраняет `/proc/1/stat`
(включая starttime), exe/cmdline до/после, Linux kernel, начальные TID/TracerPid,
версию strace и image ID. До/после каждого окна все видимые broker threads
должны иметь TracerPid этого tracer. `-f` следует новым потокам; в агрегацию
попадают только вызовы TID с доказанной принадлежностью `/proc/1/task` на одной
из границ. Короткоживущий новый поток без такого свидетельства приводит к unknown.
Чужие процессы не прикрепляются; даже потомок JVM без свидетельства thread
membership не может быть включён в измерение.

При запуске отдельный процесс выполняет `sync file` и `sync -d file` под второй
трассировкой. Raw обязан содержать успешные завершённые fsync и fdatasync;
его TID не должны встречаться в broker trace. Это положительный контроль обоих
фильтров и отрицательный контроль чужого процесса, вне измеряемых окон.
Штатный filesystem `sync` collector также отдельный процесс, его syscall не
выдаётся за вызов Kafka.

Окно: после baseline/предыдущего after (в совместном runner — после before),
проверка потоков → Linux epoch timestamp → operation/ack → штатные ожидание 1 с,
filesystem sync и file snapshot → Linux epoch timestamp → проверка потоков.
Поэтому это inclusive окно операции и диагностики с фоном broker, а не чистое
время Publish. Timestamp берётся через sidecar из того же Linux clock, raw
strace имеет микросекундную точность, duration — секунды с шестью десятичными.
`fsync-windows.json` хранит границы в ns и принадлежность TID, `fsync.trace` —
весь непрерывный поток от attach до detach, включая setup вне окон.

Счётчик — число завершённых fsync/fdatasync целиком внутри окна. Failed syscalls
тоже считаются и выделены `failed_count`. `time_ms` — сумма elapsed длительностей
вызовов, а не wall time окна: потоки могут перекрываться, tracing/scheduling
увеличивают длительность. Это не измерение device flush, диска macOS или fsync
latency без instrumentation. Фон пустого окна не вычитается автоматически.
fdatasync=0 допустим только при полном наблюдении обоих фильтров.

Ptrace синхронно останавливает tracee на syscall entry/exit, lossy ring buffer не
используется. `lost_events=0` означает подтверждённое завершение этой трассировки,
а не независимый счётчик потерь устройства: живой tracer на границах, штатный
SIGINT/detach (strace exit 130), неизменный broker starttime, допустимые diagnostics,
полный разбор raw и пар unfinished/resumed. Неизвестная строка, оборванный вызов,
пересечение границы окна, потеря tracer/доступа/принадлежности потока дают
unknown/null/qualification_blocked=true и failed collector. Unknown не равен нулю.
Raw hashes защищают от случайного изменения, verifier пересчитывает агрегаты.

Sidecar удаляется до удаления broker, затем проверяется отсутствие его label;
штатный collector удаляет свои containers/networks/volumes и проверяет labels.
SIGINT/TERM обрабатываются collector; SIGKILL/потеря Docker могут потребовать
ручной очистки по уникальным именам из report. Ошибка cleanup не считается успехом.

Подключение к OPS-023 без изменения business manifests:

```sh
python3 scripts/measure_joint.py --output /tmp/joint-fsync --observe-kafka-fsync --manifest infra/measure/control.json --command python3 infra/measure/control_adapter.py
python3 scripts/verify_joint_measurement.py /tmp/joint-fsync
```

Kafka observer работает вокруг того же session.operation и snapshots; отдельный
adapter не создаётся. `qualified=false` сохраняется: остальные компоненты и
полный DEC-008 профиль не квалифицированы. Без флага действует прежний
unsupported collector. Старые raw OPS-020/022 не меняются.

[Результаты и ограничения](../../../docs/measurements/OPS-029/README.md).
