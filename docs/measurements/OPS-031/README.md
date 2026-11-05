# OPS-031: capture lifecycle, сессионный gate

5 ноября 2026. **Gate не пройден; OPS-031 разделён на OPS-033–036.**
Исследовательские результаты ниже сохранены без переквалификации.
Очередь реализации: [OPS-033](../../backlog/tickets/OPS-033.md) ancillary raw →
OPS-034 identity/references → OPS-035 classifier/bounds → OPS-036 lifecycle/PG gate.
Это новый ptrace capture, а не повтор OPS-030 preflight и не готовый observer.
`temp_peak_live_bytes`, доказанные lower/upper остаются `null`, qualification
blocked. Общий runner, collectors и business manifests не изменены.

## Что проверено

[Capture](capture/main_linux.go) запускает наблюдаемый executable с самого exec,
использует `PTRACE_GET_SYSCALL_INFO`, follow fork/vfork/clone/exec/exit,
`TRACESYSGOOD` и `EXITKILL`. Поддержана только Linux arm64 ABI. Начальное состояние
снимается до исполнения PostgreSQL. Attach к уже работающему кластеру отсутствует.
Journal хранит entry/return с аргументами, monotonic ns одного tracer, родство,
сигналы и снимки реальных `/proc/<tid>/fd` и каталога work files с
dev/inode/mode/size/nlink. Точные syscall-entry/exit времена ядра не утверждаются:
это времена обработки ptrace stops tracer. Файлы дедуплицируются по dev/inode
в каждом снимке; исчезновение и повторное появление начинают новое поколение.

Потенциальные операции изменения файлов и FD идут через один gate. Они
остановлены в entry и возобновляются по одной до return/exit; остальные syscalls
могут исполняться параллельно. Это существенно меняет расписание. **Полнота
классификатора и доказательство монотонности внутри каждой разрешённой операции
ещё не закончены**: совпадение снимков не даёт права назвать результат точным.
Fork/clone не держат gate: удержание его через CLONE_VFORK вызвало реальный
deadlock. Сам fork дублирует ссылки, не размеры. Ранняя остановка ребёнка может
прийти до сообщения родителя: ребёнок удерживается до регистрации родства.

Финальный [gate-g/report.json](gate-g/report.json),
[checks.json](gate-g/checks.json), [исходник запуска](gate-g/gate-source.py.txt),
[измеренный capture source](gate-g/capture-source.go.txt) и raw:

- [Filesystem](gate-g/filesystem.jsonl.gz): 698 снимков, 19 процессов, два
  поколения inode; diagnostic snapshot max 16 384 B совпадает с известным
  последовательным helper. [Независимые stat](gate-g/filesystem.txt): один
  dev/inode, hardlink nlink 1→2→1→0, shrink 16 384→4 096, regrow 12 288,
  dup/наследованный FD до last close. Новое имя/FD получили 8 192 B; ФС реально
  повторно использовала тот же dev/inode, однако между lifetimes был пустой
  снимок. Это особенно важно: один dev/inode без поколения недостаточен.
- [PostgreSQL](gate-g/postgres.jsonl.gz): 9 052 снимка, 12 процессов, 32
  поколения work inode. Ordinary sort spill и настоящий Parallel Hash с двумя
  launched workers и shared fileset: [план](gate-g/parallel-plan.txt),
  [PG log с размерами удалённых файлов](gate-g/postgres.stderr),
  [независимая опись процессов](gate-g/ordinary-processes.txt).
  Shared inode считается один раз в снимке, даже при нескольких владельцах FD.
  Diagnostic max 2 899 968 B **не является ни измеренным PG peak, ни доказанной
  границей**. Отдельных независимых удержанных PG dev/inode/size controls и
  контролируемых конкурирующих shrink/grow ещё нет.
- [SIGKILL tracee](gate-g/killed-tracee.jsonl.gz) даёт неизвестный результат и
  причины `tracee killed by signal` / `exit outside serialized syscall`.
- [Настоящий mmap work inode](gate-g/unsupported-mmap.jsonl.gz), созданный
  загрузкой libc из work root, даёт `work inode mmap`, unknown. Mapping может
  держать inode после close; его нельзя молча игнорировать.
- [SIGKILL tracer](gate-g/interrupted.jsonl.gz) даёт незавершённый gzip/raw;
  replay отвергает его. EXITKILL включён; disposable контейнер затем удалён.
  Отсутствующий executable observer также реально отказал, ответ Docker
  сохранён в report. Ни один отказ не выдаёт measured zero.

Linux 6.12.76-linuxkit aarch64, pinned PostgreSQL 18.6 image ID/digest в report,
`io_method=sync`, fsync/synchronous_commit/full_page_writes on. Только default
tablespaces, temp_tablespaces пуст; SQL-проверки в gate блокируют custom
tablespaces. Work root — `PGDATA/base/pgsql_tmp`, включая вложенные shared
filesets. SQL temp relations, allocated blocks и device IO не являются
слагаемыми. Образ, настройки и syscall ABI другого профиля не поддержаны.
Ни один опыт не использует рабочие/load volumes: уникальные volume/container,
network none, 1 CPU/1 GiB, SYS_PTRACE, seccomp unconfined; label cleanup всех
попыток с доступом к Docker пуст. Окно capture сейчас — весь lifetime executable,
включая PG startup/shutdown; интерфейса отдельных begin/end ещё нет.

Helper занял 0.2523 s traced и 0.0598 s untraced, около 4.22× end-to-end
в одном контрольном запуске, включая Docker exec. Это наблюдение влияния
instrumentation, не оценка чистого overhead или нагрузки. SQL ordinary/parallel
с capture — 0.9135/0.5068 s; сопоставимого PG baseline пока нет.

## Почему нельзя перейти к ready

1. В реальном PG raw есть `recvmsg` (arm64 syscall 212). Capture сохраняет
   указатели, но не msghdr/control buffer: нельзя установить наличие/отсутствие
   SCM_RIGHTS и полноту ссылок на inode. Это **не утверждение, что PG передал
   work FD**: нужного свидетельства пока нет. PG footer блокирует квалификацию
   с `SCM_RIGHTS not audited`.
2. Default ветка классификатора пока не является проверенным allowlist/denylist
   всех syscall. FD/path crossing, rename/link за work root, первоначальные
   mappings, альтернативные пути удержания inode требуют закрытой модели.
   Особенно нельзя доверять pathname FD после hardlink/rename за границы root.
   Raw не сохраняет pathname из памяти tracee и полную опись исходных maps.
3. Не доказано, что снимки до/после охватывают все промежуточные изменения
   apparent size и последний kernel reference. Поэтому verifier возвращает
   только диагностические максимумы снимков, а **не** lower=upper. Generic
   classifier gate остаётся явным даже у filesystem helper без других issues.
4. Нет полноценного start/begin/end/finish, независимых concurrent controls,
   полного PG overhead baseline и доказательства внешней изоляции писателей.
   Регистрация видимых ptrace children не равна доказательству полноты всех
   потенциальных владельцев/писателей inode.

## Replay и воспроизведение

```sh
env GOCACHE=/tmp/binance-ops031-gocache GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  GOTOOLCHAIN=local go build -o /tmp/ops031-capture \
  docs/measurements/OPS-031/capture/main_linux.go
python3 docs/measurements/OPS-031/gate.py /tmp/ops031-new
python3 docs/measurements/OPS-031/verify-gate.py /tmp/ops031-new --record
python3 docs/measurements/OPS-031/verify-gate.py docs/measurements/OPS-031/gate-g
```

Нужны Docker socket, pinned локальный PG image и новый output directory.
Скрипт не перезаписывает существующие результаты. Hashes raw и точных source
snapshots лежат в report; binary hash фиксирует использованный executable.
Verifier проверяет hashes, монотонность seq/часов, entry/exit, ancestry,
отсутствие overlap у dispatch/release, совпадение зарегистрированной описи,
согласованность inode свидетельств, independent helper stat, PG plan/shared FD
и сохранение unknown. Семь отрицательных replay-проверок: footer, entry,
удалённое родство даже после перенумерации, conflicting inode, false zero,
реальный tracer interruption и tampered hash. Это verifier **исследования**,
он не сертифицирует полноту capture. Hash доказывает сохранность данного raw,
не отсутствие изначальных потерь kernel events.

Первые неуспехи сохранены: gate-a — отказ sandbox Docker до ресурсов (пустой
каталог удалён); [gate-b](gate-b/report.json) — ранняя readiness initdb;
[gate-c](gate-c/report.json) — vfork deadlock/timeout; [gate-d](gate-d/report.json)
— child stop до родства. Их старый report status `running` не означает успех:
это дефект первоначального error reporting, исправленный до финального запуска.
[Gate-e](gate-e/report.json) дал полный capture, но в диагностических FD
снимках ещё были директории: его числа не использовать. В gate-f mode сохранён
и фильтруются только regular files; [запуск](gate-f/report.json) не прошёл
readiness контрола обрыва из-за буферизации gzip. В gate-g prefix flush исправлен,
все заявленные выше проверки прошли. Это не два чистых итоговых опыта OPS-032.

При SIGKILL runner/отказе Docker finally может не исполниться: имя и label
проекта находятся в report, удалять только свои ресурсы. При обычном exception
сохраняется failed report, доступный неполный raw и проверяется cleanup.

Следующий шаг контейнера OPS-031 (начиная с OPS-033): сначала закрыть recvmsg/SCM_RIGHTS и полный classifier
с kernel/file-reference моделью и независимыми контролями, затем отдельно
повторить сессионный gate. До его прохождения не переносить prototype в
collector/OPS-023 и не подставлять диагностические числа в OPS-018.

Основания: [ptrace thread stops и follow options](https://man7.org/linux/man-pages/man2/ptrace.2.html),
[PG18 io_method](https://www.postgresql.org/docs/18/runtime-config-resource.html),
[PG18.6 fd.c: private и shared temporary files](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/storage/file/fd.c).
