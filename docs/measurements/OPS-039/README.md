# OPS-039 — lifecycle/shared state/nonleader exec

5 ноября 2026. [Контракт и воспроизведение](../../../infra/postgres/measure/lifecycle/README.md).
[Общая проверка](check.py), [результаты](checks.json). Peak/lower/upper=null,
qualification blocked; PG здесь не запускался.

## Диагноз и исправление

[Red](red/report.json) повторяет исходный helper OPS-037 без изменений:
без tracer `/bin/true` завершается с кодом 0; под tracer — timeout 8 s.
Добавлены только принудительно сбрасываемые диагностические stop witnesses.
Partial gzip сохраняется неполным, без footer; check читает лишь сохранившиеся
полные строки, не объявляя raw завершённым. Hashes, исходники и команды в report.

PTRACE_EVENT_EXEC доставлен для нового leader TID 35 с former TID 38.
После этого pending exec и active gate остались у 38. `/bin/true` под TID 35
успел выполнить несколько syscalls и поставил `openat` в очередь за исчезнувшим
38. Последний stop содержит active=38 и pending[38]=execve; независимые
procfs status/stat и отдельная опись процессов показывают `true`, tracing stop.
Отдельная опись procfs упакована lossless gzip после capture: сохранены hashes
сжатых и исходных байтов, включая trailing spaces в Groups. Это установленная
причина данного воспроизведения; исходный buffered partial
OPS-037 сам по себе её не доказывал.

[Точный исходный helper после исправления](exact-exec/report.json): тот же
nonleader exec в `/bin/true`, exit=0 за 0,255222 s, завершённый raw, успешный
полный lifecycle/inventory replay, cleanup пуст. Полные контрольные точки
в новом исполняемом helper до/после exec проверены отдельно в итоговом run-f.
Переход TID описан в [Linux ptrace](https://man7.org/linux/man-pages/man2/ptrace.2.html).

## Итоговые controls

[Run-f](run-f/report.json) содержит актуальные source/verifier snapshots,
SHA-256 бинарников/raw, точные команды, kernel/image, независимый stdout.
Pinned postgres:18.6-bookworm — только Linux окружение; network none,
1 CPU/512 MiB, private 16 MiB tmpfs, SYS_PTRACE/seccomp unconfined.
Контейнер/volume/network cleanup пуст; рабочие БД/volumes не затрагивались.

- Наследование inode через clone и clone3 проверено независимым fstat/census
  и FD inventory ребёнка до close. Оба syscall успешно создали изолированный
  и shared FD/VM варианты. Shared child закрыл общий FD и записал 42 в общую
  память; isolated child не изменил FD/memory родителя. Child exit=0.
- Подтверждены fcntl F_DUPFD_CLOEXEC, dup3, procfs reopen, ошибочные clone3 pointer
  и size. Flags clone3 берутся из сохранённых структур, scalar args сверяются
  с исходным kernel syscall-info. Реальный syscall restart при clone в run-d
  обнаружен, сохранён и учтён verifier без потери промежуточного return.
- Ранние child SIGSTOP получены настоящими waitpid до обработки parent event.
  Последующий child stop/lineage/census сверяются. Inherited-return branch
  сохранён и проверяется replay, но реальных inherited-return stops в этих
  прогонах не было: kernel начинал ребёнка с нового syscall entry.
- Nonleader exec переходит на leader identity с переносом pending exec/gate,
  явным retirement siblings и очисткой queue. Независимый helper после exec
  подтверждает PID/TID/starttime, код завершения 0. Ошибки procfs при exit и
  PTRACE_SYSCALL_INFO_NONE сохраняются и оставляют evidence incomplete.
- Полный verifier + 17 rehashed/resequenced controls проверяют flags/bytes,
  родство/child, returns/restarts, ранние stops, pathname, kernel info/его размер,
  exec transition/former/gate/pending/siblings, census и false completeness.
  Проверяются два raw, независимые helper stdout и все hashes/cleanup.

Run-a сохраняет найденный stop=NONE при exit_group и первоначальный panic;
run-b/c — промежуточные контракты/verifier, run-d/e — промежуточная проверка
parent-side pointed-to ID outputs и restart. Они не являются итоговым входом
OPS-040. Снимки отражают код на момент capture, в том числе исправленные затем
негативными controls пробелы verifier; использовать run-f и текущий verifier.
Старые raw OPS-033/038 не изменялись, их replay проходит.

## Граница доказанного

Inventories consistency=unknown/complete=false: нет all-owner/shared FD/VM
barrier, procfs read errors и census races сохранены. Shared flags или успешный
helper не доказывают атомарность чтений. Parent-side PIDFD/PARENT_SETTID bytes
предусмотрены протоколом; отдельного успешного PIDFD-control здесь нет.
Child TID/clear_tid lifetime явно unresolved. Unsupported ABI/размер/чтение
даёт incomplete/rejection, не успешное evidence. Kernel/queue references,
external aliases и custom tablespaces не сертифицированы. Никакой last-reference
модели или численной границы в OPS-039 нет.

Следующий разрешаемый тикет — OPS-040: reference controls, полный итоговый
verifier и ordinary/parallel PG. Существующие gates/unsupported не снимаются.
