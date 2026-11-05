# OPS-040 — reference controls и PostgreSQL evidence

5 ноября 2026. [Контракт/воспроизведение и вход OPS-034](../../../infra/postgres/measure/references/README.md).
[Итоговый run-c](run-c/report.json): новый disposable Linux arm64/PG18.6,
два SQL режима в одном чистом cluster, capture final postmaster от exec до shutdown.
Это raw evidence, **consistency unknown, qualification blocked, bounds=null**.

## Реальные controls

Три независимых серии: queue-only → receive; queue-only → MSG_CTRUNC/drop;
unlink/mmap/close → mapping-only → munmap. Sender закрывает source FD до
каждого queue-only checkpoint; нет source pathname, FD или mapping, но FIONREAD
показывает 1 queued byte. Receive подтверждает исходные dev/ino/size/nlink
независимым fstat, drop действительно возвращает MSG_CTRUNC без delivered FD.
Socket identities, syscalls/returns и checkpoints связаны полным verifier.
Mapping-only сохраняет независимый maps, запись/чтение byte 42 после close,
исторический fstat nlink=0/8192 B; raw maps остаётся видимым без FD/path.

После discharge/munmap все три серии повторяют pathname и FD=6 с размером
4096 B. Независимые fstat фактически показывают reuse dev=64/ino=208041 во
всех трёх сериях. Это наблюдаемое повторение числовой identity, не готовая
модель inode generations и не доказательство last reference.

Ordinary sort: 2000 rows, work_mem=64kB, max_parallel_workers_per_gather=0,
default temp tablespace, SQL result=2000; traced wall time 13,940265 s.
Parallel sort: тот же input/work_mem, custom `/tmp/work/custom`, forced planner;
EXPLAIN ANALYZE: Workers Planned=2, Workers Launched=2; traced wall time
10,907076 s. Default/custom temp FD, backend/worker inventories, независимый
census/ptrace comparison, settings/io_method=sync, mounts/namespaces/work paths,
raw/source/binary hashes и cleanup сохраняются. Времена включают tracing overhead
и не являются таблицей стоимости или performance-квалификацией.

## Полный verifier и фактические пределы

Capture exit=0; container/volume/network cleanup пуст. Большой PostgreSQL gzip
сохранён **точными исходными bytes**, разбитыми на ordered 32 MiB части;
исходный SHA-256 и размер плюс hashes частей — в report.archives/sha256.
Verifier собирает их и проверяет все lifecycle/inventory records через временный
индекс на диске; исходные записи не переписаны. Исходники verifier на момент
capture и уточнённые `full-*` snapshots сохранены отдельно.

При первом full replay обнаружена terminal-гонка на seq=39059, PID=186:
`status` читает Z/PPid=180, следующий `stat` — X/PPid=0 при прежнем starttime.
Это два несогласованных чтения во время teardown. Узкий branch verifier
сохраняет `terminal_identity_races`, увеличивает incomplete и оставляет blocked;
прочие несовпадения identity отвергаются. Согласованный checkpoint не заявляется.

[Preflight](preflight/README.md) сохраняет timeout/cleanup run-b, без объявления
partial capture завершённым. Итоговый runner ждёт readiness по PostgreSQL stderr,
чтобы polling pg_isready не создавал дополнительные traced backends.

Полный `references/verify.py run-c --record`: PASS, [checks.json](run-c/checks.json).
314 FS и 5660 PG checkpoints; 1 и 9 изменившихся census соответственно.
Сохранены 1873/38307 FD и 104876/3440545 map-entry read/stat errors; они не
превращены в отсутствие ссылок. PG terminal_identity_races=1, incomplete=2;
FS incomplete=7. Пятнадцать rehashed/resequenced мутаций отвергнуты полным
artifact verifier, включая удаление queue-only/mapping-only, census/owner,
согласованное удаление mapping из maps и map_entries, подмену queued inode,
потерю прав/stat, isolation/lifecycle/path/namespace и false completeness.

После изменений memory/terminal handling повторены полные dependency verifiers:
[OPS-038 regression](OPS-038-regression.json) — PASS и 17 мутаций;
[OPS-039 regression](OPS-039-regression.json) — PASS и 17 мутаций.
Cross-build capture/control выполнялся runner; Linux arm64 `go vet` ancillary
capture/control — PASS. Полный Go suite не запускался: прикладной Go-код не менялся.

## Все исходные критерии OPS-037

| Исходный критерий | Evidence | Остающийся предел |
| --- | --- | --- |
| Все FD/maps/stat/path после rename/unlink, операции/args/returns | OPS-038/run-c, OPS-039/run-f; OPS-040/run-c | Procfs errors, shared-state reads не атомарны |
| Независимый process/thread census, identity/родство против ptrace | OPS-038/039 и каждый PG/FS checkpoint OPS-040 | Census races/read errors/external owners сохранены, complete=false |
| Linux/ABI/PG/io_method/work paths/mounts/namespaces/tablespaces/isolation | Environment/raw + PG witness/settings/commands OPS-040 | Namespace не доказывает отсутствие host/kernel aliases; custom owner completeness blocked |
| Alias/dup/fcntl/inheritance/shared FD/exec/exit/rename/unlink | Сохранённые прямые controls OPS-038/039, привязанные dependencies.json | Shared VM/FD barrier отсутствует; inherited-return stops реально не наблюдались |
| Повторение имени/FD/inode; queue-only receive/drop; mmap после close | Независимый helper stdout и raw OPS-040 | Числовой inode reuse наблюдался; general queue pairing/GC/lifetimes ещё unresolved |
| Полный artifact verifier, rehashed/resequenced tamper, ordinary/parallel PG | references/verify.py + lifecycle/inventory full replay и dependency verifiers | Passing artifact не означает полноту или квалификацию |
| Входной контракт OPS-034 с пределами | references/README.md вместе с inventory/lifecycle README | Reducer/last reference/observer/bounds не выполнены |

OPS-034 получает сохранённые raw, независимые points и полный verifier, а не
утверждение замкнутой модели ссылок. Peak/lower/upper=null; внешние aliases,
queues/kernel refs, shared-state atomicity, child clear_tid, ошибки procfs и
полнота custom tablespace references остаются явными блокерами. Старые
OPS-033/038/039 raw не изменены и не переквалифицированы.
