# OPS-037: readiness preflight владельцев inode

5 ноября 2026. OPS-037 разделён до ready на OPS-038 → OPS-039 → OPS-040.
Это новые диагностические Linux filesystem/process controls, не готовый
inventory capture и не повтор ordinary/parallel PostgreSQL.
Peak/lower/upper=null, qualification blocked. Старые OPS-033 raw не менялись.

## Результат

[Проверяющий скрипт](check.py), [проверенные наблюдения](analysis.json),
[команды, hashes, kernel и cleanup](preflight/report.json).

- Linux 6.12.76-linuxkit aarch64, Go1.26.3, pinned postgres:18.6-bookworm image,
  network none, 1 CPU/256 MiB, SYS_PTRACE/seccomp unconfined. Собственный tmpfs
  /probe на 16 MiB; binaries/raw в /tmp контейнера. Контейнер disposable;
  рабочие PG volumes/сервисы/бизнес-fixtures не используются.
- [Независимый helper fstat](preflight/references.stdout) связывает один inode
  с исходными 4096 B/nlink=1, rename /probe/work/source → /probe/outside,
  ростом до 8192 B и unlink/nlink=0. Затем mmap удерживается после close,
  запись mapping[0]=42 успешна, marker open/close даёт snapshots в этом интервале,
  после чего munmap завершён. Helper source сохранён [здесь](preflight/control.go.txt).
- [Исходный capture raw](preflight/references.jsonl.gz) имеет успешный footer
  и проходит текущий ancillary replay OPS-033. До rename inode есть в snapshots,
  но 8192 B после rename не встречаются ни в одном snapshot. Check сопоставляет
  успешные mmap/close/munmap по PID/FD/address с mapping-only интервалом;
  snapshots в нём не содержат этого inode и вообще не имеют maps/census.
  Ancillary replay верно оставляет unknown/blocked: это не ошибка его узкого
  message-контракта и не доказательство owner completeness.
- Отдельный nonleader exec: [baseline stdout](preflight/exec-baseline.stdout)
  подтверждает tid != pid и успешное завершение без tracer (exit=0).
  [Traced stdout](preflight/exec.stdout) также подтверждает tid != pid,
  но capture не завершился за 8 s. [Независимый procfs](preflight/exec-processes.txt.gz)
  показывает true с прежним TGID, одним thread и State=t/tracing stop.
  [Partial gzip](preflight/exec.partial.jsonl.gz) содержит только initial flush,
  без EOF/footer; полный load его отвергает. Точная причина timeout этим
  buffered raw не устанавливается. До этого наблюдался также timeout 45 s;
  его [отчёт](preflight/attempt-timeout-report.json) не содержит stdout/partial raw,
  поэтому основное воспроизведение — финальный опыт с сохранёнными witnesses.
- В исходном [capture source](preflight/capture-main_linux.go.txt) event==4
  не обрабатывает former TID, pending syscall и gate/queue transition отдельно.
  Это существенная новая lifecycle работа. Linux документирует смену thread ID
  и получение former TID через PTRACE_GETEVENTMSG при PTRACE_EVENT_EXEC:
  [ptrace(2)](https://man7.org/linux/man-pages/man2/ptrace.2.html).
  Это основание проверить lifecycle, но не установленная причина timeout.
- Первое выполнение не смогло найти binary после docker cp в tmpfs;
  [setup-failure](preflight/setup-failure/report.json) сохранён отдельно.
  Исправлено размещением binaries/raw в /tmp контейнера. Во всех сохранённых
  report cleanup container/volume/network пуст. Последний runner удаляет
  контейнер и после timeout; исходный incomplete gzip не рекламируется как raw
  завершённого опыта.

Procfs status содержит исходный trailing space в строках Groups. Его точные
байты сохранены lossless gzip; report хранит hashes gzip и распакованного файла.
Runner source по-прежнему отражает исходное получение .txt, упаковка выполнена
после опыта без изменения witnesses.

## Проверки и воспроизведение

Из корня репозитория:

```sh
python3 docs/measurements/OPS-037/check.py
python3 infra/postgres/measure/ancillary/verify.py docs/measurements/OPS-033/run-c
```

Первый скрипт проверяет hashes сохранённых артефактов, независимый helper,
потерю rename/mapping evidence, успешный untraced baseline, tracing stop,
неполноту gzip и cleanup. Он воспроизводит **наблюдённые ограничения старого
capture**, а не сертифицирует будущий raw-контракт OPS-038–040. Второй — полный
verifier зависимости OPS-033 с десятью rehashed tamper controls; PASS.

Точные runner/source snapshots и SHA бинарников — в report. Сборка:

```sh
env GOCACHE=/tmp/binance-ops037-gocache GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  GOTOOLCHAIN=local go build -o /tmp/ops037-preflight-capture ./infra/postgres/measure/ancillary
cp docs/measurements/OPS-037/preflight/control.go.txt /tmp/ops037-preflight.go
env GOCACHE=/tmp/binance-ops037-gocache GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  GOTOOLCHAIN=local go build -o /tmp/ops037-preflight-control /tmp/ops037-preflight.go
```

Для нового опыта использовать копию [точного runner](preflight/run.py.txt)
с новым output path; сохранённый runner содержит путь исторического каталога,
поэтому его напрямую поверх сохранённых raw не запускать. Пример подготовки
нового runner без перезаписи артефактов (output не должен существовать):

```sh
python3 - <<'PY'
from pathlib import Path
source = Path('docs/measurements/OPS-037/preflight/run.py.txt').read_text()
source = source.replace("out=Path('docs/measurements/OPS-037/preflight')",
                        "out=Path('/tmp/ops037-preflight-new'); out.mkdir(exist_ok=False)", 1)
Path('/tmp/ops037-preflight.py').write_text(source)
PY
python3 /tmp/ops037-preflight.py
```

Этот runner сохраняет фактические команды и finally cleanup. SIGKILL runner
может обойти finally; для ручного удаления использовать только уникальные
project/label из его report. Проверка завершённого observer/PG gate здесь не
выполнялась, полнота census/shared state/queued references не доказана.

## Граница следующей реализации

[OPS-038](../../backlog/tickets/OPS-038.md) — один полный inventory/census
checkpoint с явным incomplete при недоказанной согласованности и узкими
rename/mapping controls. [OPS-039](../../backlog/tickets/OPS-039.md) — lifecycle
raw/clone3/shared state/nonleader exec и stop ordering.
[OPS-040](../../backlog/tickets/OPS-040.md) — queue-only receive/drop и прочие
reference controls, полный artifact verifier, ordinary/parallel PG capture
и входной контракт OPS-034. Все исходные критерии остаются в
[контейнере OPS-037](../../backlog/tickets/OPS-037.md).

Lifetime/last reference, фактический inode reuse, отсутствие внешних aliases,
полнота владельцев и численные bounds этим preflight не проверены.
