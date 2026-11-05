# OPS-030 — preflight временных work files PostgreSQL

5 ноября 2026. **Статус split: gate полного наблюдения не пройден.**
Доступ к ptrace подтверждён, но воспроизводимого observer точного пика пока нет.
`temp_peak_live_bytes=null`, qualification blocked. Collectors и business manifests
не менялись; старые отчёты не переквалифицированы.

## Факты опыта

[Report](preflight/report.json), [raw syscall trace](preflight/syscalls.trace.gz),
[диагностический разбор](preflight/diagnostic.json),
[точный исходник выполненного probe](preflight-source.py).
Это исследовательский probe, не штатный runner или raw verifier.

- PostgreSQL 18.6, закреплённый штатный image digest; Linux
  6.12.76-linuxkit aarch64, Docker Desktop. Один новый volume, сеть `none`,
  1 CPU/1 GiB. fsync/synchronous_commit/full_page_writes=on, io_method=worker,
  temp_tablespaces пуст. Установленных custom tablespaces нет.
- Sidecar с strace 6.19 из image OPS-029: отдельный PID namespace PostgreSQL,
  SYS_PTRACE/KILL, seccomp=unconfined, без сети, host PID и mounts данных.
  Прикрепление к postmaster и восьми исходным PostgreSQL процессам проверено
  через TracerPid (полный список в report); `-f` следует новым потомкам.
  Detach завершился кодом 130. Сам по себе этот факт не доказывает полноту raw.
- Две новые SQL-сессии: work_mem=64kB, parallel query отключён; сортировка
  60 000 синтетических строк курсором, FETCH 1, удержание через pg_sleep(5),
  CLOSE/COMMIT и pg_stat_force_next_flush(). Оба сеанса успешно завершились;
  [вывод 0](preflight/session-0.txt), [вывод 1](preflight/session-1.txt).
- До и после: `pg_ls_tmpdir=[]`. Во время удержания обе сессии были в PgSleep,
  а pg_ls_tmpdir показал четыре файла: два по 840 000 B и два по 16 556 032 B,
  всего **34 792 064 B**. Это независимое свидетельство одновременного
  видимого размера, **нижняя граница пика**, а не точное значение пика.
- Счётчики temp_files/temp_bytes: 0/0 → 4/34 792 064. Равенство cumulative
  bytes и held bytes в этом конкретном опыте не устанавливает их общую
  эквивалентность. Эти байты — apparent file lengths; allocated blocks,
  temporary SQL relations, pg_stat_io и device IO здесь не измерялись.
- Сохранены 27 249 строк strace. Диагностический разбор показывает примеры
  перекрытия syscall разных backend. Перекрытие только увеличений само по
  себе не доказывает неоднозначность максимума этого контрольного опыта.
- Cleanup обоих запусков: containers/networks/volumes с уникальной меткой
  отсутствуют. Первый [неуспешный probe](readiness-failure/report.json)
  использовал неверную readiness-проверку readlink /proc/1/exe от root без
  SYS_PTRACE. Это ошибка probe; исправлена чтением /proc/1/comm. Отказ не
  выдаётся за недоступность PostgreSQL или ptrace.

## Почему ready пока нельзя выставить

1. Trace даёт вход/выход syscall, а не момент изменения inode size внутри
   вызова. Для конкурирующих роста и уменьшения/освобождения разных файлов
   нельзя без доказательства принимать порядок завершения syscall за порядок
   изменений. Нужна проверяемая модель допустимых порядков: например, нижняя
   и верхняя границы пика с точным результатом только при их совпадении.
2. В pwritev probe сохранил числовые FD. Не реализовано восстановление
   open/close/наследования/повторного использования FD и общей идентичности
   файла. Удаление pathname не означает освобождение открытого файла.
   Реконструкция должна дедуплицировать его между процессами и учитывать
   последнюю ссылку, truncate, O_TRUNC, exit и повторное использование имён.
3. Две обычные сессии не проверяют parallel workers/shared filesets,
   уменьшение, открытый удалённый файл, потерю tracer, границы окна или
   неподдерживаемый путь записи. Полного raw verifier нет; diagnostic.json
   не является проверкой отсутствия потерь или полноты фильтра syscall.
4. Поэтому этих данных недостаточно для поддержки произвольного фазового
   окна OPS-023. Два чистых итоговых опыта empty/spill/parallel и интеграция
   runner ещё не выполнены. Один текущий preflight не засчитывается за них.

Нельзя утверждать, что среда запрещает точное наблюдение: BTF vmlinux доступен,
ptrace работает. В обычном контейнере без capabilities tracefs events и
bpftrace/bpftool не были доступны; это не проверка работоспособности eBPF с
соответствующими правами. Нужен Linux/Docker с проверенным способом наблюдать
все изменения размера/жизненного цикла inode всех PostgreSQL backend/workers
либо syscall observer с доказанными границами и отказом при неоднозначности.
Выбор/реализация такого observer переданы OPS-031, интеграция — OPS-032; новая архитектура
приложения или изменение durability не требуются.

## Продолжение и воспроизведение

Условный прототип и filesystem lifecycle контроль выполнены ниже. Следующий
шаг — полный capture/reconstruction OPS-031, затем фазовая интеграция OPS-032. Если syscall-интервалы не дают совпадающих границ, результат
unknown, а не оценка под видом точного пика. Sampling допустим лишь как
контрольное нижнее свидетельство, не как основной observer.

Probe запускается из **нового пустого рабочего каталога**, поскольку output
жёстко задан относительно cwd и никогда не перезаписывается:

```sh
mkdir /tmp/ops030-replay
cd /tmp/ops030-replay
python3 /Users/andrey/Desktop/Binance/docs/measurements/OPS-030/preflight-source.py
```

Нужны уже имеющиеся pinned PostgreSQL image и `binance-fsync-observer:ops029`
(сборка описана в [FSYNC.md](../../../infra/kafka/measure/FSYNC.md)).
Результаты появятся в `docs/measurements/OPS-030/preflight` относительно cwd.
SQL и команды Docker находятся в исходнике; report сохраняет параметры,
свидетельства, image IDs, команды и cleanup. Исходник — снимок исследовательского
запуска, без production гарантий timeout всех subprocess и обработки SIGKILL;
при аварии проверять только label project из report/имена из исходника.
Ptrace меняет расписание/длительности; контроль не измеряет максимальную
бизнес-операцию, device IO, нагрузочный профиль DEC-008 или квоты OPS-018.

Основания метода: [ptrace syscall stops](https://man7.org/linux/man-pages/man2/ptrace.2.html),
[strace follow-forks](https://github.com/strace/strace/blob/master/doc/strace.1.in),
[PostgreSQL 18.6 fd.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/storage/file/fd.c).
PostgreSQL различает обычные и разделяемые временные файлы; для последних
жизненный цикл не сводится к закрытию одного backend. Это причина отдельно
проверить shared filesets. Fanotify также не даёт готовой истории размеров:
[однотипные события могут объединяться](https://man7.org/linux/man-pages/man7/fanotify.7.html).

## Повторная сессия: реконструкция и контроль lifecycle

[OPS-030](../../backlog/tickets/OPS-030.md) стал контейнером:
[OPS-031](../../backlog/tickets/OPS-031.md) строит самостоятельный observer с
доказательством полноты и raw verifier;
[OPS-032](../../backlog/tickets/OPS-032.md) использует готовый observer в OPS-023
и выполняет два чистых PG опыта. Новые реализации не начаты. Это разделение
двух самостоятельно проверяемых результатов, а не признание среды недоступной.

### Что удалось восстановить

[Узкий исследовательский исходник](reconstruction-source.py),
[условные события и границы](reconstruction/conditional.json.gz),
[проверка/хэши](reconstruction/checks.json), [verifier исследования](verify-research.py).
Он связывает созданные private pathname с FD, не смешивает повторно
использованные FD и освобождает unlink-open после последнего известного close.
Область ограничена ordinary zero-created файлами, positional pwrite64 и
неперекрывающимся освобождением. Наследование live work FD, shared filesets,
усечение и альтернативные мутации не поддержаны.

После объединения unfinished/resumed разобраны все 17 130 pwrite64 исходного
preflight. Для четырёх private lifetimes получены **условные lower=upper=
34 792 064 B**, ровно независимый held snapshot. Grow применяется в начале
syscall для верхней границы, в конце для нижней; free — наоборот. Размеры
файлов суммируются в каждый момент, а не складываются их индивидуальные пики.
Для одновременных событий верхняя граница применяет grow перед free, нижняя
free перед grow. Время entry расширено на 1 us, exit берётся как максимум
entry+elapsed и resume timestamp плюс 1 us. Гарантия отсутствия скачков realtime
и общей полноты capture не доказана; границы условны также по этой причине.

Первый запуск прототипа не разобрал пробел перед resumed `)`; промежуточное
34 578 432 B отклонено, разбор исправлен до сохранённого результата. Пять
shared writable mappings не имеют достаточной identity evidence, поэтому
итог **status=unknown, temp_peak_live_bytes=null, qualification_blocked=true**.
Совпадение условных границ не снимает эти блокеры. Старый trace не содержит
полного набора link/dup/fcntl/close_range/AIO событий и исходной описи всех FD:
нужен новый capture, а не просто дополнительные regexp к старому raw.
Это не утверждение, что данные старого preflight потеряны или повреждены.

### Независимый filesystem контроль

[Исходник](lifecycle-probe.py), [report/версии/команды](lifecycle/report.json),
[stat control](lifecycle/control.txt), [raw](lifecycle/syscalls.trace.gz).
Новый disposable контейнер image OPS-029, network none, SYS_PTRACE,
seccomp=unconfined; без данных PostgreSQL, bind mounts и постоянного volume.
Strace следует shell и всем его потомкам, `%file,%desc,%process,%memory`.
Контрольный helper использует один файл, дополнительный hard link, открытый FD,
запись, unlink обоих имён, shrink/regrow через `/proc/<shell>/fd/3` и last close.

| Свидетельство | dev/inode | apparent B | nlink |
| --- | --- | ---: | ---: |
| grown | один и тот же | 16 384 | 1 |
| alias | один и тот же | 16 384 | 2 |
| linked_alias после первого unlink | один и тот же | 16 384 | 1 |
| unlinked_open | один и тот же | 16 384 | 0 |
| shrunken | один и тот же | 4 096 | 0 |
| regrown | один и тот же | 12 288 | 0 |

После last close каталог пуст. Строки control содержат реальные dev/inode;
их равенство, размеры и link counts проверены независимо от reconstruction.
Из контролируемой последовательности helper известен peak 16 384 B; это
не измеренный PostgreSQL peak и не замена двух чистых PG volumes. Прототип
этот raw оставляет unknown (среди причин linkat, dup3, ftruncate). Эта проверка
показывает реальные lifecycle случаи, которые предстоит покрыть OPS-031.

Первый sandbox запуск helper не получил доступ к Docker socket, ни один
ресурс не был создан; итоговая проверка выполнена повтором с разрешённым
Docker доступом. У успешного запуска container/network/volume label cleanup
пуст. Docker/ptrace доступны; оценка instrumentation overhead ещё не выполнена.

### Воспроизведение и ограничения проверки

```sh
python3 docs/measurements/OPS-030/reconstruction-source.py \
  docs/measurements/OPS-030/preflight/syscalls.trace.gz /tmp/ops030-bounds.json
python3 docs/measurements/OPS-030/verify-research.py
```

Второй вызов повторяет 8 проверок: hashes/replay, совпадение условных границ
с held, сохранение unknown, missing unlink, missing resumed syscall, SIGKILL,
реальные stat lifecycle evidence и отказ прототипа на этом raw. Он проверяет
сохранённое исследование, **не полноту observer**. Удалённый целый write, который
не меняет конечный размер, невозможно выявить только по оставшимся размерам;
хэши фиксируют данный raw, но не доказывают полноту его первоначального capture.

`lifecycle-probe.py` запускается только при отсутствующем каталоге `lifecycle`
рядом с исходником. Для нового опыта скопировать source в новый пустой каталог
в `/tmp`, запустить там: исторические raw не перезаписываются. Probe не является
production lifecycle runner; SIGKILL/Docker outage может помешать finally,
поэтому проверять ресурсы только по сохранённому project label. Новый source
намеренно сохранён как исследовательский артефакт, collectors не менялись.
