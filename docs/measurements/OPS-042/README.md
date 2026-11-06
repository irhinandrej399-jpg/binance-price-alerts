# OPS-042 — proof contract lifetime envelope

6 ноября 2026. **Результат отрицательный:** OPS-041/run-c сохраняет
положительные references, но не поддерживает lifetime envelope OPS-034.
`bounds=null`, qualification blocked. Новый Linux capture не выполнялся.
Это исследование достаточности свидетельств, а не изменение требований ёмкости.

## Проверяемый вход и воспроизведение

Из корня репозитория:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 infra/postgres/measure/references/verify.py docs/measurements/OPS-041/run-c
PYTHONDONTWRITEBYTECODE=1 python3 docs/measurements/OPS-042/check.py > /tmp/OPS-042-audit.json
python3 -c 'import json; from pathlib import Path; assert json.loads(Path("/tmp/OPS-042-audit.json").read_text()) == json.loads(Path("docs/measurements/OPS-042/audit.json").read_text())'
```

Первая команда проверяет полный исходный artifact, lifecycle/inventory,
независимые helper witnesses и linked OPS-038/039, а также rehashed controls.
Вторая — отдельный streaming audit без импорта candidate reducer OPS-034.
Он проверяет hashes частей и собранного gzip, читает все PG raw inventories,
сопоставляет FD identity/path с независимым census TID/starttime, ищет successful
exclusive opens и adjacent returned-FD stat. Это **не** общий syscall classifier
и не проверка полноты owners. [audit.json](audit.json) сохраняет точный результат.

В raw воспроизведены 17 PG work numeric identities и **ноль** поддержанных
exclusive-create/returned-FD bindings для них. Для `65025:208045` найдены
195/starttime=3996605 (`pgsql_tmp195.0`) и 234/starttime=4001156
(`pgsql_tmp234.1`); точные первые witness seq сохранены в audit.
Это согласуется с [candidate-only OPS-034/run-a](../OPS-034/README.md):
17 unknown-origin candidates, creation=null; первое/последнее evidence этого
кандидата 43222/65168. OPS-042 самостоятельно читает исходный raw и не выводит
корректность поколения из совпадения с результатом OPS-034.

Один и тот же dev/ino в разные моменты совместим как с продолжением incarnation
через скрытый alias, так и с освобождением и reuse. Разные имена, PID, FD или
время между ними не выбирают один из этих вариантов. Ноль exclusive bindings
означает отсутствие конкретного birth proof; не означает, что PG не создавал
файлы или что обычный O_CREAT доказывает новое поколение.

## Один минимальный профиль P и proof obligations

P — **целевой, пока не поддержанный** профиль: записанный Linux arm64 LP64,
PG18.6/io_method=sync, один disposable экземпляр на свежей эксклюзивной FS;
обычные и shared temp files кластером, все его процессы/потоки от exec до exit.
Default и custom roots входят только если лежат в доказанно закрытом файловом
домене. Совпадение container namespaces и network=none этого не доказывает:
volume может быть доступен host, другим mounts и процессам. Старый OPS-041
с custom tablespace не переводится в P задним числом.

Lifetime здесь относится к **incarnation filesystem inode**, не pathname, FD slot,
open file description или одному пребыванию inode в kernel cache. Eviction
связанного именем inode и последующая загрузка не создают новую filesystem
incarnation и не доказывают retirement. Token обязан переживать такую смену
cache object либо весь случай остаётся unsupported.
Отдельно фиксируются интервалы известных references,
интервал потери последнего reference и terminal retirement witness. Kernel
retirement может быть позже последнего пользовательского close; эти моменты
не отождествляются. До отдельного size classifier никаких численных bounds.

| Обязательство | Достаточное свидетельство в P | Что есть в OPS-041 и чего нет |
| --- | --- | --- |
| P0: область и отсутствие потерь | Изоляция FS с момента её создания, отсутствие внешнего доступа/передачи handles, список всех entrants; capture от начала домена, loss counters, shutdown/drain. Любой пробел блокирует весь опыт | Namespaces/mountinfo/census/cleanup есть; host/external aliases, полнота kernel routes и потерь не доказаны |
| P1: birth и привязка наблюдения | Уникальный run-local incarnation token с доказанной границей allocation/reuse; token связан с dev/ino и каждым нужным FD/map/path **в момент наблюдения**. Успешный O_EXCL с защищённым returned-FD binding — альтернативное локальное начало, не глобальная continuity | FS helper даёт шесть отдельных creates; все PG origins unknown. Numeric identity, адрес kernel object без epoch и timestamp сами не token |
| P2: операции и owners | Task identity/starttime, FD-table/VM identity, защищённые pre/post states и результат операции; изменения shared table сериализованы либо причинно упорядочены. In-flight syscalls, restart/partial return отражены отдельно | Args/returns, clone flags, birth/exec, FD/maps доступны. Общего all-task stop/barrier, contents/CLOEXEC/shared-state proof нет |
| P3: alias и inheritance | dup/dup2/dup3/fcntl/reopen привязывают destination к тому же token, fork копирует references, CLONE_FILES/VM делит таблицу/VM без удвоения счёта, exec/unshare/exit меняют именно свою таблицу/VM; link/rename/unlink ведут namespace edges за root | Положительные OPS-038/039 witnesses есть; topology reducer не доказывает содержимое или отсутствие конкурентного mutation |
| P4: kernel references | SCM_RIGHTS queue принадлежит token от успешного send до доказанного receive/discard/GC; mmap держит token до последнего mapping release. Все иные routes либо покрыты, либо их появление обнаружено и блокирует опыт | Queue-only и mapping-only реальные; generic peer/FIFO/GC неизвестны. Полный запрет/обнаружение иных routes не доказан |
| P5: конец | Последний положительный reference даёт нижнюю причинную границу. Привязанный к тому же token terminal kernel witness после закрытия всех reference channels даёт верхнюю границу retirement; до него end неизвестен. Точный last-reference event допустим только при отдельном полном release proof | Ни terminal token witness, ни полный reference ledger нет. Исчезновение из procfs, close/exit/munmap и конец tracer не terminal certificate |

При P0–P5 алгоритм может присваивать наблюдения поколениям, учитывать shared
inode один раз и давать консервативный lifetime interval: birth → terminal
retirement; последнее известное held → terminal ограничивает освобождение.
Он обязан явно сохранять неопределённость внутри интервала, а точный конец
выдавать только при полном release proof. Неупорядоченные события оставляют
несколько возможных histories; journal seq/ns не превращается в kernel clock.
Отсутствие любого обязательства оставляет affected lifetime/envelope unknown.
Даже kernel token не заменяет owner completeness и атомарный size checkpoint.

Достаточность **условная**: P0 замыкает множество источников references,
P1 разделяет поколения, P2/P3 сохраняют известные transitions, P4 не теряет
скрытые holders, P5 ограничивает конец независимо от отсутствия views.
В существующем raw предпосылки не выполнены, поэтому положительного примера
поддержанного P здесь нет. Доказательства production observer не заявляются.

## Независимые FS stages и семантические проверки

`check.py` читает helper stdout, связывает каждый из 13 stages с очередным
getpgid/raw inventory, сравнивает helper maps с raw maps и подсчитывает FD/path
views всех записанных owners. Source fstat после close считается историческим.
Шесть успешных exclusive creates связаны с adjacent returned-FD stat.

| Сценарий | Положительное evidence | Что нельзя заключить |
| --- | --- | --- |
| receive/drop queue-only | Unlinked nlink=0, нет FD/path/map views, FIONREAD=1; full source verifier связывает send/right stat и sender close | «Последний FD закрыт → файл освобождён» опровергнуто |
| receive-delivered | Полученный FD снова показывает source inode; queue пуста | Появление FD не новое поколение; generic queue discharge всех owners не доказан |
| mapping-only | Нет FD/path, helper и raw maps совпадают, mapped=true | «Нет FD → нет reference» опровергнуто; исторический fstat не новая size точка |
| discharged/unmapped | Положительных views больше нет | Возможны отсутствие references и скрытый owner/kernel holder; точный конец неизвестен |
| reused (три режима) | То же pathname/FD/dev/ino, 4096 вместо 8192, новый successful exclusive create | Нельзя слить два creates по числовому равенству. В последовательном helper старое incarnation ушло до reuse, но точный момент/последний owner не наблюдены |

Семантические challenges в audit проверяют claims против независимых фактов:
три ложных окончания при queue/mapping, три ложных merge reused creates и три
ложных retirement по отсутствию views. Это не сравнение сериализованного output
с тем же reducer и не schema tamper. Для последних трёх сохранены две допустимые
интерпретации; **unknown — ожидаемый правильный ответ**, а не провал опыта.
Не утверждается, что hidden owner реально присутствовал или что FIONREAD само
по себе доказывает тип queued object: права/identity связывает full verifier.

## Минимальная capture предпосылка

Нужен [OPS-043](../../backlog/tickets/OPS-043.md): один независимый поток
**incarnation binding + terminal retirement** для закрытого disposable file
домена. Это первая необходимая предпосылка P1/P5, а не скрытая реализация всех
P0–P4 и не гарантия, что один дополнительный tracepoint закроет OPS-034.
Generic eviction/destroy cache-object event не terminal certificate: нужны
доказательства отсутствия оставшихся namespace links и всех reference channels.
Конкретные kernel hooks/доступность/семантика ещё должны быть проверены на
фактическом kernel: название события или kernel pointer не доказательство.
Механизм должен привязывать FS/PG observations к token, удерживать epoch через
reuse и сохранять потерю/неподдержанные случаи. Нельзя подменить его очередным
procfs scan, i_generation без проверки semantics или observer FD pin, который
сам меняет lifetime. Если pin необходим для binding, его reference/release
должен быть явным и исключать ложный terminal; overhead отдельно сохраняется.

OPS-043 поставлен перед OPS-034. После него OPS-034 повторяет readiness:
если P0/P2–P4 всё ещё не доказаны, остаётся planned и выделяет следующую узкую
предпосылку, а не повышает snapshot count и не объявляет supported envelope.
Критерии OPS-034 сохранены. OPS-035/classifier не может исправить эти gaps.

## Итог проверок

Полный references CLI завершился с exit=0: artifact replay, linked OPS-038/039,
15 FS и 5 PG rehashed controls прошли;
[полный stdout](dependency-verification.json) сохранён отдельно. Независимый
audit повторён, его JSON совпал с audit.json. Python compile, ссылки,
backlog check и diff whitespace check прошли. Прикладной Go/runtime и capture
не менялись; Go suite и новый Docker опыт не запускались.
