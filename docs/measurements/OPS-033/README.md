# OPS-033: ancillary evidence

5 ноября 2026. Raw message evidence готовы; observer по-прежнему unknown,
qualification blocked, peak/lower/upper=null. Общий runner и gate-g не менялись.

[Capture, контракт для OPS-034 и воспроизведение](../../../infra/postgres/measure/ancillary/README.md).

## Реальные опыты

Три отдельных disposable PG18.6 volume, pinned image
`sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc`,
Linux 6.12.76-linuxkit aarch64, Go1.26.3. io_method=sync,
fsync/synchronous_commit/full_page_writes=on, только default tablespaces,
1 CPU/1 GiB, network none. В каждом опыте ordinary sort spill и настоящий
Parallel Hash с двумя launched workers и shared filesets. Команды, kernel,
image/settings, hashes точных source/binary/raw и пустые container/volume/network
cleanup — в report.json. Control ABI в независимом stdout: 56/64/16 bytes.

| Опыт | Filesystem entry/return pairs | SCM FD stat witnesses input/output | PG message calls | traced helper / baseline, s | ordinary / parallel PG, s |
| --- | --- | --- | --- | --- | --- |
| [run-a](run-a/report.json) | 24 | 10 / 8 | 2 recvmsg | 0.3171 / 0.2650 | 0.7183 / 0.3830 |
| [run-b](run-b/report.json) | 24 | 10 / 8 | 2 recvmsg | 0.3038 / 0.2497 | 0.6933 / 0.3816 |
| [run-c](run-c/report.json) | 26 | 11 / 9 | 2 recvmsg | 0.3606 / 0.2845 | 0.6974 / 0.3832 |

В пары включены неуспехи и restart, не только completed transfers. Все четыре
вида вызовов встречаются в filesystem controls; в PG встречается только recvmsg:
в каждом опыте возвраты 156 и 20 B, control length=0, flags=0, SCM_RIGHTS нет.
Это свидетельство данных конкретного опыта, не общий запрет PG передавать FD.
Helper baseline содержит те же sleeps, Docker exec, JSON/fstat, поэтому это
wall time с instrumentation, не чистая стоимость ptrace и не PG overhead baseline.

[Финальные checks](run-c/checks.json), [independent fstat](run-c/filesystem.txt),
[FS raw](run-c/filesystem.jsonl.gz), [PG raw](run-c/postgres.jsonl.gz),
[parallel plan](run-c/parallel-plan.txt), [PG log](run-c/postgres.stderr).
Run-a — первая реализация (helper vet выявил unsafe.Pointer conversion);
run-b — helper держит control buffers напрямую, явные capture issues добавлены;
run-c дополнительно передаёт unlink-open FD отдельному процессу. Точные source
snapshots каждого запуска сохранены как .txt, исторические версии не заменены.
Replay усилен после первых captures; текущий verify.py — итоговый проверяющий.

## Контроли и отрицательные проверки

- Обычный work FD 4096 B/nlink=1 и unlink-open 8192 B/nlink=0; independent
  helper fstat сопоставлен с sender-entry/receiver-return `/proc` stat по
  dev/ino/size/nlink/mode и номерам FD. Path не определяет идентичность.
- Несколько FD в одном SCM_RIGHTS; no-control; 20-byte recv control с одним
  из двух FD и MSG_CTRUNC; нулевая ёмкость с MSG_CTRUNC. Доставленная часть
  сохранена, потерянные ссылки не объявлены отсутствующими.
- Реальные EBADF socket/right, EFAULT iovec и msghdr address=1, EAGAIN пустого
  recvmmsg; sendmmsg/recvmmsg partial N=1 из 2 и full N=2, msg_len=1.
- Блокирующий recvmsg получает SIGUSR1, raw содержит restart -512 и новое
  entry с последующим успехом. Отказ не декодирует stale output.
- Run-c передаёт unlink-open FD другому PID через inherited socket; источник
  не входит в ExtraFiles ребёнка. Получатель независимо подтверждает inode,
  size=8192/nlink=0. Это ещё не модель очереди и последней kernel reference.
- Первое отрицательное replay исследование обнаружило реальный дефект:
  substituted sender inode принимался после пересчёта hash. Исправлено
  сверкой каждого sender и всех receiver witnesses с независимыми controls.
- Итоговые десять tamper controls во всех трёх каталогах: missing message,
  header, control bytes, FD stat; substituted inode; снятый MSG_CTRUNC;
  подменённая input capacity; отсутствующий output, entry; false peak=0.
  Проверяется полный artifact verifier на временной копии с **пересчитанным
  SHA-256 raw и перенумерованным seq**. Все отвергнуты.

```sh
python3 infra/postgres/measure/ancillary/verify.py docs/measurements/OPS-033/run-a --record
python3 infra/postgres/measure/ancillary/verify.py docs/measurements/OPS-033/run-b --record
python3 infra/postgres/measure/ancillary/verify.py docs/measurements/OPS-033/run-c --record
python3 docs/measurements/OPS-031/verify-gate.py docs/measurements/OPS-031/gate-g
```

Все replay PASS; старый gate-g сохраняет unknown и семь прежних отрицательных
контролей. Linux arm64 cross-build и go vet двух новых executable — PASS.
Новый runtime приложения, inode history, classifier или численные границы
не реализованы. Следующий интерфейс — message events для OPS-034; очередь
сокета, общие FD tables/memory и ссылки после close остаются нерешёнными.

## Сохранённое происхождение

Коммит включает прежнюю декомпозицию и исследовательские артефакты OPS-030/031
без изменения их статусов. Два исходных stdout OPS-030 имеют пустую строку в
конце: Git whitespace check их отвергает как новые текстовые файлы. Их точные
байты сохранены lossless gzip; исходные .txt в текущем дереве не менялись и
остались вне Git. После нового checkout перед старым verify-research распаковать:

```sh
gzip -dk docs/measurements/OPS-030/preflight/session-0.txt.gz
gzip -dk docs/measurements/OPS-030/preflight/session-1.txt.gz
```

Hashes прежнего report соответствуют распакованным bytes. Gate-g не менялся.
