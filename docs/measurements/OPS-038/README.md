# OPS-038: inventory/census checkpoints

5 ноября 2026. [Capture/control/verifier](../../../infra/postgres/measure/inventory/README.md).
Новая версия raw сохраняет независимый census и все видимые FD/maps, включая
владельцев вне ptrace lineage и inode после rename за work root/unlink/close.
Это не квалификация observer: consistency unknown, inventories incomplete,
peak/lower/upper=null, qualification blocked. Старые raw не изменены.

## Опыты

- Исходный Red: `python3 docs/measurements/OPS-037/check.py` подтверждает
  сохранёнными helper/raw потерю 8192 B после rename и отсутствие mappings/census.
  Новый `replay` отвергает этот raw: `missing OPS-038 schema`. OPS-033 run-c
  по полному ancillary verifier и всем 10 rehashed controls проходит.
- `run-a`: первый новый реальный capture, exit=0, cleanup пуст. При полном
  verifier обнаружена слабость negative control: `tamper accepted
  substituted-FD-inode`. Мутация попадала в не привязанный к helper промежуточный
  checkpoint. Это не атомарное свидетельство; прежний checkpoint объявлен unknown.
  Negative control привязан к точному getpgid checkpoint независимого helper;
  дополнительно сохранены directory listings для проверки полноты path walk.
  Исторический run-a не считается финальным проверенным контрактом.
- `run-b`: повтор с directory witnesses и привязанным tamper control.
  Исходники capture/helper/runner/verifier, raw и report сохранены в каталоге;
  это промежуточный опыт. Для актуальной проверки использовать run-c.
- `run-c`: финальный disposable capture; [report](run-c/report.json) хранит
  точные команды, kernel/image, binary/source/raw hashes и cleanup; независимые
  [helper](run-c/helper.stdout) и [external owner](run-c/external.stdout).
  [Проверки](run-c/checks.json) создаются полным verifier, включая rehashed/seq
  tamper controls: PASS, 448 checkpoints и 17 мутаций. Census изменился внутри
  1 checkpoint; 0 census read errors, 3100 FD-entry и 218431 map-entry read/stat
  ошибок сохранены. Полный ordinary/parallel PostgreSQL не запускался — OPS-040.

В каждом опыте собственный контейнер, network none, 1 CPU/512 MiB,
SYS_PTRACE/seccomp unconfined, tmpfs /probe 16 MiB. Рабочие volumes не затронуты.
Все containers/volumes/networks с соответствующим project label после cleanup
отсутствуют. ABI Linux arm64 LP64, kernel LinuxKit; точная версия в report.

## Что проверяет control

Семь getpgid checkpoints привязаны к независимым fstat/maps/task witnesses
главного thread. Один inode имеет 4096 B до/после rename, 8192 B после роста,
nlink=0 после unlink; FD target остаётся `/probe/outside (deleted)`.
После mmap/close ни один FD главного thread не держит этот inode, но raw maps и
независимый maps helper сохраняют тот же mapping. После munmap он исчезает.
Path walk сохраняет исходное и внешнее имя, пока оно существует.

Независимый от ptrace процесс держит отдельный inode 12288 B: он виден в census
и FD inventory как external; до последнего checkpoint выходит и исчезает.
Новые/исчезнувшие PID/TID/starttime и изменения census внутри checkpoint
вычисляются verifier. Реальный failed read `/proc/2147483647/status` сохраняется,
как и прочие ошибки procfs/map_files/stat; они не превращаются в пустые ссылки.

Ни getpgid stop главного thread, ни одинаковые census не останавливают всех
владельцев. Проверка подтверждает fixture bindings и явную неполноту, а не
атомарность procfs. Host/kernel/socket references и aliases за namespace
не покрыты; отсутствие FD/path/nlink не доказывает last reference. PG input
witness намеренно отсутствует и сохранён как ошибка; custom tablespaces без
покрытия блокируют опыт. Численные bounds и старые отчёты не переквалифицированы.

## Воспроизведение

```sh
python3 infra/postgres/measure/inventory/verify.py docs/measurements/OPS-038/run-c
python3 infra/postgres/measure/inventory/run.py /tmp/ops038-new
python3 infra/postgres/measure/inventory/verify.py /tmp/ops038-new --record
```

Схема и контракт для OPS-039 — в README inventory. Новый lifecycle scheduler,
clone3/nonleader exec остаются OPS-039; queue/reference fixtures и PG — OPS-040.
