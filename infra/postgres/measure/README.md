# Физический collector PostgreSQL (OPS-019)

Из корня репозитория, Docker/Compose с поддержкой `!override`, Python 3.9+, Go из `.go-version`:

```sh
python3 scripts/measure_postgres.py --output /tmp/pg-1000-a --rows 1000
python3 scripts/measure_postgres.py --output /tmp/pg-1000-b --rows 1000
python3 scripts/measure_postgres.py --output /tmp/pg-2000 --rows 2000
python3 scripts/measure_postgres.py --output /tmp/pg-error --fail-after-up
```

Последняя команда должна вернуть exit 1, сохранить failed report с пустыми cleanup lists.
Каталог output должен быть новым. Каждая команда создаёт уникальный проект
`binance-pg-measure-<uuid>` с новым volume и случайным loopback-портом.
Compose/init и `scripts/migrate.py` используются без функциональных fixtures;
явно применяются только `migrations/{api,market_data,matcher,notifications}`.
Рабочие и load проекты/volumes не адресуются. Полный test suite не запускается.
Пароли генерируются в файле 0600 во временном каталоге, удаляются при выходе;
не сохраняются в report, DSN передаются adapter только через environment.
Диагностика postgres/superuser доступна runner, прав runtime-ролей не расширяет.
SIGKILL runner/отказ Docker могут помешать finally: ресурсы искать по
`label=com.docker.compose.project=<project из report>`, удалять только этот проект.

Collector запускает одну доверенную последовательность операций между снимками.
По умолчанию `internal/tools/pgmeasure` вызывает реальный `account.Repository.Create`
для N разных синтетических email и заранее заданного фиктивного hash (это не
проверка password adapter). Проверяются runtime-роль/БД и точное число строк.
Независимые транзакции adapter и реальные индексы/триггеры сохраняются.
Другой executable можно передать последним аргументом:

```sh
python3 scripts/measure_postgres.py --output /tmp/pg-custom --command /path/to/adapter --manifest /path/to/input
```

Получает DATABASE_URL (api), MARKET_DATA_DATABASE_URL, MATCHER_DATABASE_URL,
NOTIFICATIONS_DATABASE_URL только runtime-ролей. Сам fixture отвечает за flush
своих статистик/завершение всех соединений. Команды и stdout сохраняются:
не передавать credentials в argv/stdout. Custom fixture не получает диагностический
DSN; Docker на машине runner остаётся административной границей доверия.
Бизнес-manifests и их квалификация реализуются отдельно, в OPS-021/022/018.

## Границы и единицы

Перед измерением сохраняется warmup snapshot: первые чтения каталогов могут
установить hint bits и породить WAL. Каждый опыт содержит empty и adapter окна: начальный принудительный CHECKPOINT,
ожидание 2 с, before → операция → ожидание 2 с → after → CHECKPOINT → ожидание
2 с → checkpoint. Full-page writes включены: adapter работает в новой checkpoint
эпохе. Дельты operation/checkpoint/inclusive сохраняются раздельно. Checkpoint
до before не списывается операции; финальный входит в inclusive. Счётчики не
сбрасываются. Завершение adapter flush-ит статистику его backend, ожидание
учитывает обычную задержку публикации ≥1 с; фоновые процессы могут публиковать
позже. Это ограничение счётчиков, а не обещание точного barrier для всех backend.
Автовакуум/bgwriter/автоматические checkpoint не отключаются: их сырые counters
и дельты сохраняются, шум сравнивается с empty и повтором, автоматически не вычитается.
fsync/synchronous_commit/full_page_writes остаются on, включаются только timing.

- `*-{before,after,checkpoint}.json.gz` — сырые снимки четырёх БД; свежий psql на
  каждый снимок, без кэша прежней транзакции. Снимки последовательные, не атомарные:
  started_at/ended_at каждого SQL обозначают окно. Cluster counters/WAL берутся
  только из api, никогда не суммируются по четырём копиям. Доступ к счётчикам
  сам влияет на статистику; empty содержит эту же диагностику.
- `*-schema.sql.gz` — schema-only pg_dump каждой БД, report хранит применённую
  history и SHA-256 всех migrations, collector/fixture/Compose/init, версии,
  image ID/digest/platform и лимиты контейнера. Source commit + dirty фиксируют
  базу; hashes указывают фактически измеренный код. Это локальный Docker-профиль
  1 CPU/1 GiB, не паспорт LOAD из DEC-008.
- `relation_bytes`: суммарные длины файлов каждой физической relation по
  schema/kind/fork; raw сохраняет OID/name и связь toast_oid. Heap `r`, индекс `i`,
  TOAST heap `t`, sequence `S`, materialized `m`; main/fsm/vm/init раздельно.
  TOAST и его индексы перечислены сами по себе, pg_total_relation_size не
  прибавляется. Системные каталоги также сохранены. Нет двойного счёта.
  pg_database_size — отдельный gauge для сверки, не слагаемое relation_bytes:
  включает другие файлы/каталоги. shared relations присутствуют в снимках всех
  БД: для cluster total дедуплицировать raw shared=true по OID, **не** складывать
  database totals без анализа общей области (этот collector cluster total не выводит).
- Размеры — байты, не KiB; рост страниц квантован `block_size`, малый INSERT
  может иметь нулевую дельту размера, занимая свободное место существующей страницы.
  Это длины файлов, не allocated filesystem blocks, SSD amplification или RSS.
- `wal_insert_bytes` — разница insert LSN (логический диапазон WAL с framing/padding),
  `wal.wal_bytes` — отдельный счётчик генерации. `wal_segment_file_bytes` — сумма
  длин файлов pg_wal (выделенный пул сегментов); дельта может быть нулевой или
  отрицательной при recycling/removal. Это не logical WAL и не сумма по БД.
- `temp_bytes/temp_files` — накопленный объём/число temp work files из
  pg_stat_database; `pg_stat_io` object=temp relation даёт отдельно операции и
  read/write/extend bytes/pages согласно столбцам PG18. pg_ls_tmpdir сохраняет
  живые default-tablespace temp файлы. Custom tablespaces блокируют проход.
  Пик удалённых файлов между снимками **unsupported** и блокирует квалификацию пика.
- `pg_stat_io` сохраняет backend_type/object/context, writes/write_time,
  writebacks/writeback_time, fsyncs/fsync_time и bytes; checkpointer/bgwriter/WAL
  counters хранятся отдельно. Счётчики — количество, время — ms (operation_seconds
  — s). pg_stat_wal PG18 не подменяет IO fsync counters. Метод wal_sync_method
  записан: при open_datasync/open_sync write уже синхронный, нулевой explicit fsync
  не означает бесплатную durability. OS fsync time не является device latency.
- NULL в raw и `unsupported` в numeric delta означает неподдерживаемую метрику
  для конкретного backend/object/context, никогда не ноль; компонент нельзя
  квалифицировать по ней. Нулевой поддерживаемый счётчик означает отсутствие
  наблюдённой активности. Все stats_reset проверяются между границами; изменение
  прерывает проход. Null reset stamp без изменения допустим для неактивных counters.

Report имеет статус `collected-not-qualified`: успешный collector не доказывает
верхнюю стоимость, квоты, будущие бизнес-операции или нагрузочный SLO. Недоступные
компоненты перечислены в unsupported; они закрывают соответствующую квалификацию.
Проверка размеров/шумов и результаты контрольных опытов —
[docs/measurements/OPS-019](../../../docs/measurements/OPS-019/README.md).

Семантика: [PG18 statistics](https://www.postgresql.org/docs/18/monitoring-stats.html),
[relation/WAL functions](https://www.postgresql.org/docs/18/functions-admin.html),
[WAL/checkpoint](https://www.postgresql.org/docs/18/wal-configuration.html).
