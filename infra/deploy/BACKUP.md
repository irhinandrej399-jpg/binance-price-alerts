# Cold backup и восстановление одного хоста

OPS-076 использует **полную остановку** writers, consumers, ingress, cleaner,
PostgreSQL и Kafka. Downtime длится от начала `backup` до завершения `start`;
включает чтение обоих volumes, шифрование и копирование. Во время остановки
внешние Binance trades не принимаются и не восстанавливаются backfill.
RPO — время последней успешной копии; RTO зависит от объёма/диска/запуска.
Это восстановление после потери хоста, без HA, PITR или zero downtime.

## Что сохраняется

- `postgres-data.tar`: весь остановленный PostgreSQL cluster, четыре БД,
  migration/runtime роли, SQL WAL, journal/outbox, sessions, subscriptions,
  matcher projections/checkpoints, notifications jobs/receipts и replay-pins.
- `kafka-data.tar`: весь остановленный broker/KRaft, ClusterID/TopicIDs,
  partition data/log-start/offsets, transaction/group metadata. Retention и
  compaction не включаются; topics при restore не пересоздаются.
- `private.tar`: DATA_DIR без transient smoke signals и maintenance lock.
  Включены `inventory/sources.json`, secrets/config, PKI (в том числе offline
  development CA при его наличии), ingress/Caddy ACME state, `cleaner/`.
- `manifest.json`: версия формата, UTC timestamp, точный Git revision,
  закреплённые PostgreSQL/Kafka images из Compose, Docker platform, prefix/mode, перечень
  реестров, bytes/seconds и SHA-256 **всех трёх частей одного snapshot**.
  Manifest записывается последним после fsync; без него копия незавершена.

Physical restore требует тот же checkout, PostgreSQL 18.6, Kafka 4.3.1 и
закреплённые в Compose image digests, ту же архитектуру хоста и numeric UID/GID
внутри образов. Сохранить checkout/commit и образы в доступном registry/offline
хранилище. Для backup production source должен быть закоммичен и соответствовать
запущенной версии: не выполнять backup между checkout новой версии и её deployment.
Сохранить исходный deploy.env отдельно в приватной копии настроек; для нового
хоста заменить PROJECT/DATA_DIR/API_PORT, сохранив PREFIX/MODE/PUBLIC_DOMAIN и
осознанно выбрав INGRESS_SUBNET. Команда не архивирует env вне DATA_DIR. Версии не
обновлять одновременно с восстановлением. Upgrade — отдельная операция после
успешного recovery. Копия включает роли/пароли, независимые live dumps четырёх
БД и копирование работающего Kafka её не заменяют.

## Подготовка и остановка cleaner

Все operator lifecycle-команды `deploy.py` и backup используют постоянный
`DATA_DIR/maintenance.lock` (локальный flock, отказ немедленный при занятости).
Ручные `docker start/exec`, внешние SQL/Kafka writers, provisioning, ротацию
секретов и изменение topic lifecycle во время операции исключить организационно.
Команды не являются распределённой блокировкой для администратора Docker.

Все авторитетные state-dir cleaner этой среды должны находиться **под
DATA_DIR/cleaner/**, например `cleaner/prices-p0/registry.json`. Не делать
вторые реестры. Перед первым backup остановить расписание cleaner и дождаться
завершения последней команды. Если реестры уже вне DATA_DIR, при остановленном
cleaner перенести весь каталог с прежним state/lock, изменить единственный
state-dir в расписании и запретить исполнителей по старому пути. Проверить полный
inventory всех source/partitions по [cleaner runbook](../../cmd/kafka-cleaner/README.md).

Backup берёт **те же** `registry.lock` всех найденных реестров до остановки
writers и держит до seal. Busy/missing lock — отказ, cleaner не прерывается
насильно. Сохраняются exact consumers/generations, retired tombstones и durable
deletion floors, включая intent с неизвестным результатом DeleteRecords.
Во время backup нельзя создавать новые state-dir/init/retire вне этого набора.
После restore нельзя подмешивать более новый или старый cleaner registry либо
только SQL/Kafka из другой копии. Восстанавливается весь согласованный набор;
floor и offsets не обнуляются. Более поздняя очистка исходного broker не влияет
на отдельную полную cold копию его прежних данных.

Если cleaner **никогда не был provisioned**, разрешён явный `--no-cleaner`.
Он отвергается при наличии state/содержимого cleaner root. Отсутствие реестра
после его потери не повод использовать этот флаг: остановить cleanup и разбирать
утрату, не делать новый init. Включение cleaner в deployment — OPS-077.

## Создание и проверка

Запускать от того же доверенного оператора Docker. DATA_DIR и staging — 0700,
private files — 0600; локальный диск должен вмещать полные volumes и копию.
Новый каталог backup должен отсутствовать и находиться вне DATA_DIR.

```sh
umask 077
python3 infra/deploy/backup.py --env /private/deploy.env backup /backup-staging/20261109T030000Z
# Только для ещё не provisioned cleaner: добавить --no-cleaner.
python3 infra/deploy/backup.py verify /backup-staging/20261109T030000Z
# Зашифровать и проверить внешнюю копию, затем:
python3 infra/deploy/deploy.py --env /private/deploy.env start
python3 infra/deploy/deploy.py --env /private/deploy.env status
```

Команда сама останавливает приложения в обратном порядке, затем SQL/Kafka,
проверяет exit 0 приложений/PostgreSQL и отсутствие running mounts обоих
volumes. Kafka image штатно возвращает 143 после SIGTERM: допускается 0/143,
но обязательно проверяется валидный `.kafka_cleanshutdown` с brokerEpoch.
OOM/137 и отсутствие marker запрещают копирование. Helper без сети читает readonly volumes, GNU tar сохраняет numeric
owners/modes. Проверки не меняют migration/schema/topic IDs. После seal сервисы
**остаются остановленными**: неуспех не скрывается автоматическим restart.
Любой ненулевой exit, нет manifest, ENOSPC, checksum mismatch или source mismatch
означает отказ. Частичный каталог не перезаписывается и не считается backup.
Сохранить его для диагностики, исправить причину и выбрать новый путь. Проверить
логи/статус без публикации secrets; затем явно принять решение о `start`.

`verify` проверяет только целостность относительно доверенного manifest, а не
восстанавливаемость или подлинность от злоумышленника. Manifest и части хранить
вместе внутри зашифрованного/authenticated контейнера. Каталог целиком содержит
credentials и пользовательские данные: не Git, не публичный artifact/лог.

## Внешняя копия, расписание и хранение

Staging на основном диске не является единственной резервной копией. Использовать
отдельный mounted encrypted backup disk/внешнее хранилище на другом хосте.
На backup host достаточно публичного GPG recipient; recovery private key и
passphrase держать отдельно offline, с проверенной доступностью владельцу.
Ключ не передавать в argv/env/cron и не хранить единственно внутри этой копии.
На внешнем хранилище ограничить read/delete отдельной ролью, включить versioning
или immutable retention, если доступно. Экспорт production issuer private key
делать по собственной offline процедуре; online service keys уже в private.tar.

Пример тела `/usr/local/sbin/binance-cold-backup` (Linux, GPG/GNU tar/coreutils
установлены; recipient уже импортирован и проверен оператором). Пути заменить
реальными; `/mnt/binance-offsite` должен быть **отдельным** backup mount.

```sh
#!/bin/sh
set -eu
umask 077
cd /srv/binance-checkout
mountpoint -q /mnt/binance-offsite
stamp=$(date -u +%Y%m%dT%H%M%SZ)
snapshot=/backup-staging/$stamp
encrypted=/mnt/binance-offsite/$stamp.tar.gpg
trap 'logger -t binance-backup "FAILED: inspect offline deployment; no automatic restart"' HUP INT TERM
python3 infra/deploy/backup.py --env /private/deploy.env backup "$snapshot"
python3 infra/deploy/backup.py verify "$snapshot"
# pipefail в POSIX sh отсутствует: сначала завершить tar, затем шифровать.
tar -cf "$snapshot.bundle.tar" -C /backup-staging "$stamp"
gpg --batch --encrypt --recipient "$(cat /private/backup-recipient-fingerprint)" \
  --output "$encrypted.part" "$snapshot.bundle.tar"
test -s "$encrypted.part"
mv "$encrypted.part" "$encrypted"
sha256sum "$encrypted" > "$encrypted.sha256"
sync
python3 infra/deploy/deploy.py --env /private/deploy.env start
logger -t binance-backup "OK: encrypted offsite snapshot and deployment start completed"
```

Планировать ежедневно в окно downtime, например crontab оператора с Docker:

```cron
CRON_TZ=Europe/Moscow
0 3 * * * /usr/bin/flock -n /private/binance-backup-schedule.lock /bin/sh -c '/usr/local/sbin/binance-cold-backup || { logger -t binance-backup "FAILED: requires operator"; exit 1; }'
```

Exit/отсутствие ежедневного OK контролировать существующим мониторингом cron;
при сбое стенд может остаться offline, нужен оператор. Фактический offsite mount,
ключи, cron и оповещение на целевом хосте настраиваются вручную; локальный smoke
не подтверждает их доступность. Хранить 14 daily + 4 weekly зашифрованных копии.
Удалять старые только после проверки новой внешней копии, ежемесячного restore
и наличия минимум двух разных дат. Plaintext staging/bundle удалить после
подтверждённого внешнего хранения; обычный unlink не гарантирует secure erase,
поэтому staging диск тоже должен быть зашифрован. Оставлять свободное место
до следующей копии, не удалять принятые данные приложения ради backup.

## Восстановление

1. Остановить исходный deployment/cleaner и его расписание. Исключить одновременный
   запуск двух копий с прежними ClusterID/TopicIDs/transactional IDs и bot token.
   Drill проводить в изолированной сети только с fake, без настоящего bot.
2. Получить trusted encrypted bundle с отдельного диска, сверить checksum,
   расшифровать offline ключом в приватный каталог. Например:

   ```sh
   sha256sum -c /mnt/binance-offsite/20261109T030000Z.tar.gpg.sha256
   gpg --output /private/recovery.bundle.tar --decrypt /mnt/binance-offsite/20261109T030000Z.tar.gpg
   tar -xf /private/recovery.bundle.tar -C /private/recovery
   python3 infra/deploy/backup.py verify /private/recovery/20261109T030000Z
   ```

3. Checkout ровно `revision` из manifest; загрузить прежние images для той же
   архитектуры. Создать новый приватный env **вне нового DATA_DIR** с новым
   PROJECT/DATA_DIR, прежними PREFIX/MODE; API_PORT свободен. DATA_DIR должен
   отсутствовать, родитель существует. Новые volumes и project ещё не существуют.
   Не вызывать prepare/dev-pki и не генерировать новые secrets/inventory.
4. Выполнить:

   ```sh
   python3 infra/deploy/backup.py --env /private/recovery.env restore /private/recovery/20261109T030000Z
   ```

   До записи проверяются **все** checksums, mode/prefix/revision/images и пустой
   target. Restore не заменяет существующие volumes/каталог. После извлечения
   запускаются только PostgreSQL/Kafka и source-provision **verify**: все три
   broker source/partition sets и 16 SQL intake checkpoints/binding/generation.
   Ни init/reset, ни topic creation, ни migrations не запускаются. SQL/Kafka снова
   останавливаются; ошибка оставляет частичный target для диагностики, apps не
   запускаются. Не повторять поверх него и не «исправлять» source identity.
5. До открытия трафика проверить права/UID оператора, срок PKI, secret mounts,
   firewall, domain/ACME. Старый CA key можно хранить offline после recovery;
   не генерировать новые identities для сокрытия ошибок binding. Cleaner пока
   выключен; его команды перевести **на восстановленные** state-dir, сохранить
   registry/floor/tombstones и проверить `inspect`. Не обращаться к исходному
   broker с восстановленным старым floor.
6. Для настоящего recovery, когда исходный стенд гарантированно выключен:
   `deploy.py ... start`, проверить health/status, source guards matcher,
   сохранённую login/session/pending subscription и durable outcomes. Runtime
   проверяет source-bound projection/price checkpoints и retained ranges.
   Не сбрасывать SQL cursor на latest при отказе. После проверки восстановить
   расписания cleaner/backup с новыми абсолютными путями. Реальные sends делать
   только по разрешённому пользовательскому сценарию E2E-006.

## Проверенный малый drill

```sh
GOCACHE=/tmp/binance-go-cache python3 infra/deploy/smoke.py --restore
```

Существующие Go vendor fixtures и Compose journey: две подписки, первая доставка
и durable API completion; реальный cleaner удаляет один подтверждённый префикс
по mTLS SQL proof и сохраняет ненулевой floor. Затем cold copy, намеренно
смешанный/повреждённый private component отвергается **до target writes**,
восстановление в два новых volumes, неизменные inventory/registry, source verify,
password login + прежняя session, сохранённая pending subscription/проекция и
вторая доставка. История того же Telegram fake содержит ровно два sends,
подтверждённая первая доставка не повторяется. Исходные volumes остаются offline.
Оба disposable набора, networks, snapshot/private files удаляются в cleanup.
Время/объём фактического прогона — в [карточке OPS-076](../../docs/backlog/tickets/OPS-076.md).
