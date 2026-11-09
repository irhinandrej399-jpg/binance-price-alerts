# Наблюдение и maintenance одного хоста — OPS-077

Профиль: один хост, одна реплика каждого сервиса, неизменные 16 prices / 1 changes /
16 delivery partitions. Команды выполнять из checkout под тем же оператором, что
запускает [deployment](README.md). DATA_DIR и Docker daemon доверенные. Все
пороги ниже — начальные настройки предупреждений, не измеренный SLO.

## Наблюдение и локальное предупреждение

```sh
python3 infra/deploy/observe.py --env /srv/binance/deploy.env
```

Один вызов читает process/health состояния Compose, внутренние `/readyz`, один
notifications `/metrics`, диагностический read-only SQL **под runtime ролью
соответствующего владельца** и `df` внутри обоих data volumes. Дополнительно
проверяется ФС DATA_DIR на хосте. SQL имеет statement_timeout 2 с; HTTP/Compose
команды ограничены timeout. Snapshot с числами пишется mode 0600 в
DATA_DIR/observation.json; он нужен только для сравнения двух наблюдений.
Никакие IDs, цены, payload, credentials, SQL errors или сырые ответы в локальный
журнал не попадают. Ошибка записи snapshot даёт отказ, а не успешное наблюдение.

stdout содержит безопасный числовой snapshot; stderr доставляет оператору строки
`WARNING <code>`. Это выбранный **локальный канал** — терминал либо журнал
systemd. Он не отправляет Telegram/email и требует просмотра журнала оператором.
Exit 0: пороги не превышены; 1: есть предупреждения; 2: сама команда не смогла
завершить наблюдение, включая конфликт maintenance lock. Ненулевой код нельзя
трактовать как нулевые очереди. Нужен отдельный внешний канал, если оператору
требуется уведомление вне хоста; он здесь не настроен.

| Код / сигнал | Начальный порог | Диагностика и действие |
| --- | --- | --- |
| `process` | Контейнер отсутствует, exited/restarting/unhealthy — сразу | `deploy.py --env … status`, затем локально `Deployment.dc('logs','--since','5m','--tail','100', service)`; не печатать env/полный inspect. Проверить причину выхода, PKI, свободное место; устойчивый crash остановить штатным stop. |
| `feed`, `readiness.market-data` | Feed не `live` — каждое наблюдение | Сверить каталог, WS connectivity и runtime logs. Raw feed сейчас намеренно `unconfirmed`, поэтому warning ожидаем постоянно; не подавлять readiness и не записывать `live` в SQL. |
| `readiness.<service>`, `probe.*` | HTTP 503, timeout или недоступное чтение — сразу | Проверить SQL/Kafka, startup и clocks; API/notifications readiness не доказывает отсутствие relay backlog. Ошибка `/metrics` не заменяется последним успешным значением. |
| `quote_age` | BTCUSDT отсутствует, trade_time старше 60 с или более 5 с в будущем | Проверить clocks и feed. Используется SQL-копия настоящего trade_time, не received_at. BTCUSDT — только активная контрольная пара; редкая пара может долго не торговаться. Возраст и подтверждение feed — разные сигналы. |
| `relay` | Более 100 pending price_outbox, delivery_outbox или batches за subscription_relay.position | Проверить Kafka и фиксированные сообщения runtime. Pending считать по SQL durable publication markers; broker offsets не заменяют их. Повторить наблюдение через минуту, сравнить рост/дренирование. |
| `delivery_pending` | Любой pending API report | Кратковременный warning допустим в момент ack; сохранение более 5 мин требует проверки API/mTLS/reporter. Job уже может отсутствовать в backlog. |
| `delivery_blocked` | Любой текущий block | Проверить причину через внутренний scrape и результат подписки в API; исправить канал/привязку штатным потоком, дождаться recovery. Не удалять job. |
| `delivery_age` | Самый старый unfinished job старше 300 с | Может ожидать условие цены или восстановления канала; проверить `delivery_jobs`, `delivery_blocks`, `delivery_state_oldest_seconds`, а не объявлять все jobs зависшими. |
| `queue_growth` | Backlog вырос более чем на 100 jobs/мин | Нужны два успешных scrape одной БД с интервалом 30–300 с. Это разность gauges, не incoming throughput. После reset/уменьшения/пропуска окна рост не вычисляется. |
| `disk.host/postgres/kafka` | Меньше 5 GiB **или** 15% доступного места | Проверить, какая ФС заполнена; на Desktop учитывать виртуальный диск Docker и свободное место хоста отдельно. Действия ниже. |

Пороги меняются аргументами `--free-gib`, `--free-percent`, `--quote-age`,
`--delivery-age`, `--relay-pending`, `--growth-per-minute`. Повторять обычно раз в
минуту; warning приходит на каждом нарушенном наблюдении, без скрытого silence.
Сначала наблюдать обычную работу и скорректировать значения под хост/каталог.
Счётчик `delivery_completed` и admission histogram доступны в [контракте
метрик](../../internal/notifications/metrics/README.md); histogram измеряет весь
RPC допуска, а не чистое чтение цены. Один DB-wide target, без суммы реплик.

Пример установки локального расписания Linux (пути заменить на реальные):

```ini
# /etc/systemd/system/binance-observe.service
[Unit]
Description=Binance local operator observation
[Service]
Type=oneshot
User=binance
WorkingDirectory=/opt/binance
UMask=0077
ExecStart=/usr/bin/python3 /opt/binance/infra/deploy/observe.py --env /srv/binance/deploy.env
TimeoutStartSec=180
StandardOutput=journal
StandardError=journal
```

```ini
# /etc/systemd/system/binance-observe.timer
[Unit]
Description=Observe Binance every minute
[Timer]
OnBootSec=2min
OnUnitActiveSec=1min
[Install]
WantedBy=timers.target
```

После установки файлов: `sudo systemctl daemon-reload`,
`sudo systemctl enable --now binance-observe.timer`, затем
`journalctl -u binance-observe.service -n 30` и `systemctl list-timers`.
Проверить время последнего запуска: остановленный timer/daemon сам себя не
контролирует. На macOS запускать команду вручную либо установить своё расписание.
Файлы units не содержат секретов. Учётная запись должна читать private inputs и
иметь доступ к Docker; это привилегированный оператор, не публичный HTTP client.

## Диск, SQL и логи

Compose уже ограничивает json-file logs каждого контейнера, включая tools,
3 файлами по 10 MiB. Не отключать ротацию при диагностике. Для локального
systemd-журнала согласовать с владельцем хоста его общий предел, например
`SystemMaxUse=256M`, `RuntimeMaxUse=64M` в journald.conf.d; проверить
`journalctl --disk-usage`. Это общая настройка журнала хоста, автоматически здесь
не применяется. Не создавать бесконечные перенаправления logs в обычный файл.

При warning диска проверить рост БД и Kafka, состояние cleaner и backup. Удалять
можно ненужные проверенные локальные **копии** backup после подтверждения offsite,
либо выделить больше места. Не выполнять volume prune/down -v, truncate outbox,
удаление WAL/Kafka segments или age/size retention принятых событий.
Если место продолжает заканчиваться, закрыть приём трафика и штатно остановить
стенд; восстановить место/хранилище и запустить с прежними IDs.

Фактическая SQL/fsync/broker write error означает неуспех соответствующей
операции. Сохранить принятую работу/outbox/registry, устранить диск и повторить
штатный запуск/relay. Timeout/потеря ответа могут иметь неизвестный commit:
проверять durable результат, не считать его ни подтверждённым успехом, ни
доказанным rollback. Свободное место сейчас не гарантирует будущую запись.

Диагностика SQL размера и pins изнутри operator helper, без SQL payload:

```sh
python3 - /srv/binance/deploy.env <<'PYCODE'
import sys
from pathlib import Path
sys.path.insert(0, 'infra/deploy')
from deploy import Deployment, OWNERS
from observe import sql
d = Deployment(Path(sys.argv[1]))
for owner in OWNERS:
    print(owner, sql(d, owner, "SELECT json_build_object('bytes',pg_database_size(current_database()));"))
print('pins', sql(d, 'api', """SELECT json_build_object(
 'snapshots',(SELECT count(*) FROM app.subscription_snapshots),
 'rows',(SELECT count(*) FROM app.subscription_snapshot_rows),
 'active_pins',(SELECT count(*) FROM app.subscription_replay_pins WHERE phase<>'abandoned' AND expires_at>clock_timestamp()),
 'expired_pins',(SELECT count(*) FROM app.subscription_replay_pins WHERE expires_at<=clock_timestamp()));"""))
PYCODE
```

Сравнивать ежедневные размеры с ожидаемым свободным местом. SQL хранит snapshots,
pins, журнал/outbox, price work и evidence; опубликованное событие само по себе
не разрешает удалять SQL строки. TTL снимка запрещает чтение ресурса, но не
удаляет его физически; Abandon снимает pin своего владельца без удаления истории.
Штатного SQL cleaner сейчас нет. Ручные DELETE/VACUUM FULL не являются процедурой
освобождения места этого выпуска. При доказанном опасном SQL росте сначала отдельный
тикет с измеренным размером/темпом; воспроизведение должно показать необходимость.
Будущий journal cleaner обязан учитывать relay, всех обязательных consumers и
pins под [RetentionLock](../../internal/api/subscription/README.md), не только TTL.
Здесь такого доказанного роста не получено и новая SQL-очистка не добавляется.

## Provisioning Kafka cleaner

Cleaner работает в bridge как отдельная maintenance-команда, не daemon. Его
контейнер получает только brokers, RPC client PKI, входной inventory и реестры;
SQL DSN туда не монтируются. Operator wrapper использует read-only census каждой
БД под её runtime ролью. Источник истины — DATA_DIR/cleaner/*/registry.json,
не черновики inputs и не broker group offsets.

1. Убедиться, что работает только штатная единственная matcher replica `one`,
   с неизменными Environment/Group/topics и полным набором partitions 0..15.
   Не запускать дополнительные consumers/реплики и не менять assignment topology
   при включённом cleaner. Формулы prospective IDs в wrapper соответствуют
   `replica.prepare`/`scope`; smoke сверяет их с настоящим SQL runtime. Несколько
   реплик/иной topology требуют отдельного полного inventory до их запуска.
2. Остановить cleaner timer и убрать DATA_DIR/cleaner/enabled. Первый запуск
   приложений допустим для census при полностью выключенной очистке; до первого
   DeleteRecords все существующие и prospective consumers должны быть enrolled.
   Любой start/update/stop также снимает enabled. После restore сначала следовать
   [BACKUP.md](BACKUP.md), сохранить прежние floor и tombstones, не делать новый init.
3. Подготовить private каталоги DATA_DIR/cleaner, DATA_DIR/config/cleaner,
   DATA_DIR/pki/cleaner, mode 0700. Выпустить отдельный ClientAuth сертификат с
   URI `spiffe://deploy/cleaner`, доверенный matcher/notifications; файлы
   `client.pem`, `client.key`, `ca.pem` mode 0600. ServerName — `services.internal`.
   Production выдача/ротация — по [PUBLIC.md](PUBLIC.md); dev CA не использовать
   для публичного deployment. Не копировать client identity API/notifications.
4. Создать DATA_DIR/config/cleaner/clients.json mode 0600:

   ```json
   {"brokers":["kafka:9092"],"endpoints":{
     "matcher":{"Address":"matcher:9080","ServerName":"services.internal","CAFile":"/run/pki/ca.pem","CertFile":"/run/pki/client.pem","KeyFile":"/run/pki/client.key"},
     "notifications":{"Address":"notifications:9080","ServerName":"services.internal","CAFile":"/run/pki/ca.pem","CertFile":"/run/pki/client.pem","KeyFile":"/run/pki/client.key"}
   }}
   ```

   Для разных CA использовать отдельные пути CAFile и смонтированные файлы.
5. `python3 infra/deploy/clean_cycle.py --env /srv/binance/deploy.env --draft`
   создаёт только 33 входных JSON без замены прежних файлов, без init/enable.
   Источники берутся из сохранённого inventory/sources.json. Prices включают
   каждый существующий SQL scope/assignment своей partition и prospective scope фиксированной
   реплики; changes — все исторические projection IDs из grants/checkpoints плюс
   prospective projection; delivery — exact binding ConsumerID/Generation на
   всех 16 partitions. Legacy price checkpoint без source или чужой source
   запрещает draft/cycle. Unknown projection включается, но не получает
   выдуманный 0. Сверить census с учётным списком всех consumers этой среды:
   отсутствующая/потерянная SQL БД не доказывает отсутствие обязанности.
6. Просмотреть все descriptors и источники. Проверить PKI, собрать образ,
   выполнить **явный init только новых** 33 реестров:

   ```sh
   python3 - /srv/binance/deploy.env <<'PYCODE'
   import sys
   from pathlib import Path
   sys.path.insert(0, 'infra/deploy')
   from deploy import Deployment, maintenance
   from clean_cycle import REGISTRIES
   d = Deployment(Path(sys.argv[1]))
   with maintenance(d.data):
       d.dc('build', 'kafka-cleaner')
       for name in REGISTRIES:
           (d.data / 'cleaner' / name).mkdir(mode=0o700)  # existing state: STOP
       print(d.dc('run','--rm','--no-deps','--entrypoint','/bin/bash','kafka-cleaner','-ec',
           'set -e; for name in "$@"; do /app/service -operation init -state-dir "/state/$name" -config /inputs/clients.json -inventory "/inputs/$name.json"; done',
           'init-reviewed', *REGISTRIES), end='')
   PYCODE
   ```

   При частичном init сохранить всё состояние, inspect существующих и явно
   init только отсутствующих. Не удалять реестр ради повторения примера.
   Для прежних реестров добавлять новые полные descriptors через `register`,
   используя [CLI](../../cmd/kafka-cleaner/README.md), endpoint aliases и те же
   авторитетные state-dir; не выполнять init повторно. Новый consumer ниже floor
   требует отдельного bootstrap-плана владельца. Исторические offline/revoked
   проекции остаются обязательными. Retire — отдельное явное решение прекратить
   exact обязанность, с tombstone; текущий consumer не retire-ить ради места.
7. После review всех 33 реестров: создать DATA_DIR/cleaner/enabled mode 0600
   (обычный пустой файл), затем выполнить
   `python3 infra/deploy/clean_cycle.py --env /srv/binance/deploy.env`.
   Wrapper держит общий maintenance lock, требует все 33 реестра, повторяет census
   и проверяет enrollment (включая сохранённые retirement tombstones).
   Сам CLI под registry.lock повторно проверяет каждое exact source-bound SQL
   доказательство и broker range, сохраняет durable floor до удаления.
   Unknown/missing/offline/invalid/timeout останавливает цикл с WARNING и exit 1;
   последующие partitions не очищаются. Ранее завершённые операции цикла не
   откатываются. До первой сделки некоторые checkpoints могут быть неизвестны:
   это нормальный безопасный отказ, а не разрешение удалить partition целиком.

Для периодического запуска установить аналогичную oneshot service с
`ExecStart=/usr/bin/python3 /opt/binance/infra/deploy/clean_cycle.py --env /srv/binance/deploy.env`,
`TimeoutStartSec=10min` и timer `OnBootSec=10min`, `OnUnitActiveSec=1h`.
Включать timer только после шага 7; после lifecycle заново проверить inventory
и enabled. За цикл не более 1000 offsets на partition, timeout CLI 15 с;
offsets не равны bytes или числу records. При необходимости следующий цикл,
без изменения safety checks. Локальный журнал — тот же выбранный канал оператора.

Нельзя запускать lifecycle напрямую через Docker в обход этого порядка.
Любое изменение membership регистрировать до запуска нового consumer и после
остановки timer. Вне этой операторской модели wrapper не является атомарным
протоколом enroll новых runtime consumers. Потерянный/старый inventory, другой
source, отсутствующий registry, unknown broker result — повод остановиться и
сохранить registry/floor. Не откатывать floor и не удалять lock inode.
Kafka age/size retention и compaction остаются выключены. Очистка не обещает
немедленного освобождения физических bytes; при disk pressure нужен запас места
и остановка приёма, а не обход SQL proofs.

## Проверенный малый сценарий

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s infra/deploy -p test_observe.py -v
python3 infra/deploy/smoke.py --operations
```

Signal fixture вызывает каждый основной warning и проверяет локальный stderr,
рост с учётом интервала и unknown вместо нулей. Compose smoke использует только
fake vendors: реальные HTTP/SQL/df probes, контролируемый disk threshold без
заполнения ФС, age/feed warning, остановку SQL/process, отсутствие секретов в
выводе, все 33 inventory/init, bounded цикл и настоящий CLI. Unknown SQL consumer
запрещает попытку DeleteRecords, broker start не меняется; подтверждённый prefix
очищается, повторный CLI clean работает. Затем полный stop/start с прежними
volumes и вторая доставка без дублей; disposable cleanup обязателен.
Операторские units, реальные host thresholds, production PKI и внешний alert
канал этим сценарием не устанавливаются.
