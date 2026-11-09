# Постоянный стенд одного хоста (OPS-074)

Четыре отдельных образа/runtime, четыре БД и восемь SQL ролей, mTLS с отдельными
service URI, PostgreSQL и Kafka/KRaft. Одна реплика каждого сервиса. Нужны Docker
с Compose v2+, Python 3.10+ и OpenSSL для локальной dev-PKI. Интернет нужен при
первой сборке и для настоящих Binance/Telegram. Go устанавливать на хост не нужно.

Из чистого checkout подготовить приватную конфигурацию **вне репозитория**:

```sh
umask 077
mkdir -p /srv/binance
chmod 700 /srv/binance
cp infra/deploy/.env.example /srv/binance/deploy.env
# Отредактировать DATA_DIR, PROJECT, PREFIX и свободный loopback API_PORT.
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env prepare
```

`prepare` однократно создаёт случайные SQL passwords, webhook secret и ClusterID.
Существующие файлы не заменяются. Внести настоящий bot token в приватный файл
`/srv/binance/secrets/bot_token` через редактор/хранилище секретов; права 0600.
Не вставлять token в командную строку, не делать shell `set -x`. Для development
выпустить локальные сертификаты отдельной командой:

```sh
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env dev-pki
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env check
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env start
```

`start` — одна воспроизводимая команда запуска после настройки. Она собирает
образы, останавливает прежние приложения, поднимает хранилища, выполняет отдельные
шаги миграций и init topics, затем явный source provisioning и запуск приложений
по порядку market-data → API → matcher → notifications. Ошибка любого шага
возвращает ненулевой код. Автоматического rollback migrations/update нет.
Сервисы используют настоящие `api.binance.com`, `stream.binance.com:9443` и
`api.telegram.org`; `MODE=smoke` обычная CLI отвергает. Startup требует Binance
catalog. Публичный HTTPS и provisioning webhook описаны в [PUBLIC.md](PUBLIC.md).

```sh
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env status
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env stop
# После доставки нового checkout в эту же папку, с прежним deploy.env:
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env update
```

`update` выполняет тот же остановленный start с пересборкой образов. Планировать
downtime; перед обновлениями с изменением SQL нужен backup (OPS-076). Не менять
PROJECT/PREFIX/DATA_DIR/SQL passwords/ClusterID или source inventory при обычном
restart/update. Сохранять checkout по тому же пути: Compose использует его init
scripts. Одновременно работает только один оператор команды; parallel start и
provisioning не поддерживаются. Перед provision исключить внешних producers.

`stop` останавливает приложения в обратном порядке, затем Kafka/PostgreSQL и
**сохраняет** именованные volumes `${PROJECT}_postgres-data`, `${PROJECT}_kafka-data`,
inventory и secrets. Не выполнять `docker compose down -v`, удаление volumes,
новый init или замену inventory для исправления startup failure. Следующий start
проверяет прежние ClusterID/TopicID и все notifications checkpoints; непустые topics
и ненулевой SQL прогресс допустимы. Transactional IDs, groups, generation и
bootstrap берутся из первоначального inventory, а не генерируются на рестарте.

Если первый provisioning прервался, сохранить `inventory/sources.json`. Сначала
повторить start (verify). Если bootstrap не завершён, при остановленных внешних
producers выполнить явный `resume`, затем start:

```sh
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env resume
```

`resume` не запускает приложения и не обнуляет прогресс. При частичной/чужой SQL
provenance либо отсутствии inventory на старых данных остановиться и следовать
[операторскому контракту](../../cmd/source-provision/README.md). Ручное удаление
checkpoints, повторный bootstrap и очистка broker запрещены как способ рестарта.

## Доступ, секреты и диагностика

В базовом локальном профиле только API HTTP опубликован на `127.0.0.1:API_PORT`; SQL, Kafka, health остальных
сервисов и все RPC остаются в Compose bridge. Kafka/SQL внутри доверенной сети
используют plaintext, RPC — TLS 1.3/mTLS. Доступ к Docker daemon и каталогу DATA_DIR
эквивалентен доступу к секретам. Хост и Docker должны быть доверенными.

Runtime получает только свою DSN под `*_runtime`, приватную per-service директорию
PKI и сгенерированный env файл. Migration passwords доступны только PostgreSQL
init/операторскому migration шагу; они не передаются приложениям. CA key остаётся
в DATA_DIR/pki и в контейнеры не монтируется. Образы собираются по allowlist
`.dockerignore` и содержат binary/entrypoint/публичные системные CA, без локальных
env, DSN, bot token и private keys. Секреты загружаются entrypoint из read-only
mount, не передаются argv или Compose interpolation. Приложения работают от UID/GID
оператора, с read-only root и tmpfs; запускать операторские команды от отдельного
обычного пользователя Linux. На Docker Desktop используется UID текущего пользователя.

Dev-PKI: сертификаты серверов имеют DNS SAN `services.internal` (общая настройка
проверяемого имени у matcher для API и Latest); клиенты имеют различные
`spiffe://deploy/api`, `/matcher`, `/notifications`. Разрешения задаёт существующая
RPC policy. Листовые сертификаты действуют 7 дней, CA — 30 дней. `dev-pki` никогда
не перезаписывает существующую PKI. Это локальная development выдача; production
выдача/ротация и публичный TLS описаны в [PUBLIC.md](PUBLIC.md). Истёкшие сертификаты требуют остановки,
согласованной замены и restart; команду не считать автоматической ротацией.

`check` проверяет формат настроек, обязательные private inputs и Compose config
без печати его содержимого. Сертификаты/SQL/источник дополнительно проверяются
при startup. Скрипт подавляет сторонние command errors, которые могут содержать
секреты; провал возвращает ненулевой код. `status` показывает контейнеры/health,
не env. Для диагностики разрешены фиксированные runtime сообщения и состояния
контейнеров; не публиковать `docker inspect` целиком, env, SQL errors или rendered
Compose с добавленными вручную credentials. После неуспешного start уже запущенные
компоненты могут остаться работающими: выполнить stop перед вмешательством.

Docker healthcheck проверяет `/healthz`, а не рыночное подтверждение. После startup
проверять API `/readyz`, а market-data — Catalog/Latest и `/readyz=503` с
`feed=unconfirmed`. Raw Binance feed сохраняет предупреждение и sticky gap;
успешный dial/ACK/сделка не делают его `live`. Ни скрипт, ни smoke не меняют SQL
ради зелёного readiness. `restart: unless-stopped` сохраняет настройку при
перезапуске daemon, startup errors повторяются с прежними IDs; при устойчивой ошибке
оператор останавливает сервис. Логи ограничены 3×10 MiB на контейнер, Kafka retention
не ограничен — [наблюдение, диск и safe cleaner](OPERATIONS.md).

Публичный профиль — [PUBLIC.md](PUBLIC.md): PUBLIC_DOMAIN включает Caddy, удаляет
API_PORT binding и закрывает служебные маршруты. Dev-PKI для публичного start
отклоняется; нужны отдельные production credentials.

## Малый изолированный smoke

```sh
python3 infra/deploy/smoke.py
```

Команда сама создаёт уникальные project/data directory/SQL credentials, не принимает
параметры постоянного стенда и не использует реальные vendor endpoints. Готовые
binancefake/telegramfake работают в отдельном контейнере `smoke` той же bridge-сети
через `NewWithListener`; порты fake наружу не публикуются. Только test override
`compose.smoke.yaml` даёт notifications сетевой namespace fixture: существующий
Telegram client разрешает plaintext HTTP только к literal loopback IP. Поэтому
Telegram origin — `http://127.0.0.1:18081`, guard клиента сохранён. DNS alias
notifications для health указывает на этот общий namespace. Fake вручную подтверждает
подписки/цены/Telegram ответы, Go не заменяет ни один production процесс.

Проверяются config/build/startup failure, четыре `/healthz` и реальные readiness,
регистрация/логин/link webhook, две подписки, raw trade → точная доставка, stop/start
всех четырёх сервисов и хранилищ с теми же двумя volumes/inventory, сохранённой
сессией/подпиской и второй доставкой без дублей. Хост управляет только lifecycle;
файлы фаз внутри приватного temporary directory координируют этот единственный
сценарий. После него удаляются только disposable containers/volumes/network/private
files и проверяется cleanup. Build cache/образы остаются в Docker.

Cold backup/restore: [состав, downtime, команды, offsite/расписание и recovery](BACKUP.md).
Отдельный `python3 infra/deploy/smoke.py --restore` использует тот же journey,
добавляет реальный cleaner floor и восстановление в два новых volumes; прежний
набор остаётся offline, подтверждённая доставка не повторяется.

Production ready и внешняя доставка этим smoke не подтверждаются: реальный домен/ACME,
установка расписаний на целевом хосте и E2E с настоящими Binance/своим Telegram ещё впереди.

## Внутренние метрики notifications — OPS-079

GET `http://notifications:8080/metrics` доступен только в Compose bridge на
существующем HTTP listener. Новый published port не нужен; Caddy пропускает
только разрешённые `/v1/...` пути, `/metrics` снаружи отвечает 404.
Для разового scrape из контейнера notifications (без curl/нового образа),
из корня checkout с собственным путём к deploy.env:

```sh
python3 - /srv/binance/deploy.env <<'PYCODE'
from pathlib import Path
import sys
from infra.deploy.deploy import Deployment

d = Deployment(Path(sys.argv[1]))
print(d.dc('exec', '-T', 'notifications', '/bin/bash', '-ec',
           'exec 3<>/dev/tcp/127.0.0.1/8080; '
           'printf "GET /metrics HTTP/1.0\r\n\r\n" >&3; cat <&3'), end='')
PYCODE
```

Сначала проверить HTTP status: успешный scrape — 200, SQL failure/отмена/
истечение 2 с — 503 с фиксированным сообщением без SQL/секретов и без кеша.
Не интерпретировать неуспешный scrape как нулевую очередь. `delivery_jobs`,
`delivery_pending_reports`, `delivery_blocks` и остальные SQL gauges относятся
ко всей notifications DB: брать один target на БД или дедуплицировать реплики,
не складывать копии. `delivery_admission_rpc_seconds` — локальный histogram
каждого процесса; его bucket/sum/count rates можно агрегировать по процессам.
Полный контракт, bounded labels и ограничения drain rate — в
[metrics README](../../internal/notifications/metrics/README.md).
Локальные предупреждения, пороги и периодическое обслуживание —
[OPERATIONS.md](OPERATIONS.md).
