# Локальный PostgreSQL (SET-003)

Один PostgreSQL, четыре БД: `api`, `market_data`, `matcher`, `notifications`.
Для каждой — `<owner>_runtime` и `<owner>_migration`, схема `app`.
Прикладных таблиц и runner миграций пока нет (SET-004).

## Функциональный запуск

Нужны работающий Docker Engine и Docker Compose с `up --wait`.
Из корня репозитория:

```sh
cp infra/postgres/.env.example infra/postgres/.env
# Заполнить ВСЕ девять паролей разными случайными значениями.
chmod 600 infra/postgres/.env
docker compose -f infra/postgres/compose.yaml up -d --wait --wait-timeout 120
docker compose -f infra/postgres/compose.yaml ps
```

`.env` игнорируется Git. Не выводите `docker compose config` без `--quiet`:
развёрнутая конфигурация содержит пароли. Docker-администратор может читать
окружение контейнера; credentials администратора нельзя выдавать сервисам.
Пароли читаются init-скриптом из окружения через `psql \getenv`, не передаются
в аргументах команд. Для значений с `$` используйте одинарные кавычки в `.env`.
При неполной инициализации healthcheck не станет зелёным. Исправление `.env`
не повторяет init на непустом volume: сохраните нужные данные и исправьте
роли администратором либо явно удалите только ненужный volume и начните заново.
Смена паролей на существующем volume требует `ALTER ROLE`, а не перезапуска.

Порт доступен только на `127.0.0.1:5432`; `POSTGRES_PORT` меняет порт хоста.
Клиент с хоста использует `host=127.0.0.1 port=5432 dbname=api user=api_runtime`,
с паролем из конфигурации. Внутри compose-сети hostname — `postgres`, port — 5432.
Остальные сервисы аналогичны. TLS для этого локального стенда не настроен.
`local trust` относится только к Unix socket внутри контейнера; TCP требует
SCRAM-пароль. Контейнерный доступ Docker равносилен административному.

Остановка и удаление контейнера **сохраняют** именованный volume:

```sh
docker compose -f infra/postgres/compose.yaml stop
docker compose -f infra/postgres/compose.yaml up -d --wait
# Можно также убрать контейнер и сеть, сохранив volume:
docker compose -f infra/postgres/compose.yaml down
```

Явное удаление локальных данных, только когда они больше не нужны:

```sh
docker compose -f infra/postgres/compose.yaml down --volumes
```

PGDATA PostgreSQL 18 — `/var/lib/postgresql/18/docker`; volume смонтирован
на `/var/lib/postgresql`. `fsync`, `synchronous_commit`, `full_page_writes`
включены явно. Одиночный диск/узел не дают HA и защиты от потери носителя.

## Изоляция и DDL

Runtime может подключаться только к своей БД, читать/менять прикладные таблицы,
использовать sequences; DDL, TEMP, административные атрибуты и членство в
migration-ролях отсутствуют. У PUBLIC отозваны CONNECT/TEMP в БД и CREATE
в `public`. Migration — владелец своей БД/схемы; права будущих таблиц и sequences
в `app` выдаются runtime через `ALTER DEFAULT PRIVILEGES`. Миграции выполняются
именно своей migration-ролью, без superuser, в схеме `app`. Функции по умолчанию
не имеют PUBLIC EXECUTE; нужные grants будущие миграции выдают явно.

Административный event trigger в каждой БД запрещает создание unlogged-таблиц
и `ALTER TABLE ... SET UNLOGGED`; migration не может отключить этот trigger.
Администратор остаётся доверенным: он может менять защиту и настройки.
Будущий код обязан не ослаблять `synchronous_commit` на уровне сессии/транзакции.

## Проверка на чистом окружении

```sh
bash infra/postgres/verify.sh
```

Используется отдельный проект `binance-storage-check`, порт 55432
(переопределение: `STORAGE_CHECK_PORT=55433 bash infra/postgres/verify.sh`),
с тем же `.env` и отдельным volume. Первое выполнение начинается с пустого volume.
Проверяются все восемь login-ролей: своя БД, отказ к трём чужим и служебным БД,
отсутствие административных атрибутов/членства, search_path, durability.
Для всех четырёх владельцев — отказ runtime DDL/TEMP/SET ROLE, migration DDL,
runtime CRUD/identity sequence, запрет unlogged и отключения guard, отказ
неверного пароля. Затем `down`/`up` пересоздают контейнер: четыре committed
строки должны сохраниться. Тестовые таблицы удаляются, контейнер останавливается,
volume сохраняется. Ошибка прерывает проверку; контейнер/volume остаются для разбора.

Повторный успешный запуск использует сохранённый volume. Для повторения именно
чистого init явно удалите только тестовые данные (не рабочий проект):

```sh
docker compose -p binance-storage-check -f infra/postgres/compose.yaml down --volumes
bash infra/postgres/verify.sh
```

## Нагрузочный профиль DEC-008

Функциональный профиль выше: **1 vCPU/1 GiB**, обычный именованный volume
**без квоты**. Он проверяет функциональность; его измерения не соответствуют DEC-008.

Нагрузочный override задаёт **4 vCPU/10 GiB**, shm 1 GiB и требует **внешний**
volume. На выделенном Linux-стенде заранее подготовьте отдельную файловую систему
на устройстве/разделе **120 GiB** и смонтируйте её, например, в `/srv/binance-postgres`.
Файловая система не должна разделять свободное место с Kafka или данными хоста.
Каталог используется исключительно PostgreSQL; проверьте права записи контейнера.
Создание раздела и форматирование выполняет оператор стенда по своим устройствам.
Обычный `docker volume create` без такого backing storage не задаёт квоту 120 GiB.

```sh
# Проверить реальное отдельное устройство, размер и точку монтирования:
findmnt --target /srv/binance-postgres
df -B1 /srv/binance-postgres
# После подготовки отдельной файловой системы:
docker volume create --driver local --opt type=none --opt o=bind \
  --opt device=/srv/binance-postgres binance-postgres-load
POSTGRES_LOAD_VOLUME=binance-postgres-load docker compose -p binance-storage-load \
  -f infra/postgres/compose.yaml -f infra/postgres/compose.load.yaml up -d --wait
# Остановка: external volume не удаляется даже при down --volumes.
POSTGRES_LOAD_VOLUME=binance-postgres-load docker compose -p binance-storage-load \
  -f infra/postgres/compose.yaml -f infra/postgres/compose.load.yaml down
```

Порты функционального и нагрузочного стендов не должны конфликтовать; запускайте
по одному или задайте `POSTGRES_PORT`. Проверяйте `docker inspect` фактического
контейнера (NanoCpus=4000000000, Memory=10737418240), mount/размер файловой системы,
версию и durability перед нагрузкой. Фиксируйте CPU/диск/ОС/архитектуру в отчёте.
Из 120 GiB: 80 — обычная работа, 24 — резерв завершения, 16 — аварийный запас.
Это бюджет DEC-008, **не реализованные SQL-квоты**: учёт и ограничители OPS-015/016,
квалификация fixture/операций и полная проверка стенда LOAD-009 ещё впереди.
Этот тикет проверяет меньший функциональный стенд; нагрузка не измеряется.

## Закрепление образа

Выбрана поддерживаемая ветка PostgreSQL 18, minor **18.6**, Debian bookworm;
manifest digest закреплён в compose (multi-platform, включая amd64/arm64).
Сверено 26 октября 2026: [политика версий PostgreSQL](https://www.postgresql.org/support/versioning/),
[официальный образ, PGDATA и init](https://hub.docker.com/_/postgres).
Digest получен `docker pull postgres:18.6-bookworm`. Обновление версии/digest —
явное изменение профиля с повторной проверкой, не автоматический pull нового тега.
