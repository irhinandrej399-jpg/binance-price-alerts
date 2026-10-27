# Локальная Kafka (SET-005)

Apache Kafka **4.3.1**, JVM, один broker/controller KRaft, RF=1/min ISR=1.
Multi-platform digest образа закреплён в Compose; версия выбрана из
[поддерживаемых выпусков Apache](https://kafka.apache.org/community/downloads/).
[Официальный JVM-образ](https://kafka.apache.org/43/getting-started/docker/).

## Запуск

Нужны Docker Engine и Compose с `up --wait`. Из корня репозитория:

```sh
docker compose -f infra/kafka/compose.yaml up -d --wait --wait-timeout 180
docker compose -f infra/kafka/compose.yaml exec -T kafka bash -s -- local < infra/kafka/init-topics.sh
docker compose -f infra/kafka/compose.yaml ps
```

Host bootstrap: `127.0.0.1:9092`. Переопределение: `KAFKA_PORT=9095` перед
каждой Compose-командой. В compose-сети: `kafka:9092`; другой контейнер должен
быть присоединён к `binance-broker_default`. Controller :9093 наружу не открыт.
На хосте опубликован только loopback; PLAINTEXT, без TLS/SASL/ACL, для доверенной
локальной разработки. Docker-администратор имеет полный доступ.

`init-topics.sh` требует префикс среды: `local`, `load` или уникальный `check-...`.
Создаёт `<prefix>-subscription-changes` (1 партиция), `<prefix>-prices` (16),
`<prefix>-delivery-requests` (16). Повторный запуск сверяет параметры существующих
topics; несовпадение завершает команду ошибкой. Скрипт не меняет партиционирование
существующих данных. Auto-create отключён; имена topics задаёт конфигурация клиента.

Для трёх topics явно заданы `min.insync.replicas=1`, `flush.messages=1`,
`cleanup.policy=delete`, `retention.ms=-1`, `retention.bytes=-1`, без compaction.
Broker defaults сохраняют эти flush/retention параметры; внутренние offsets и
transaction state topics имеют RF=1, transaction state min ISR=1, по одной
партиции. Внутренние topics управляются Kafka, в том числе их compaction.
[Broker configs](https://kafka.apache.org/43/configuration/broker-configs/),
[topic configs](https://kafka.apache.org/43/configuration/topic-configs/).

Producer обязан использовать стабильный `transactional.id` среды/владельца/шарда,
`acks=all`, идемпотентность, retries, max in-flight ≤5 и ждать commit транзакции.
Consumer: `read_committed`, auto-commit выключен; offset подтверждается после
сохранения бизнес-результата. Java smoke-check демонстрирует транспортный commit,
прикладных producers/consumers и SQL-checkpoints здесь нет.

## Хранение и остановка

Данные и KRaft metadata лежат в именованном `binance-broker_data`, log directory
`/var/lib/kafka/data/logs`. Cluster ID/node ID закреплены для этого одиночного
кластера. Отдельные Compose-проекты имеют отдельные volumes и сети.

```sh
docker compose -f infra/kafka/compose.yaml stop
docker compose -f infra/kafka/compose.yaml up -d --wait
# Пересоздание контейнера/сети с сохранением данных:
docker compose -f infra/kafka/compose.yaml down
docker compose -f infra/kafka/compose.yaml up -d --wait
```

`down --volumes` удаляет данные обычного профиля; применять только к ненужным
данным. Без retention/cleaner диск растёт. Автоматической очистки прикладных
topics и gate приёма пока нет (OPS-014/015/016): самостоятельно контролировать
место и останавливать источники до исчерпания. Бесконечные повторы проверки также
накапливают тестовые topics, их не следует запускать бесконтрольно.

Лимиты контейнера — **2 vCPU/4 GiB**, heap 512 MiB–1 GiB, Docker-логи ротируются
3 × 10 MiB. Обычный volume **не имеет отдельной квоты 40 GiB** и разделяет диск
Docker VM. Это функциональный стенд; производительность DEC-008 не измерена.

## Отдельный диск нагрузочного профиля DEC-008

На выделенном Linux-стенде оператор заранее готовит отдельную файловую систему
на устройстве/разделе **40 GiB** с правами записи пользователя образа. Она не
должна делить свободное место с PostgreSQL/хостом. Создание/форматирование
устройства выполняется оператором по его реальным устройствам. Проверить:

```sh
findmnt --target /srv/binance-kafka
df -B1 /srv/binance-kafka
# Только после подготовки отдельной ФС:
docker volume create --driver local --opt type=none --opt o=bind \
  --opt device=/srv/binance-kafka binance-kafka-load
KAFKA_LOAD_VOLUME=binance-kafka-load KAFKA_PORT=29092 docker compose -p binance-broker-load \
  -f infra/kafka/compose.yaml -f infra/kafka/compose.load.yaml up -d --wait
KAFKA_LOAD_VOLUME=binance-kafka-load KAFKA_PORT=29092 docker compose -p binance-broker-load \
  -f infra/kafka/compose.yaml -f infra/kafka/compose.load.yaml \
  exec -T kafka bash -s -- load < infra/kafka/init-topics.sh
```

Override требует внешний volume и сохраняет 2 vCPU/4 GiB. External volume не
удаляется через Compose `down --volumes`. Обычный `docker volume create` без
отдельного backing storage не ограничивает размер. Бюджет 40 GiB: 24 обычная
работа / 8 резерв завершения / 8 аварийный запас. Это границы будущего учёта
OPS-015/016, а не уже реализованные квоты внутри Kafka. Квалификация физических
затрат, пропускной способности, размера fixture и cleaners не входит в SET-005.

## Воспроизводимая проверка

```sh
bash infra/kafka/verify.sh
# Другой host port:
KAFKA_CHECK_PORT=19093 bash infra/kafka/verify.sh
```

Проверка использует проект `binance-broker-check`, порт 19092, отдельный volume,
уникальный префикс topics, transactional ID и consumer group. Рабочий брокер не
останавливается. Не запускать две проверки одновременно в одном проекте.
Дополнительный tools-образ собирается из закреплённых Kafka и Temurin JDK 21;
использует Java-клиент самой Kafka, не меняя Go-модуль. Нужен доступ к registry
при первом запуске. Проверяются фактические параметры всех трёх topics, commit,
abort, невидимость открытой транзакции для `read_committed` (при видимости через
`read_uncommitted`), fencing старого producer тем же ID, consumer offset commit.
После SIGKILL брокера проверяются сохранность committed records и offset,
RF/ISR внутренних topics и новая транзакция восстановленного координатора.
Затем down/up пересоздаёт контейнер, CLI снова читает три committed сообщения.
Успешная проверка останавливает тестовый контейнер и сохраняет volume; при ошибке
ресурсы остаются для разбора. Изолированные ненужные данные можно удалить явно:

```sh
docker compose -p binance-broker-check -f infra/kafka/compose.yaml down --volumes
```

Гарантия ограничена рестартом процесса/контейнера при исправном сохранном volume.
RF=1 не даёт HA. `acks=all` не заменяет fsync; `flush.messages=1` не ослабляется
ради скорости. Проверка SIGKILL не моделирует потерю питания, отказ диска/VM,
не доказывает поведение контроллера хранения при отключении питания или
совместную атомарность с PostgreSQL. Go-клиенты проверяются в своих тикетах.

## Go-клиент и управляемая потеря commit-ответа

`bash infra/kafka/verify-go.sh` — транзакционные Go-проверки SET-009 в отдельном
уникальном disposable-проекте с удалением его volume при выходе. Рабочий
`binance-broker` и Java-проверка SET-005 не затрагиваются. Интерфейс producer,
настройки, классификация ошибок, opt-in и пределы fault proxy описаны в
[internal/platform/kafka/README.md](../../internal/platform/kafka/README.md).
