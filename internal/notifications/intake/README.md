# Durable приём AlertTriggered — NTF-001

`New(Config{Brokers, Topic, Group}, NewStore(notificationsPool))`, `Step(ctx)` /
`Run(ctx)` и `Close()` принимают запросы из префиксного delivery-requests.
Consumer group распределяет 16 партиций; группа стабильна для среды. Методы
consumer вызываются последовательно; отмена контекста завершает ожидание.
Runtime и запуск процесса подключаются отдельно.

Перед чтением каждого назначения `AdjustFetchOffsetsFn` регистрирует SQL
checkpoint каждой партиции и восстанавливает абсолютные позиции. Первый приём
начинается с нуля; broker committed offset не заменяет SQL. Потеря необходимой
истории, offset вне retained начала / read_committed LSO и fetch errors явно
останавливают consumer. Автоматического reset/skip нет. Один логический intake
на topic: SQL-ключ `(topic, partition)` не зависит от replica/group member.
При переименовании topic или сбросе SQL требуется отдельный операторский план;
checkpoint не разрешает удалять историю, пока не подключён безопасный cleaner.

Kafka настроен на read_committed, DisableAutoCommit, NoResetOffset и
BlockRebalanceOnPoll. Step читает одну запись, проверяет AlertTriggered/ключ,
затем блокирует строку checkpoint и сохраняет job плюс следующий offset одной
транзакцией notifications (бюджет 5 с). SQL-позиция сравнивается с cursor
предыдущей принятой записи; abort/control records могут оставлять численные
пропуски offsets. Непрерывность доказывает последовательный fetch без skip,
а не равенство соседних чисел. CAS не позволяет устаревшему reader перепрыгнуть
уже изменившийся SQL cursor; повтор ниже durable позиции ничего не меняет.

Только успешный SQL commit разрешает явный CommitOffsetsSync (бюджет 3 с).
Любая ошибка Step завершает работу экземпляра; следует закрыть его и создать
новый с теми же topic/group/БД. Невалидная запись останавливает весь экземпляр,
включая её партицию, с ErrContract и координатами; автоматического ack после
повторов нет. Частичный успех/неизвестный исход SQL commit безопасно перечитывается
из SQL; потерянный Kafka ack не меняет принятое задание.

Миграция `notifications/001_delivery_jobs.sql`: trigger_id — NOT NULL primary key,
сохраняются subscription/user/event ID, исходные байты и начальное state=pending.
`ON CONFLICT(trigger_id) DO NOTHING` сохраняет первоначальное задание и его
текущее состояние даже при новом event_id/версии workflow после wait_condition.
Событие не разрешает попытку Telegram. Слот, подписка и outbox API здесь не
изменяются; роль notifications не имеет CONNECT к остальным БД. Worker leases,
попытки, проверки цены, результаты и Telegram — последующие задачи; их schema
расширяется отдельными миграциями. Поведение при rebalance безопасно за счёт
порядка fetch/SQL CAS/уникальности; отдельный fault/load прогон реплик не заявлен.

Память ограничена одной доставленной записью, fetch/decompression до 1 MiB,
ответом до 2 MiB; приём записи — до 8 KiB (бюджет producer SingleRecord), ключ до 256. Сохраняются точные
JSON-цены/uint64 версии без float. Нагрузочная производительность не измерялась.

Проверка реальных PostgreSQL/Kafka с race detector и удалением disposable volumes:

```sh
KAFKA_RUN_POSTGRES=1 GOCACHE=/tmp/binance-go-build bash infra/kafka/verify-go.sh
```

Runner миграций применяет notifications и test-only checkpoint/deferred commit
failures. Без POSTGRES_TEST SQL-тесты пропускаются; Kafka-тесты требуют также
KAFKA_INTEGRATION и конфигурацию тестового брокера. Обычный `go test` не заменяет
совместный opt-in прогон.
