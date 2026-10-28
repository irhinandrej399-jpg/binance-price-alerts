# Публикация AlertTriggered — SUB-009

`New(pool, kafka.Config, topic)`, `Step(ctx)` и `Run(ctx, observe)` повторяют
протокол SUB-007: одна SQL-lease на 30 с, бюджет шага 5 с, новый producer epoch
перед каждой попыткой, повторная проверка token после initialization и остановка
владельца при `kafka.ErrFenced` / `ErrLeaseLost`. Runtime подключается отдельно.
Наблюдатель Run синхронный, должен быстро возвращаться; idle polling 250 мс,
backoff ошибок до 5 с. Повреждённый outbox и неверная конфигурация останавливают Run.

Все реплики этой очереди используют один стабильный transactional.id, например
`local-api-delivery-relay-0`, отличный от ID relay подписок и market-data.
Topic — префиксный `delivery-requests` с 16 партициями по SET-005. Одна запись —
одна Kafka-транзакция; ключ — subscription_id, Value — сохранённый bytea.
Конверт проверяется по AlertTriggered и SQL subscription_id/trigger_id/event_id.
ID, цена и диагностические времена не создаются заново. Даже отменённая подписка
не удаляет принятое событие; consumer обязан запрашивать канонический допуск API.

Миграция API `012_delivery_relay.sql` создаёт singleton lease/token и отдельный
набор `delivery_published`. Выбирается одна ещё не отмеченная строка outbox.
UUID не является порядком commit: отметка по каждой идентичности позволяет
обнаружить и поздний commit с меньшим UUID. Непрерывный cursor здесь неприменим.
SQL-отметка разрешена только после подтверждённого Kafka commit и при живом token.
Сетевых вызовов под SQL-блокировками нет; PostgreSQL и Kafka не образуют общую
транзакцию. При потере Kafka-ответа или SQL-отметки повторяется исходное событие,
возможный дубль сохраняет event_id и trigger_id. Consumers используют read_committed
и дедупликацию. После потери lease старый initializer не публикует; замена процесса
после fencing остаётся обязанностью runtime/supervisor.

Outbox не изменяется/не удаляется, SQL-права runtime на него остаются SELECT/INSERT.
Отметки не разрешают retention. В памяти одна запись; нагрузочная вместимость,
производительность поиска среди отмеченных строк и initialization на каждой
попытке не квалифицированы. Cleanup, admission gates и consumers — отдельные задачи.

Проверка настоящих PostgreSQL/Kafka, с race detector и удалением обоих disposable
volumes, включая сбой брокера, fencing, потерю EndTxn-ответа, сбой SQL-ack и
read_committed-повторы исходных байтов:

```sh
KAFKA_RUN_POSTGRES=1 GOCACHE=/tmp/binance-go-build bash infra/kafka/verify-go.sh
```

Без POSTGRES_TEST SQL-тесты пропускаются; KafkaRecovery дополнительно требует
KAFKA_INTEGRATION=1 и параметры тестового брокера. Рабочие БД/topics не используются.
