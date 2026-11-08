# Полный пользовательский сценарий (E2E-001)

Из корня репозитория, с локальными Go/Docker/PostgreSQL credentials по
[инструкции стенда](../e2estack/README.md):

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_e2e_environment.py --package ./internal/testutil/e2escenario
```

`TestUserJourney` использует готовые `e2estack.New/With`, четыре настоящих
race executable, mTLS RPC, штатные миграции и disposable PostgreSQL/Kafka.
HTTP API создаёт аккаунт, сессию и Telegram link code; fake Telegram вызывает
настоящий private `/start` webhook. Все цены поступают через raw WebSocket
fake Binance, затем через SQL outbox и Kafka к matcher. RPC владельцев
и бизнес-обработчики не заменяются. Без opt-in тест явно SKIP.

Проверяются три последовательные подписки одного аккаунта с порогом 100 вверх:

- Цена 99 при создании, затем 101: один sendMessage на привязанный chat,
  точная фактическая цена и предупреждение unconfirmed. Пока fake удерживает
  ответ, слот занят. После ответа с message_id — API delivered, сохранённые
  attempt/message/workflow, notifications success и завершённый report, слот свободен.
- После durable pending_delivery при 101 цена становится 99 до допуска:
  worker паркует тот же job в wait_condition, API active, слот занят, HTTP нет.
  Следующая цена 102 даёт один sendMessage и подтверждение в том же workflow.
- После durable pending_delivery при 101 подписка отменяется до допуска:
  API cancelled, слот свободен; после реального intake и повторного claim
  по истечении lease нет разрешений, attempts, success и Telegram-вызовов.
  Terminal PrepareRecipient сам по себе не переводит локальный claim в released;
  тест не требует отсутствующего terminal cleanup поведения.

Для двух последних случаев родитель кратко удерживает `LOCK TABLE
app.delivery_jobs IN SHARE MODE` в rollback-only транзакции notifications:
он задерживает intake/claims, пока через настоящие Latest/HTTP не наблюдается
99 либо отмена. Блокировка снимается также при ошибке до остановки процессов.
Она не создаёт/изменяет jobs, котировки, разрешения или результаты. Ожидания
проверяют SQL/HTTP/RPC состояния с deadline и опросом, без фиксированных sleep
как доказательства завершения. Итоговые счётчики проверяют ровно один job
на workflow, по одному допуску/attempt/success для доставленных подписок,
ноль для отменённой и ровно два Telegram запроса во всём сценарии.
GET subscriptions в конце пуст; четыре child должны завершиться exit 0,
быть reaped и закрыть listeners. Runner проверяет cleanup credentials,
process registry и обоих Compose projects с volumes.

Пакет запускается отдельно от smoke-пакета e2estack: каждый вызов `New`
требует собственного пустого dataset. Команда без DB/Kafka opt-in не является
E2E-проверкой. Raw market-data остаётся unconfirmed/readyz=503; по DEC-002/004
это предупреждение, а не запрет на доставку. SQL live не подставляется.
Публичные Binance/Telegram, нагрузка, SLA и real webhook не проверяются.
По DEC-010 после E2E-001 нагрузочный этап требует отдельного поручения.
