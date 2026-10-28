# Запрос работы доставки: SUB-014

Unary `/api.v1.Delivery/RequestDelivery`, схема
[`delivery.proto`](../../../proto/api/v1/delivery.proto), generated Go —
`internal/gen/api/v1`. Запрос содержит UUID владельца/подписки и положительную
ожидаемую версию. Цену, пару и порог передать нельзя. Владелец в этом запросе —
ключ проекции доверенного matcher, не пользовательская сессия.

API читает собственную каноническую запись по обоим ID, получает последнюю
котировку через `latest.Client` (MKT-006) и проверяет сохранённые порог/направление.
Старая корректная цена, unconfirmed и gap допустимы без TTL; исходные времена,
source ID и ревизия сохраняются в диагностике работы. Missing, invalid_time,
будущее или некорректная котировка откладывают создание работы.
Затем готовая `subscription.RequestDelivery` (SUB-013) повторно проверяет
каноническое состояние/версию/условие под блокировками и атомарно пишет workflow,
delivery_outbox и журнал. Сетевых вызовов внутри транзакции нет.

Повтор по тем же owner/subscription возвращает сохранённый workflow и текущее
состояние даже со старой ожидаемой версией, при недоступной цене и в terminal
состоянии. Идентичность восстанавливается до чтения цены; новые события и слоты
не создаются. Неоднозначная ошибка commit/потеря ответа требует повтора той же
команды. Повтор после отказа по условию или недоступности проверяет новую
последнюю цену. Автоматических прикладных retries клиент не добавляет.

Подключение будущим runtime (секреты и URI из доверенной конфигурации):

```go
opts, err := deliveryrpc.ServerOptions(certPEM, keyPEM, clientCAPEM, matcherURI)
// Проверить err; установить весь набор options без переопределений.
server := grpc.NewServer(opts...)
pb.RegisterDeliveryServer(server, deliveryrpc.NewServer(
    deliveryrpc.NewPostgres(apiPool), latest.NewClient(marketConn), time.Now))
```

Только точная mTLS URI SAN matcher получает право на этот RPC.
API, notifications, публичный Bearer и подставленные metadata этого права не дают.
Клиент: `deliveryrpc.NewClient(conn).RequestDelivery(ctx, ownerID, subscriptionID, version)`;
соединение создаётся с `serviceauth.ClientCredentials` и ожидаемым DNS сервера.
Для нескольких RPC на одном сервере следует составить одну общую политику через
`serviceauth.ServerOptions`; см. [контракт mTLS](../../platform/serviceauth/README.md).

Клиент и сервер ограничивают весь запрос 3 секундами, сохраняя более ранний
caller deadline. В него входят SQL-чтение, RPC последней цены (до 1 с) и
транзакция. Context передаётся во все зависимости, в том числе ожидание SQL-lock.
Это начальный бюджет, не измеренный SLA/p99; нагрузка остаётся отдельной задачей.
Котировка между чтением и commit может измениться: этот запрос создаёт работу,
допуск непосредственно перед Telegram выполняется отдельно по DEC-004.

| Результат | gRPC |
| --- | --- |
| Создан или восстановлен workflow | OK + версия/состояние/workflow |
| Неверный UUID или версия | InvalidArgument |
| Нет записи по обоим ID | NotFound |
| Версия изменилась без существующего workflow | Aborted |
| Условие ложно / состояние не active | FailedPrecondition |
| Цена отсутствует/некорректна, БД/зависимость недоступна | Unavailable без исходных ошибок |
| Нет права у доверенного сервиса | PermissionDenied до handler |
| Ошибка TLS / API недоступен | Unavailable |
| Deadline / отмена | DeadlineExceeded / Canceled |

Проверки:

```sh
go test -race ./internal/api/deliveryrpc -count=1 -timeout=30s
python3 scripts/check_postgres.py
python3 scripts/generate_proto.py --check
```

Первый прогон использует настоящий mTLS loopback и две защищённые связи
matcher → API → market-data; SQL-тест без opt-in пропускается. Второй поднимает
одноразовый PostgreSQL и проверяет настоящую SUB-013 через RPC: ложную последнюю
99, подходящую 101, stale CAS, missing, 10 конкурентных команд с одним workflow,
неизменяемую диагностику/outbox, повтор при missing, изоляцию владельца, отмену
ожидания SQL-lock и повтор после timeout. Публикация outbox (SUB-009), runtime,
PKI окружения, admission/attempt/results и Telegram сюда не входят.
