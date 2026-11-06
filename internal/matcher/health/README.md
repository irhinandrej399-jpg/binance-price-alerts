# Health matcher

`health.New(matcherPool.Ping, replica.Status, time.Second)` создаёт HTTP handler.
Runtime вызывает `SetInitialized(true)` после сборки обязательных компонентов,
`SetInitialized(false)` при остановке. Обе функции callback поддерживают
конкурентные вызовы; Ping соблюдает context, Status — быстрое локальное чтение.

- `GET /healthz`: всегда 200 `{"status":"ok"}`, без SQL/RPC.
- `GET /readyz`: 200 при готовности либо 503; JSON содержит `status` и `projection`
  с `ready`, `applied_c`, `head`, `lag`. JSON не кэшируется (`no-store`).

Readiness требует инициализации, доступности собственной БД и `replica.Status().Ready`.
Это сохраняет условия Replica.Ready: живое назначение, завершённый Pending recovery,
первая оценка активных пар и готовая live view. View требует завершённого snapshot,
replay, индекса, Latest-подключений, действующего pin и живых регистраций.
Пустая полностью синхронизированная проекция может быть готова; пустой старт или
частично загруженный snapshot готовности не дают. Потеря регистрации, истечение
pin, rebalance и остановка снимают готовность. Health не выдаёт полномочия и
не подменяет fencing. Сам HTTP-компонент не делает snapshot/replay и не меняет SQL.

`live.Coordinator.Status()` возвращает согласованное локальное наблюдение;
`replica.Status()` дополнительно учитывает назначение и recovery. `applied_c` —
последний успешно прочитанный durable checkpoint текущего поколения; `head` —
последняя валидированная граница API replay. `lag = max(head - applied_c, 0)`
считает позиции журнала подписок. До получения head `lag: null` (нулевая head
сама по себе не доказывает синхронизацию). Kafka может опережать последний
наблюдённый head: разность тогда нулевая, без uint64 underflow. Новый head
наблюдается штатным периодическим replay, HTTP не вызывает API. Значения
диагностические: это не текущий глобальный head, не секунды задержки, не ценовой
prefix и не число ожидающих уведомлений. Порог допустимого отставания не вводится;
готовность берётся из существующего lifecycle. Исторические наблюдения могут
сохраняться после потери готовности и не доказывают её.

При ошибке SQL, nil callback, отмене или deadline — 503 с пустым наблюдением
(`ready:false`, `lag:null`), без текста внутренних ошибок. Ping получает deadline
(по умолчанию 1 с) и отмену HTTP; поздний успех не становится готовностью.
Инициализация перепроверяется после probes. HEAD поддерживается ServeMux,
неизвестный путь даёт 404, неподдерживаемый метод — 405.

Проверка: `GOTOOLCHAIN=local GOCACHE=/tmp/binance-go-build go test -race ./internal/matcher/health ./internal/matcher/live ./internal/matcher/replica`.
SQL/Kafka-протоколы и Ping уже проверяются средствами зависимостей; OPS-007
проверяет локальное наблюдение и HTTP. `cmd/matcher` пока каркас: HTTP listener,
инициализация и wiring `replica.Status`/pool нужны при сборке runtime перед
E2E-002. Работающий процесс и нагрузка этим компонентом не подтверждены.
