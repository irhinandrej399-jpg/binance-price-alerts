# Health notifications

`health.New(notificationsPool.Ping, worker.Running, time.Second)` создаёт HTTP
handler. Runtime вызывает `SetInitialized(true)` после сборки обязательных
компонентов, `SetInitialized(false)` перед остановкой. Пока `Worker.Run` не
начался или уже вернулся, readiness отсутствует, даже при доступной БД.

- `GET /healthz`: 200 `{"status":"ok"}`, без SQL, worker callback и внешних вызовов.
- `GET /readyz`: 200 `{"status":"ready"}` при инициализации, запущенном worker
  и доступности собственной durable jobs БД; иначе 503 `{"status":"not_ready"}`.

Ответы JSON с `Cache-Control: no-store`, без текста внутренних ошибок.
Nil callback, SQL-ошибка, отмена запроса и deadline дают 503. Ping получает
HTTP context и timeout (по умолчанию 1 с), обязан соблюдать отмену и поддерживать
конкурентные вызовы. После Ping повторно проверяются инициализация и Running:
поздний успех не скрывает остановку. Неизвестный путь — 404, другой метод — 405;
HEAD поддерживается ServeMux.

`Worker.Running()` — атомарное локальное наблюдение владения вызовом Run.
Оно истинно при пустой очереди, ожидании, обычных ошибках, Telegram 429/5xx и
повторах PendingWrite; ложно до Run и после любого возврата. Это наблюдение
lifecycle, оно не доказывает прогресс или успешность последнего SQL/HTTP.
Отмена ещё завершающегося Run сама по себе не снимает флаг: runtime обязан
отозвать SetInitialized перед shutdown. HTTP-наблюдение не выдаёт lease/permit
и не участвует в fencing.

Telegram не опрашивается из health endpoints. Временный отказ, rate limit,
ожидание retry_after, отсутствие заданий и недоступность API не создают
liveness-рестартов. SQL-ошибка снимает readiness, но не liveness; восстановление
проверяется новым запросом. Проверка Ping показывает доступность БД, не
гарантирует успешную запись любого будущего задания/квоту диска. Миграции должны
быть применены до инициализации. Health не отправляет сообщения и не меняет jobs.

Проверки: `GOTOOLCHAIN=local GOCACHE=/tmp/binance-go-build go test -race ./internal/notifications/health ./internal/notifications/worker`.
HTTP-тесты проверяют инициализацию, отказ/восстановление storage probe, deadline,
отмену, конкурентность, остановку worker; настоящий Worker.Run и loopback
Telegram sender проверяют ответ 429 с retry_after без потери health/readiness.
Source/Handler в этом тесте управляемые: durable запись retry проверена
зависимостями NTF-012/013, SQL/Kafka здесь не подменяются интеграционным прогоном.

`cmd/notifications` подключает listener, Ping notifications pool, инициализацию,
Worker.Running и общий lifecycle intake/worker в OPS-055. Process/race сценарий
проверяет HTTP 200 и сохранение health/readiness при 429; supervisor проверяет
503 до инициализации, при SQL probe failure и после выхода компонента.
Четыре процесса, SIGTERM (OPS-011), реальный Telegram и нагрузка отдельно.
