# Управляемый Telegram fixture (E2E-004)

`telegramfake.New()` открывает случайный loopback HTTP-порт. Родитель стенда
владеет fake и обязательно вызывает `Close()`; дочернему notifications передаёт
`f.URL()` как Telegram BaseURL и `telegramfake.Token` как тестовый token.
Настоящий `telegram.Client` использует этот endpoint вместо публичного Telegram.
Секреты и реальные bot credentials не нужны. Каждый fake изолирован.

```go
f := telegramfake.New()
defer f.Close()
// Запустить notifications с BaseURL=f.URL(), Token=telegramfake.Token.
call, err := f.Next(ctx) // ctx с deadline; дождаться полного sendMessage
// Проверить err, call.Request.ChatID/Text/ParseMode.
call.Reply(73)          // успешный message_id
// Для других запросов выбрать одно действие:
// call.RateLimit(17)   // 429, retry_after=17 секунд
// call.Forbid()       // 403, постоянная ошибка канала
// call.Disconnect()   // полный запрос принят, TCP закрыт без ответа
journal := f.Requests() // независимая копия принятых запросов
_ = journal
```

Ответ удерживается до действия сценария; первый ответ побеждает, повторное
действие безопасно. ID и retry_after должны быть положительными (иначе panic).
Обрыв означает неоднозначный исход; fake не доказывает отсутствие доставки.
Никаких автоматических повторов, сообщений или ответов success нет.
При конкурентных вызовах сопоставляйте ChatID/текст; порядок прихода сетевых
запросов не определён. Журнал и Next имеют одинаковый порядок приёма.
Не меняйте call.Request конкурентно с его чтением своим кодом.

Принимается только POST `/bot<test token>/sendMessage`, Content-Type
`application/json`, полный JSON до 64 KiB, положительный int64 chat_id,
непустой text и parse_mode=HTML. Текст и большие chat ID сохраняются точно.
Неверные запросы получают 400/404/405 и не попадают в журнал; очередь Next
вмещает 16 ещё не забранных запросов, переполнение даёт 503 без записи.
Журнал хранится в памяти до освобождения fake; это функциональный helper,
его не следует использовать как неограниченный нагрузочный collector.
Удержание ограничено 20 с, затем 504. Отмена клиента освобождает handler,
принятый запрос остаётся в журнале. Next отменяется context или Close.
Close идемпотентен, закрывает активные TCP-соединения (включая неполные тела)
и присоединяет handlers; вызывайте его после остановки дочерних отправителей.

Для привязки используйте настоящий webhook/link handler на тестовом API:

```go
status, err := telegramfake.Link(ctx, apiOrigin, 42, chatID, linkCode)
// Проверить err и status: не-2xx возвращается явно, без автоматического retry.
```

Link отправляет заданный update_id и обычное private message с `/start <code>`
на фиксированный `/v1/telegram/webhook`, с заголовком
`X-Telegram-Bot-Api-Secret-Token: e2e_test_webhook_secret` (`telegramfake.Secret`).
API стенда должен быть настроен на этот же тестовый секрет. code берётся из
реальной выдачи кода API; helper не создаёт и не поглощает код вместо API.
Origin допускает только HTTP с literal loopback IP, без credentials/query/path
(кроме `/`). DNS, proxy и redirect не используются. Один запрос ограничен
context и 2 с; тело ответа читается до 64 KiB и не возвращается в ошибках.
Снаружи стенда Link не отправляет update и не регистрирует webhook.

Проверка через настоящие sender, telegramwebhook.Handler и telegramlink.Handler
(Consumer в тесте фиксирует code/chat, БД и полный E2E здесь не требуются):

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race -count=10 ./internal/testutil/telegramfake
```

Проверены все ответы, точный wire payload/секрет, большие int64, журнал,
первый ответ, конкурентность, отмена, неправильные запросы, запрет внешних
origin/redirect, чужой секрет, Close удержанного запроса и неполного TCP-тела.
Старый `notificationsprocess.NewTelegram` остаётся совместимым узким helper
его процессных тестов; новый стенд использует этот пакет. CLI/control endpoint,
полный E2E четырёх процессов и лимиты нагрузки 25/с и 1/с на chat по DEC-008
в этот функциональный fixture не входят; лимиты не объявляются проверенными.
