# Telegram sendMessage — одна попытка

`New(Config{Token, Timeout, BaseURL})` создаёт переиспользуемый конкурентный
HTTP-клиент; Timeout обязателен и положителен, ограничивает запрос целиком,
включая чтение ответа. BaseURL по умолчанию `https://api.telegram.org`;
альтернативный HTTPS endpoint и loopback HTTP нужны для управляемых стендов.
Токен передаётся из конфигурации caller, не хранится в исходниках.

`Send(ctx, chatID int64, html string) (messageID int64, err error)` делает один
JSON POST `/bot<token>/sendMessage`, с `parse_mode=HTML`. Положительный private
chat ID соответствует IAM-008; caller передаёт адресата из актуального допуска
API и текст `message.Format` по котировке этого же допуска. HTML повторно не
экранируется. Пустой текст/неположительный chat/уже отменённый context не
отправляются. Ограничения длины и корректности HTML проверяет Telegram.

Успех — 2xx, корректный JSON, `ok=true` и положительный `result.message_id`.
Ошибка — `*telegram.Error` (через `errors.As`):

| Kind | Значение |
| --- | --- |
| NotSent | Локальный отказ до вызова HTTP; запроса не было |
| RateLimited | 429; положительный `RetryAfterSeconds` из `parameters.retry_after`, иначе 0 (неизвестен) |
| Forbidden | 403, проблема канала |
| Server | 5xx, серверная ошибка |
| Rejected | Остальные 4xx; точный код доступен caller для политики |
| Unknown | Потеря/обрыв/отмена запроса, timeout, повреждённый/неполный/слишком большой ответ либо redirect; доставка могла состояться |

`HTTPStatus` — полученный HTTP status (0 без ответа); `Code` — error_code из
корректного JSON; `Timeout` отмечает deadline/сетевой timeout, в том числе при
чтении тела. HTTP 4xx/5xx классифицируется и при не-JSON теле; при ошибке чтения
тела исход консервативно Unknown. Для `ok=false` с HTTP 2xx классифицируется
error_code. Тело ответа ограничено 1 MiB. Поля ошибок не включают description,
текст, chat, URL, bot token или исходную `url.Error`; пакет ничего не логирует.

Redirect не выполняется. Автоматических повторов POST, ожидания retry_after,
лимитера и сохранения исхода нет. Unknown не доказывает отсутствие доставки:
caller сохраняет неоднозначный исход по DEC-004; повтор может дать внешний дубль.
Server также не является доказательством отсутствия побочного эффекта. Повтор,
blocked-политика, leases и runtime реализуются следующими тикетами. После
ожидания/рестарта нужен новый допуск API; этот адаптер сам допуск не проверяет.

Контракт повторно сверен 30 октября 2026 с официальными
[правилами запросов](https://core.telegram.org/bots/api#making-requests),
[sendMessage](https://core.telegram.org/bots/api#sendmessage) и
[ResponseParameters](https://core.telegram.org/bots/api#responseparameters).
Потеря ответа означает возможную доставку — инженерный вывод, а не внешняя
гарантия идемпотентности Telegram.

Проверка: `GOTOOLCHAIN=local go test -race ./internal/notifications/telegram -count=1`.
Fake HTTP покрывает success/HTML и большой chat ID, 429/retry_after, 403, 4xx,
5xx, некорректный/огромный ответ, обрыв после приёма тела, timeout до заголовков
и в теле, context deadline/cancel, отсутствие redirect и повторов. Реальные
сообщения и bot credentials для тестов не нужны.
