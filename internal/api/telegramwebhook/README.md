# Telegram webhook — IAM-007

`telegramwebhook.Handler(secret, next)` возвращает `(http.Handler, error)` для
`POST /v1/telegram/webhook`. `next` — обработчик Telegram Update, который добавит
IAM-008. Конструктор отклоняет nil-обработчик и секрет вне формата Telegram:
1–256 ASCII-символов `A-Z`, `a-z`, `0-9`, `_`, `-`. Ошибки не содержат секрет.

Запрос должен иметь ровно одно значение `X-Telegram-Bot-Api-Secret-Token`, точно
совпадающее с настроенным секретом. Сравниваются SHA-256 через constant-time
compare. Пробелы не обрезаются; дубли и значения через запятую отклоняются.
Отсутствующий/неверный секрет даёт 403, JSON
`{"error":{"code":"invalid_webhook_secret"}}`, `Cache-Control: no-store`.
Проверка выполняется до чтения тела и вызова `next`. Query-параметры не служат
аутентификацией. Неверный метод даёт 405, неизвестный путь — 404.

Допущенный запрос передаётся в `next` без изменения тела; код ответа принадлежит
обработчику update. Он должен подтвердить приём только после необходимой записи:
Telegram повторяет доставку при ответе вне 2xx. Секрет подтверждает знание общего
ключа; защита от повторного update и идемпотентность обработки сюда не входят.

## Подключение и регистрация

Runtime `cmd/api` пока пуст. При его сборке создать адаптер с секретом из
окружения/secret manager, обработать ошибку конструктора как ошибку запуска и
зарегистрировать на внешнем mux по `/v1/telegram/webhook`. Не оборачивать маршрут
в Bearer middleware: Telegram передаёт собственный webhook-секрет.
Внешний адрес должен быть HTTPS с доверенным сертификатом; proxy обязан сохранять
заголовок и не писать его или тело update в логи. Секрет и bot token не должны
попадать в репозиторий, URL webhook, командную строку или диагностические дампы.

Оператор задаёт `TELEGRAM_BOT_TOKEN`, `TELEGRAM_WEBHOOK_SECRET`,
`TELEGRAM_WEBHOOK_URL` вне репозитория. Рекомендуется случайный секрет, например
`secrets.token_urlsafe(32)`, независимый от bot token. В API при запуске используется
то же значение `TELEGRAM_WEBHOOK_SECRET`. Следующий пример выполняется вручную
после готовности публичного endpoint; он регистрирует только message updates:

```python
import json
import os
import urllib.request

payload = json.dumps({
    "url": os.environ["TELEGRAM_WEBHOOK_URL"],
    "secret_token": os.environ["TELEGRAM_WEBHOOK_SECRET"],
    "allowed_updates": ["message"],
}).encode()
request = urllib.request.Request(
    "https://api.telegram.org/bot" + os.environ["TELEGRAM_BOT_TOKEN"] + "/setWebhook",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.load(response)
    if result.get("ok") is not True or result.get("result") is not True:
        raise ValueError("registration failed")
except Exception:
    # Не печатать exception: URL может содержать bot token.
    raise SystemExit("Webhook registration failed; inspect configuration privately") from None
print("Webhook registered")
```

Проверить `getWebhookInfo` тем же способом, не публикуя ответ с адресом/ошибками.
Не задавать `drop_pending_updates` без отдельного намерения удалить ожидающие
updates. При смене секрета согласовать конфигурацию API и повторный `setWebhook`;
во время рассогласования запросы будут отклоняться и повторяться Telegram.
Здесь реальный webhook не регистрировался: публичного runtime и credentials нет.

Контракт сверён 27 октября 2026 по официальному
[setWebhook](https://core.telegram.org/bots/api#setwebhook).
Проверки: `go test ./internal/api/telegramwebhook`,
`go test -race ./internal/api/telegramwebhook`.
