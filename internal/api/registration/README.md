# Регистрация — IAM-003

`registration.New(account.New(pool))` возвращает `http.Handler` для
`POST /v1/accounts`. Его можно подключить к основному mux через
`mux.Handle("/v1/accounts", registration.New(account.New(pool)))`.
Передаётся repository на pool с самостоятельным commit INSERT, а не на внешней
транзакции. Runtime-конфигурация и запуск `cmd/api` пока не реализованы.

Запрос — `application/json` (допустим параметр charset):

```json
{"email":"Alice+tag@example.com","password":"example password"}
```

Ответ 201 содержит только `id` (UUID) и `email` (`alice+tag@example.com`).
Email нормализуется через IAM-001; формат проверяется `net/mail.ParseAddress`
как один адрес без display name, комментариев и whitespace. Доставляемость,
DNS и подтверждение email не проверяются. Пароль непустой, без trim и иной
нормализации; сохраняется только результат `password.Hash` из IAM-002.

Тело должно содержать единственный JSON-объект с полями email/password.
Неизвестные поля, неправильные типы, пустые credentials, неверный email,
некорректный JSON и данные после JSON возвращают 400 `invalid_request`.
Дубль нормализованного email — 409 `email_exists`; другой Content-Type —
415 `unsupported_media_type`; сбой хранилища — 500 `internal_error`.
Форма ошибок: `{"error":{"code":"invalid_request"}}`.
Стандартный ServeMux возвращает 405 для другого метода и 404 для другого пути.

Credentials и ошибки БД не сериализуются и не логируются. Ответ не выдаёт
сессию или refresh token. Общий лимит тела и распределённый rate limit —
отдельные OPS-012/013; самостоятельный runtime этим handler не запускается.

Проверки: `go test ./internal/api/registration` — валидация и маршрутизация;
`python3 scripts/check_postgres.py` — дополнительно реальные HTTP-handler
сценарии с PostgreSQL под race detector: неверный запрос не создаёт аккаунт,
успешный сохраняет проверяемый Argon2id-хеш, дубль не меняет запись,
ошибка закрытого pool возвращает безопасный ответ. Используется httptest,
внешний listener не нужен; disposable БД и volume удаляются runner.
