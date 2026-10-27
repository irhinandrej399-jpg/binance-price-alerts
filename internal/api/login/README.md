# Вход — IAM-004

`login.New(account.New(pool), session.New(pool, nil))` возвращает подключаемый
HTTP handler для `POST /v1/sessions`. Runtime cmd/api пока не подключён.

Запрос: `Content-Type: application/json`, объект только с `email` и `password`.
Email нормализуется account.NormalizeEmail, пароль проверяется без trim/изменений.
Отсутствующие/пустые поля, malformed JSON, неизвестные поля или данные после
JSON дают 400 invalid_request. Непустой неизвестный email (включая неправильный
формат) и неверный пароль дают одинаковые status/body 401 invalid_credentials;
для неизвестного аккаунта выполняется dummy Argon2id того же профиля.
Это не обещание идентичного времени ответа на все запросы.

Ответ 200:

```json
{"access_token":"<43 base64url символа>","token_type":"Bearer","expires_in":86400,"expires_at":"2026-10-28T12:00:00Z"}
```

Токен имеет 256 случайных бит, TTL 24 часа. Только digest хранится в API БД;
[контракт хранилища и проверки](../session/README.md),
[согласованное решение](../../../docs/decisions/IAM-004.md).
Другой Content-Type — 415 unsupported_media_type; ошибка БД — 500 internal_error.
Форма ошибок: `{"error":{"code":"invalid_credentials"}}` без внутренних деталей.
Ответы endpoint имеют JSON Content-Type и Cache-Control: no-store.
Стандартный ServeMux возвращает 404/405 для другого пути/метода.

Успех не раскрывает email, password_hash или user_id; только выданные credentials.
Неверный пароль/отсутствующий аккаунт не создают сессию. Повреждённый сохранённый
хеш отклоняется как invalid_credentials. Dummy-хеш создаётся один раз при New;
handler создаётся при старте, а не на каждый запрос.

Клиент передаёт токен по HTTPS в `Authorization: Bearer <token>`.
Защита endpoints/middleware — IAM-005. Refresh, восстановление пароля, runtime,
лимит тела OPS-012 и распределённый rate limit OPS-013 сюда не входят.

Проверка: `go test ./internal/api/login` (валидация без БД),
`python3 scripts/check_postgres.py` (вход и срок с реальными account/password/
session, безопасные ошибки обоих хранилищ, race detector).
