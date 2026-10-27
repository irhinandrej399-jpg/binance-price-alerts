# Защищённые handlers — IAM-005

`auth.New(session.New(pool, nil), next)` проверяет сессию перед вызовом `next`.
Handler получает владельца через `auth.UserID(r.Context()) (string, bool)`.
Ключ context закрыт внутри пакета; владельца определяет только `session.Store.Verify`.
Поля user_id в query/body и X-User-ID не участвуют в аутентификации. При работе
с ресурсами handler обязан использовать полученный ID для проверки владения.

Принимается ровно один заголовок `Authorization: Bearer <token>`; регистр схемы
не важен. Несколько заголовков, объединённые credentials, другая схема, пустой
или неканонический токен отклоняются. Credentials в query/cookie не принимаются.
Формат токена и строгую границу `now < expires_at` проверяет
[session.Store](../session/README.md). Клиент использует HTTPS.

Отсутствующая, истёкшая и поддельная сессии дают одинаковый ответ:
401, `WWW-Authenticate: Bearer`, `{"error":{"code":"invalid_session"}}`.
Ошибка хранилища или отменённый context не допускают вызов handler и дают
500 `{"error":{"code":"internal_error"}}` без внутренних деталей.
Ответы отказа имеют Content-Type application/json и Cache-Control no-store.
Успешным ответом управляет защищённый handler.

Middleware подключаемый: runtime cmd/api и продуктовые защищённые endpoints
остаются последующим задачам. Администраторские роли, refresh/logout, лимиты
и авторизация доступа к конкретной подписке сюда не входят.

Проверки: `go test ./internal/api/auth` (некорректные/отсутствующие credentials),
`python3 scripts/check_postgres.py` (настоящая сессия, подмена владельца,
истечение, поддельный токен, отказ БД, отмена context; race detector).
