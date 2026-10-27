# Пользовательские сессии — IAM-004

`session.New(pool, nil)` использует серверные часы; второй аргумент — optional
`func() time.Time` для проверок границы без ожидания. Нужна API runtime роль;
миграция `migrations/api/002_sessions.sql` применяется отдельно runner.

- `Issue(ctx, userID)` генерирует 256 случайных бит и возвращает `Credentials`
  только после успешной записи. Сохраняются SHA-256 строкового токена, FK user_id,
  expires_at; открытого токена в БД нет. TTL — `session.TTL`, 24 часа.
- `Verify(ctx, token)` проверяет canonical base64url (43 символа), наличие digest
  и строгое `expires_at > now`; возвращает доверенный user_id. На границе срока,
  для отсутствующей/поддельной/некорректной сессии — `ErrInvalid`.
- Ошибки БД — `ErrStorage` без SQL Detail; отмена context возвращается отдельно.
  При недоступности БД нельзя трактовать сессию как действительную.
- Часы UTC; время выдачи усечено до микросекунд PostgreSQL. `expires_at` в ответе
  взято из INSERT RETURNING. Доверенный production clock должен быть согласован
  между API-репликами. Verify проверяет срок в момент проверки, не продлевает его.
- Pool коммитит INSERT до возврата. При передаче pgx.Tx commit остаётся
  обязанностью вызывающего; HTTP handler должен получать committing pool.

Удаление пользователя удаляет сессии по FK cascade; индекс user_id поддерживает
эту операцию. Повторный вход не отзывает предыдущий токен. Cleanup истёкших строк,
logout/refresh и HTTP middleware не добавлены; IAM-005 использует Verify.
Credentials нельзя логировать. Передача — HTTPS Authorization Bearer.

Проверка: `python3 scripts/check_postgres.py` — реальный PostgreSQL, race detector,
успех выдачи, независимость токенов, digest/FK, срок до/на/после границы,
некорректный/поддельный токен, удалённый владелец и недоступное хранилище.
