# Общий лимитер аутентификации — OPS-013

`New(apiPool, nil).Handler(next)` подключается после bodylimit к POST-маршрутам
`/v1/accounts` и `/v1/sessions`. Оба используют одну квоту: 20 попыток за
фиксированную минуту UTC на канонический IP из RemoteAddr. IPv4-mapped IPv6
приводится к IPv4, IPv6 zone и некорректный адрес отклоняются. Forwarded и
X-Forwarded-For игнорируются. За proxy/NAT пользователи делят квоту.

Схема — `migrations/api/015_auth_limits.sql`, только API PostgreSQL. Уникальная
строка IP и транзакционная блокировка сериализуют допуски всех реплик.
После блокировки время читается через clock_timestamp(), окно — [минута,
следующая минута). В новом окне строка переиспользуется; откат часов не
сбрасывает счётчик. Допуск возвращается после commit. Отказ handler и
прерывание запроса после commit попытку не возвращают; неизвестный commit
даёт отказ, хотя квота могла быть потрачена. Вызовов credentials под SQL-lock нет.

Все допущенные POST-запросы учитываются до JSON, Argon2id и account/session,
независимо от результата. 404/405 и превышение внешнего bodylimit не учитываются.
Отклонённый лимитером запрос счётчик не увеличивает. SQL-допуск имеет бюджет
1 с, учитывает более раннюю отмену HTTP context. Следующий handler получает
исходный context запроса. При ошибке транзакция откатывается с отдельным
cleanup context; при неизвестном результате commit обработка не разрешается.

Ошибки JSON `{"error":{"code":"..."}}`, без адреса, credentials и SQL-деталей:

| Статус | Code | Дополнительно |
| --- | --- | --- |
| 400 | invalid_request | Некорректный RemoteAddr |
| 429 | rate_limited | Retry-After: целые секунды вверх до конца окна |
| 503 | auth_limit_unavailable | Сбой/таймаут/отмена SQL-допуска |

Ошибки имеют Cache-Control: no-store. IP и секреты не логируются и не
используются в labels. Для тестов можно передать clock вместо nil;
production использует только общее время БД. `Allow` — низкоуровневая операция
над IP без порта, caller задаёт deadline; HTTP Handler задаёт его сам.

Ограничения: фиксированное окно позволяет до 40 попыток на границе минут,
автоматической очистки строк нет (рост по уникальным IP), распределённые
источники и нагрузка не квалифицированы. Одна общая БД не означает HA.
[Согласованная политика](../../../docs/decisions/OPS-013.md).

Проверки: `go test ./internal/api/authlimit` для валидации адреса;
`python3 scripts/check_postgres.py` для реальной конкуренции двух pools,
временных границ, HTTP credentials, общей квоты, clock/timeout/cancel и
безопасного отказа хранилища под race detector. Настоящий cmd/api:
`python3 scripts/check_api_process.py --package ./cmd/api`.

В публичном профиле OPS-075 внешний middleware clientip нормализует RemoteAddr
только для одного TRUSTED_PROXY_IP. Сам limiter forwarding headers не читает.
Контракт доверия — [PUBLIC.md](../../../infra/deploy/PUBLIC.md).
