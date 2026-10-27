# Подключение Go к PostgreSQL

Драйвер — `github.com/jackc/pgx/v5` **v5.11.0**, закреплён в `go.mod`/`go.sum`.
Выбран native API и `pgxpool`: PostgreSQL — единственная СУБД проекта,
нужны явные транзакции и независимые соединения. На 27 октября 2026 версия
проверена по [официальным релизам](https://github.com/jackc/pgx/releases/tag/v5.11.0).
[Политика поддержки](https://github.com/jackc/pgx#supported-go-and-postgresql-versions)
включает используемые Go 1.26 и PostgreSQL 18.

`postgres.Open(ctx, dsn)` принимает PostgreSQL URL или keyword DSN, создаёт
пул и выполняет `Ping`: успешное создание ленивого пула ещё не означает
доступность БД. Ошибка закрывает пул. Пустой DSN отклоняется. Внутри не
выполняются миграции и не выбирается роль: runtime получает credentials
только своего владельца. См. [pgxpool API](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool).

Пример для будущего runtime:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
pool, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
    return err
}
defer pool.Close()
```

Конфигурацию передаёт вызывающий код через окружение/секрет-хранилище;
URL, пароль, `pool.Config()` и исходные ошибки парсинга/подключения не логировать.
`Open` возвращает безопасную ошибку без исходного DSN. Сведения о конкретной
причине подключения намеренно не раскрываются в этой ошибке. Для каждой операции
нужен **новый** context с deadline; context открытия не ограничивает последующие
запросы. После `Acquire` нужен `Release`, после `Begin` — отложенный `Rollback`
на случай ошибки и явный `Commit` при успехе. Cleanup rollback выполняйте с
отдельным ограниченным context, если context запроса уже отменён. Перед `Close`
освободите выданные соединения, иначе закрытие будет ждать их возврата.

## Реальные интеграционные проверки

Нужны Go из `.go-version`, Python 3.9+, Docker Engine/Compose и заполненный
игнорируемый `infra/postgres/.env` по [инструкции стенда](../../../infra/postgres/README.md).

Из корня:

```sh
python3 scripts/check_postgres.py
# Если порт занят:
GO_POSTGRES_CHECK_PORT=55437 python3 scripts/check_postgres.py
```

Скрипт использует только фиксированный проект **binance-go-check**, порт
loopback:55436, отдельную сеть и отдельный disposable volume. Он запускает
функциональный профиль 1 vCPU/1 GiB из SET-003, объединяет `migrations/api/*.sql`
и тестовую fixture во временный manifest и вызывает существующий `migrate.py`
под `api_migration`. Затем передаёт только пароль `api_runtime` через окружение
Go-процесса и запускает `go test -race -count=1 -v ./...`.
Пароль захватывается без вывода и отсутствует в аргументах процессов.
`sslmode=disable` используется только для локального loopback-стенда.

Fixture `app.connection_probe` содержит только bigint ID; это не схема аккаунта.
Её максимальная версия зарезервирована только в тестовом manifest и никогда
не применяется к рабочей БД. Проверка подтверждает текущую роль/БД, разные PID
двух одновременно удерживаемых соединений, невидимость незакоммиченной записи,
видимость после commit и сохранность записи после rollback удаления.
Тест удаляет свою строку, а runner в `finally` удаляет контейнер, сеть и **только
тестовый volume**, в том числе после ошибки миграции/Go-тестов. Ошибка cleanup
также даёт ненулевой код. При принудительном завершении самого Python-процесса
или недоступном Docker cleanup может не выполниться; повторите команду или:

```sh
docker compose -p binance-go-check -f infra/postgres/compose.yaml down --volumes
```

Рабочий `binance-storage` не адресуется. Одновременные запуски runner запрещены
локальной файловой блокировкой. Повторный запуск проверяет новую чистую БД.

Обычный `go test ./...` без `POSTGRES_TEST` явно пропускает интеграционный тест
(причина видна с `-v`). Для опционального подключения к уже подготовленному
**одноразовому** стенду задайте `POSTGRES_TEST=1` и `DATABASE_URL` через окружение.
При включённом opt-in отсутствие URL или недоступность БД — ошибка, никогда skip.
Пример отрицательной проверки (URL без секрета, закрытый порт):

```sh
POSTGRES_TEST=1 DATABASE_URL='postgres://api_runtime@127.0.0.1:1/api?sslmode=disable&connect_timeout=1' \
  GOTOOLCHAIN=local go test -count=1 -run '^TestTransactions$' ./internal/platform/postgres
# Ожидается FAIL, ненулевой код, "cannot connect to PostgreSQL".
```

## Прикладные хранилища

Добавьте прикладные миграции в `migrations/api/`; runner автоматически включит их
перед fixture. В интеграционном Go-тесте вызовите
`postgrestest.Open(t)` из `internal/testutil/postgrestest`: получите проверенный пул
с `t.Cleanup(pool.Close)`. Создавайте собственные уникальные тестовые записи и
регистрируйте их очистку; контексты операций задавайте в тесте. Для конкурентного
сценария удерживайте два `Acquire` одновременно, как в `TestTransactions`.
Общая команда выше подхватит новые тесты. IAM-001 добавил `app.users`,
репозиторий и конкурентную проверку email; контракт и воспроизведение —
в [account/README](../../api/account/README.md).
