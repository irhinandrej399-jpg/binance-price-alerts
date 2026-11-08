# Общее disposable окружение (E2E-005)

Из корня репозитория, с Go из `.go-version`, Docker Desktop и локальным
`infra/postgres/.env` по [инструкции PostgreSQL](../../../infra/postgres/README.md):

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_e2e_environment.py
```

Обёртка использует `check_api_process.main(owner="e2e")`: существующие
Compose/init/migrate и [apiprocess lifecycle](../apiprocess/README.md), без
второго supervisor. Создаются уникальные `binance-process-pg-<random>` и
`binance-process-kafka-<random>`, случайные loopback порты и `process-<random>`
topic prefix. Применяются штатные миграции всех четырёх владельцев; test-only
notifications fault/reset SQL отсутствует. Рабочие projects/volumes не адресуются.

Родительский Go-пакет получает:

| Env | Назначение |
| --- | --- |
| `E2E_PROCESS_INTEGRATION=1` | Явный opt-in общего окружения |
| `POSTGRES_TEST=1` | Opt-in существующих SQL helpers |
| `DATABASE_URL` | API runtime |
| `MARKET_DATA_DATABASE_URL` | market_data runtime |
| `MATCHER_DATABASE_URL` | matcher runtime |
| `NOTIFICATIONS_DATABASE_URL` | notifications runtime |
| `KAFKA_TEST_BROKER` | Loopback bootstrap broker |
| `KAFKA_TEST_PREFIX` | Префикс трёх новых topics |
| `API_PROCESS_PID_DIR` | Приватный registry групп child для apiprocess |
| `TMPDIR` | Временный каталог под контролем Python runner |

Унаследованные DSN, application settings и opt-in удаляются. Secrets считываются
в память, не передаются в argv и не печатаются. DSN доступны только родителю;
для каждого child задаётся явный `apiprocess.Config.Env` с **одним своим DSN**
и необходимыми ему Kafka/RPC/HTTP/PKI параметрами. Env родителя не наследуется.
Временные credentials создавать под `t.TempDir()`/`TMPDIR`, чтобы Python удалил
их также при аварийном Go owner exit. Не логировать Env/DSN.

Qualification проверяет DB name/runtime role каждого владельца и настоящий
отказ CONNECT (`42501`) к трём чужим БД, также из отдельного child на каждую
роль. Kafka Metadata подтверждает непустой ClusterID и разные ненулевые TopicID:
`<prefix>-prices` — 16, `-subscription-changes` — 1, `-delivery-requests` — 16.
ListOffsets earliest/latest равны 0 у всех partitions. Это discovery только
нового пустого dataset; старые checkpoints/offsets/SQL generations к нему
не привязываются. Никаких сообщений qualification не публикует.

Для E2E-002 используется та же команда с другим пакетом:

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_e2e_environment.py --package ./path/to/e2e/package
```

Путь здесь — место будущего пакета E2E-002. Он сам собирает четыре executable,
PKI, fake endpoints и явный source/bootstrap через готовые интерфейсы. Здесь
runtime и полный пользовательский сценарий не запускаются. Restart процесса
в одном dataset сохраняет его source identity; новый runner создаёт другой dataset.

Все команды ограничены deadlines; Go-пакет получает 300 с, Python — 330 с.
После успеха/ошибки/timeout runner убивает оставшиеся зарегистрированные child
process groups, удаляет PID registry и TMPDIR, выполняет down обоих Compose
projects с volumes и проверяет отсутствие containers/volumes/networks по точным
labels. Ошибка cleanup приводит к exit 1. Произвольный внешний SIGKILL самого
Python не выполняет finally: тогда требуется ручная уборка по точным project labels.

Намеренно аварийный Go owner, ожидается exit 1 с подтверждённым cleanup:

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_e2e_environment.py --package ./internal/testutil/apiprocess/testdata/ownerexit
```

Без `E2E_PROCESS_INTEGRATION=1` qualification явно SKIP. С opt-in отсутствие
любого DB/Kafka параметра или недоступная инфраструктура — FAIL.
Нынешние API/market-data/matcher/notifications/cleaner режимы сохраняют свои
DSN, opt-in и наборы миграций.
