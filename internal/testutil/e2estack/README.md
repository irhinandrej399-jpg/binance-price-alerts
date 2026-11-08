# Четыре локальных процесса (E2E-002)

Из корня репозитория, с Go из `.go-version`, Docker Desktop и локальным
`infra/postgres/.env` по [инструкции](../../../infra/postgres/README.md):

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_e2e_environment.py --package ./internal/testutil/e2estack
```

Одна команда создаёт [disposable окружение E2E-005](../e2eenvironment/README.md),
применяет штатные миграции четырёх владельцев, собирает race executable
`cmd/market-data`, `cmd/api`, `cmd/matcher`, `cmd/notifications`, запускает
[Binance fake](../binancefake/README.md) и [Telegram fake](../telegramfake/README.md).
Публичные интеграции не вызываются. Рабочие данные не используются.
Без `E2E_PROCESS_INTEGRATION=1` Go-проверка явно SKIP; с opt-in отсутствие
любого из четырёх DSN или Kafka — FAIL.

`New(t, ctx)` выполняется **один раз на fresh dataset**, до запуска producers:

- Metadata фиксирует ClusterID/TopicID и partitions 16/1/16. Полный
  read_committed earliest/latest vector должен быть нулевым; непустой dataset
  отклоняется. Matcher получает полный `PRICES_BOOTSTRAP` 0 для 16 partitions.
- Notifications получает явный `intake.Store.Bootstrap` для 16 partitions,
  consumer ID и generation. При restart этот bootstrap не повторяется.
- Временный CA и отдельные server/client сертификаты лежат в `t.TempDir()`
  с mode 0600. DNS SAN проверяется как `market.test`; URI API, matcher,
  notifications и cleaner различаются. RPC обслуживают настоящие executable.
- Каждый child получает явный Env allowlist с единственным собственным DSN.
  Parent env, opt-in и чужие credentials не наследуются. Kafka IDs стабильны
  внутри dataset, prefixed и изолированы от другого запуска runner.

`stack.With(ctx, callback)` запускает market-data → API → matcher → notifications
через [apiprocess](../apiprocess/README.md). Callback начинается после `/healthz`;
он задаёт собственные deadline для операций. `stack.Ready(ctx)` проверяет все
четыре процесса и health endpoints. Ошибка или выход child прерывает проверку.
В штатном callback отправляется одна явная raw trade с точной decimal ценой,
проверяются ping/pong и настоящие mTLS Catalog/Latest. Подписки не создаются,
Telegram journal остаётся пустым.

| Процесс | Ожидаемая проверка |
| --- | --- |
| API | healthz=200, readyz=200, собственная БД доступна |
| matcher | healthz=200, readyz=200 после assignment/recovery/первой оценки |
| notifications | healthz=200, readyz=200, worker запущен |
| market-data | healthz=200, readyz=503 и feed=unconfirmed; Fetch/Latest доступны |

Raw feed по контракту не получает live от соединения или первой сделки.
Стенд не меняет SQL stream state и проверяет warning unconfirmed в Latest.
Динамический feed/backfill не реализуется здесь.

Остановка выполняется в обратном порядке; успешный возврат With требует
четырёх exit 0. `AssertStopped` проверяет reap и закрытие HTTP listeners.
Ошибки callback/startup/panic/Goexit очищаются тем же существующим lifecycle;
каждый stop имеет предел 22 с (включая watchdog сервисов до 20 с), затем KILL.
Тест запускает и останавливает весь stack дважды в одном dataset с исходными
source IDs/vector/generation/transactional IDs. Повтор команды создаёт другой
dataset; runner удаляет child registry, временные PKI, оба Compose project и
volumes, проверяя отсутствие containers/volumes/networks по точным labels.
Go-проверке даётся 300 с, внутреннему сценарию — 240 с. Внешний SIGKILL самого
Python требует ручного cleanup по точным labels, как описано в E2E-005.

Для следующего E2E-001 доступны `New`, `With`, оба fake, процессы с `URL`,
`MarketConn` и проверки `Ready`/`CheckQuote`. Callback может выполнять настоящий
HTTP пользовательский сценарий; этот тикет его не проверяет. Исправление
бизнес-поведения, реальная доставка, нагрузка и производительность не входят.

Полный пользовательский сценарий E2E-001 запускается отдельным
[пакетом e2escenario](../e2escenario/README.md) на собственном fresh dataset.
