# E2E-007 — Малое восстановление принятой работы

9 ноября 2026. Исходная ревизия `b3d08f1`, изменения E2E-007 поверх неё.
Один функциональный результат проверяется существующим Compose smoke и
готовыми process/E2E fixtures; универсальный fault runner не добавлен.
Production Go/SQL/config не менялись. Это несколько подписок, без нагрузки.

## Воспроизведение

Из корня checkout, Go из `.go-version`, Docker Desktop, Python и OpenSSL.
Для process runners нужен локальный `infra/postgres/.env` по существующему
[контракту](../../../internal/testutil/e2estack/README.md). Compose smoke сам
генерирует disposable credentials и не принимает постоянный project.

```sh
python3 infra/deploy/smoke.py --broker-restart
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py --package ./cmd/matcher
GOCACHE=/tmp/binance-go-build python3 scripts/check_notifications_process.py --package ./cmd/notifications
GOCACHE=/tmp/binance-go-build python3 scripts/check_e2e_environment.py --package ./internal/testutil/e2escenario
```

Каждая команда создаёт собственный dataset. Их можно выполнять последовательно.
В этой сессии отдельные стенды частично пересекались по времени; это не тест
нагрузки или сравнение реплик. Полный matcher runner вернул **exit 1** из-за
самостоятельного startup-сбоя source-теста, описанного ниже; целевой crash-тест PASS.
Без opt-in обычный Go test эти интеграции пропускает.

## Наблюдения и критерии

- **Kafka offline.** После первой подтверждённой доставки и при второй active
  подписке host останавливает только Kafka. Fake Binance передаёт цену 102;
  настоящий Latest подтверждает source ID/цену/unconfirmed, SQL observer читает
  сохранённый outbox с `published=false`. После возврата Kafka та же строка/event
  получает `published=true`. Source-provision verify подтверждает прежние
  ClusterID/TopicIDs/binding, inventory побайтово тот же. Подписка и первая
  доставка сохранены; новая цена 201 доставляет вторую, всего ровно два HTTP.
  Финальный Compose PASS (93.14 s), exit 0, cleanup PASS. Наблюдаемая строка:
  position=5, event_id=`c51fb7ed008645b59c8df1521d71ff81`; сохранённая подписка
  `d628ba6e-35cb-404f-9cb7-d071da7866f3`, ранее delivered
  `f7eca770-abaf-4cf7-9062-3d9a2740c219` не переотправлена.
  Полный stop/start deployment остаётся отдельным обычным OPS-074 smoke.
- **Matcher SIGKILL.** Готовый `TestMatcherProcess` удерживает RequestDelivery
  после canonical commit. До SIGKILL сохранены 3 кандидата и 1 результат,
  state=pending, price offset=0, durable changefeed next=6. Work
  `view:f8bd182f6cedf70fbb53f71419b2ec0d2020cf72fe0d1bea69e3668c12e99758`,
  trigger `f62d1503-6095-4f80-8c7b-f3fbb33cdca5`. После возврата с Latest=99
  тот же work явно superseded, SQL и broker next=1, прежний trigger не изменён,
  третий ложный кандидат не создаёт исторический workflow. PASS, 94.88 s.
- **Notifications pending job.** Готовый `TestNotificationsProcess`: trigger
  `b859759f-dc6a-4f43-94df-73549e6b1281`, job body/IDs/checkpoint сохранены.
  Latest=99 сначала даёт wait_condition без HTTP; в предаварийном snapshot
  очередной claim уже leased, attempts=0, next=3. После SIGKILL/restart binding,
  bytes и IDs прежние; при новой цене 101 разрешается отправка. Проверены
  последовательные 429, неоднозначный disconnect и success message_id=88.
- **Notifications pending report.** После success API уже delivered, но ответ
  Result RPC задержан после commit: success=1, attempts=3, pending_reports=1.
  SIGKILL/restart уменьшает pending_reports до 0 при прежних attempt IDs,
  claims и outcomes; HTTP не повторяется. IDs сохранены в
  [выдержке фактических наблюдений](observations.txt). Полный пакет notifications
  PASS, `TestNotificationsProcess` 161.09 s; прежние source/shutdown guards сохранены.
- **Цена и отмена через четыре процесса.** Готовый `TestUserJourney` PASS,
  33.74 s: 101 → 99 до допуска паркует тот же job без HTTP и без освобождения
  слота; 102 доставляет тот же workflow. Третья pending подписка отменена до
  допуска: реальный intake/повторный claim, attempts=0, success=0, HTTP нет,
  слот свободен; всего две подтверждённые доставки. SQL feed не подменяется.

## Границы времени и cleanup

Compose test: общий context 7 min, Go test timeout 8 min; фазы по 140 s,
ожидание offline quote 25 s, Kafka stop command 25 s, startup health storage
150 s, ожидание drain 70 s, каждый SQL observer statement 2 s / command 12 s,
ожидание выхода fake 15 s. Фазы завершаются по наблюдаемым состояниям.
Process runners: Go package timeout matcher 150 s, notifications/E2E 300 s;
дополнительный subprocess предел 180/330 s, context и startup/kill/join bounds
существующих fixtures сохранены. Sleep только регулирует частоту опроса.

Matcher, notifications и E2E runners подтвердили удаление PID registry,
temporary credentials, containers, volumes и networks точных проектов
`binance-process-{pg,kafka}-1f665b3b34f648a2`,
`binance-process-{pg,kafka}-273faeeca1ef4fcb`,
`binance-process-{pg,kafka}-bf2adc3d87bc4cf5` соответственно.
Рабочие volumes/проекты не адресовались. Docker build cache и образы сохраняются.

## Неуспешные проверки и ограничения

- Первый Compose запуск: Kafka offline/drain/inventory PASS, затем ненужный
  этому профилю полный stop/start не завершился в 140 s ожидания Go fixture:
  `host restart deadline exceeded`, TestCompose FAIL (216.81 s). Во время
  первого запуска добавлялась диагностика process-тестов, поэтому последующий
  start пересобирал образы; точная доля build/start в задержке не измерена.
  Контроллер прерван SIGINT (exit 130), finally cleanup PASS; дополнительно
  labels `binance-ops074-4e75ce77` дали пустые containers/volumes/networks.
  Исправлена композиция smoke: Kafka-профиль заканчивается второй доставкой
  после возврата брокера, обычный OPS-074 stop/start сохранён. Deadline не увеличен.
- Полный matcher runner: следующий `TestMatcherSourceProcess` упал до readiness
  первого запуска, `source_process_test.go:90`, exit status 1 (19.76 s).
  Это **не** успешная проверка source suite. Причина неизвестна, отдельный
  [OPS-080](../../backlog/tickets/OPS-080.md) поставлен перед E2E-006;
  startup/identity guards не ослаблялись, исправление здесь не выполнялось.
- Первое обращение к Docker в sandbox отказало в доступе; реальные запуски
  выполнены с разрешённым доступом. Build exit 0 с sandbox warning записи
  module stat cache; предупреждение не подменяет успешную сборку.
- Внешние Binance/Telegram, публичный хост, webhook и ACME не проверялись.
  После неоднозначного внешнего timeout возможен дубль по DEC-004; повтор
  проверяет текущую цену. Backfill пропущенных Binance сделок не обещается.
  Production ready до E2E-006 не заявляется.

## Обычные проверки

С `GOCACHE=/tmp/binance-go-build`, `GOTOOLCHAIN=local`: `go test -race ./...`,
`go vet ./...`, `go build ./...` — exit 0. Opt-in SQL/process проверки в обычном
suite SKIP; они запущены выше отдельными runners. После уточнения Kafka-only
композиции повторён только затронутый `go test -race ./infra/deploy/smoke`
(компиляция, opt-in smoke SKIP) и настоящий Compose smoke.
`python3 scripts/generate_proto.py --check` — exit 0, generated code current.
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s infra/deploy -p test_observe.py -v`
— 4 tests PASS, включая финальное состояние helper. `gofmt -l .` пуст,
Python AST, локальные Markdown links, `git diff --check`, backlog check — PASS.
Новый full suite/нагрузочный повтор после успешных проверок не выполнялся.
