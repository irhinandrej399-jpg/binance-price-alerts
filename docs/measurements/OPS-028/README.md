# OPS-028 — проверка API fixture

Fixture и точные команды: [инструкция](../../../infra/measure/NOTIFICATIONS_FIXTURE.md).
Это проверка инфраструктурного setup и настоящих RPC, без измерений стоимости.
Исходные OPS-024/025 артефакты не изменялись.

5 ноября 2026:

- Source SHA256: `60187b1c96bab46ddc9009a6853e43c0780099659470c60fdfd67a97fcd85106`.
- Связанный OPS-025/run-a3/identities.json SHA256:
  `749c6ec1f6dbb484193e5c63fecb5b0e53df2186723c34bf5b82e16ceb8dfc8c`.
- `verification-1`: сборка прервана с `could not import grpc/codes/status`.
  Отдельный повтор `go test -race ... -run '^TestNotificationFixtureSource$'`
  прошёл; SQL/RPC этот первый запуск не подтверждает. Cleanup подтверждён.
- `verification-2`: SQL import и rollback прошли; вызов исторического admission
  с явно переданным nil CAS отвергнут клиентом (`one expected recipient required`).
  В fixture исправлен вызов на отсутствие optional argument. Cleanup подтверждён.
- `verification-3`: полный итоговый проход, четыре отдельных чистых volume,
  по два для reports и handler. Все Go-проверки с `-race` прошли.
  Логи, JSON evidence и отдельный cleanup JSON лежат в этом каталоге.

| Dataset | jobs / source records | result rows / new journal rows | quota | Итог |
| --- | --- | --- | --- | --- |
| a-reports | 4 / 5 | 4 / 4 | 3 | PASS |
| b-reports | 4 / 5 | 4 / 4 | 3 | PASS |
| a-handler | 4 / 5 | 0 / 4 | 4 | PASS |
| b-handler | 4 / 5 | 0 / 4 | 4 | PASS |

Для reports проверены точные bytes response/outbox, исходные permissions,
истёкшие сроки replay, четыре результата с реальным commit/потерянным ответом,
восстановление через новый reporter/pool и отсутствие повторного перехода.
Для handler — PrepareRecipient/CAS и новые permissions по свежей quote.
Новые UUID двух handler-прогонов различны, исходные ID/bytes у всех сохранены.

Во всех случаях проверены отрицательные provenance/identity проверки,
неизвестная граница, ошибка импорта после первого INSERT с полным rollback,
отказ повторного импорта в занятый dataset, реальные RPC-отказы чужому owner/
workflow и отсутствие UPDATE прав на старые permits. Права не расширялись,
тестовые миграции не добавлялись. Cleanup проверяет отсутствие контейнеров и
volumes точного disposable project `binance-ops028-check`.

Бинарный код fixture — файлы того же локального коммита OPS-028; SHA находится
через `git log --all --grep='(OPS-028)'`. Профиль — функциональный PostgreSQL 18.6
в Docker, loopback mTLS, Go 1.26.3 darwin/arm64; не нагрузочный стенд DEC-008.
Физические измерения, квалификация стоимости, настоящий Kafka consumer и
HTTP handler/Telegram не проверялись. Для handler проверена необходимая исходная
API граница; полный worker/manifest продолжает OPS-026. Численных затрат нет.
