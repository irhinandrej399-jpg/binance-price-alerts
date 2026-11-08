# SUB-024: SQL/RPC expiry и часы

Дата: 8 ноября 2026. Результат: доказанное ограничение окружения с проверкой
предусловий; исправления production-политики срока не потребовалось.

## Что доказано

SQL `buildSnapshotOn` задаёт срок через `clock_timestamp()` и проверяет его перед
commit. Повтор BuildRequest и Page проверяют snapshot/pin по SQL-часам.
`snapshotrpc.Server.page` после успешного Page сравнивает тот же абсолютный срок
с `time.Now()` Go. Разные часовые домены могут дать успешный SQL и
`FailedPrecondition` RPC сразу после создания даже при TTL 60 s.

`TestPostgresSnapshotClockDomains` выполняет настоящие BuildRequest/Page и
сдвигает только пересекающий границу Store/RPC expires_at на −2 min, моделируя
отставание SQL VM. Сдвига системных часов, SQL-строк и ослабления guards нет.
RPC сдвинутой страницы даёт FailedPrecondition, а повтор реальных SQL BuildRequest
и Page с тем же ключом успешен. При offset=0 RPC успешен. Источник отказа в
управляемом случае локализован в RPC expires_at, а не в SQL BuildRequest/Page.
Проверка предусловий различает оба случая.

Для SQL-замера внутри Go-интервала [before, after] возможное смещение SQL−Go
лежит в [sql−after, sql−before]. Fixture требует, чтобы весь интервал находился
в ±50 ms (половина прежнего короткого TTL 100 ms). Слишком медленный замер или
скачок часов также не позволяют подтвердить предусловие и дают явный FAIL.
Сэмплы снимаются в начале и cleanup теста. Между Build/Page добавлены только
журнальные строки: диагностические SQL round trips не расходуют короткий TTL.

## Границы вывода

В обычных прогонах этой сессии старый сбой не повторился. Часы SQL и Go сейчас
согласованы. В историческом отказе 29 октября времена и стадии не сохранены,
поэтому его точную ветку и связь с Docker clock drift доказать нельзя.
Рассинхронизация как историческая причина остаётся гипотезой. Закрытие относится
к предусмотренному карточкой результату «доказанное ограничение окружения с
воспроизводимой проверкой предусловий», а не к заявлению об исправленном
production-дефекте. Будущий отказ оставит SQL outcome, Go-время, expires_at и
начальный/конечный SQL/Go-интервал. Реальное истечение внутри SQL остаётся ошибкой.

## Проверки

- Первоначальный целевой disposable SQL/race: PASS; исходные recovery/cancel
  сценарии успешны, SQL/Go времена согласованы, volume/network/container удалены.
- Red: `go test -run '^TestSnapshotClockPrecondition$' ./internal/api/snapshotrpc`
  — exit 1: отсутствуют snapshotClockError/requireSnapshotClocks.
- Green: `go test -race -run '^TestSnapshotClockPrecondition$' ./internal/api/snapshotrpc`
  — PASS. Управляемые ±2 min отклоняются; согласованные часы принимаются.
  Добавлены проверки слишком широкого интервала и обратного хода host clock.
- `python3 scripts/check_postgres.py --snapshot-only` — целевые SQL/race PASS:
  snapshot/pin storage, commit recovery, expiry/rollback, retention, TLS recovery,
  настоящий expiry 100 ms, cancelled SQL-lock и clock-domain injection.
- `go test -race ./...`, `go vet ./...`, `go build ./...`,
  `python3 scripts/generate_proto.py --check` — exit 0. SQL/Kafka opt-in в обычном
  race suite пропускаются штатно; SQL проверен отдельной командой выше.
- Python tooling regression: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -v`
  — 35 tests, OK, 7 opt-in migration checks SKIP.
- Первые sandbox-запуски Docker/cache/Protobuf были недоступны по правам/сети;
  те же проверки затем выполнены с разрешённым доступом. Ограничения песочницы
  не считались поведенческим Red или успешной проверкой.

Окончательная безопасная выборка журнала — [snapshot.txt](snapshot.txt).
Паролей/DSN в отчёте нет. Рабочая БД не адресовалась. Нагрузки и повторов полного
SQL suite не было; независимая binding-проверка NTF-020 не включалась в scope.
