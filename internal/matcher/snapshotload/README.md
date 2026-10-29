# Загрузка снимка matcher — MAT-007

`New(matcherPool, snapshotrpc.NewClient(mTLSConn)).Load(ctx, request)` читает
BuildSnapshot и все ReadSnapshotPage, сохраняя исходные opaque cursors. Caller
сохраняет устойчивые request ID/TTL **до** первого вызова. Конечный бюджет каждого
RPC обеспечивает SUB-016; context ограничивает всю загрузку и SQL.

Миграция matcher `002_snapshot_generations.sql` создаёт отдельные staging
поколения, ключ — исходный snapshot_id. Каждая страница и её durable счётчик
фиксируются одной транзакцией. Полный protobuf after-image сохраняет все поля,
точные decimal-строки, исходные timestamps/optional percent, workflow и blocked.
C хранится как uint64 без преобразования к float/int64. Текст expiry сохраняет
наносекунды, которые потерял бы один PostgreSQL timestamptz.

Загрузчик проверяет постоянные snapshot ID/C/expiry, срок, порядок ID, ошибки
контракта, дубли строк и циклы cursors. Последняя страница атомарно выставляет
`Generation.Complete`; это означает только завершение снимка. `Info` показывает
диагностический durable прогресс; `Get` возвращает after-image только полного
поколения. Ни один из этих методов не разрешает matcher работать или отправлять
кандидатов. Рабочие `matcher_checkpoints`/`matcher_subscriptions` не меняются.

После ошибки Load возвращает ошибку и нулевой результат, не частичное поколение.
При Unavailable/timeout повторяют **те же request ID/TTL**, начиная с BuildSnapshot,
в том числе через новый Loader/pool. Одинаковые страницы идемпотентны, иной размер
страницы допустим. Сохранённого restart cursor нет. Конкурентные повторы
сериализуются строкой поколения; несовпадающие данные отвергаются, уже сохранённые
строки не перезаписываются. На страницу нет полного пересчёта всех строк снимка.

FailedPrecondition RPC означает потерю/истечение ресурса: частичное поколение
логически отбрасывается, нужен новый request ID. Локальный некорректный/истёкший
ответ даёт ErrInvalidSnapshot. Остальные ошибки RPC/SQL/context передаются caller.
Старые staging строки остаются изолированными для последующей очистки; runtime
не имеет DELETE. Полное поколение тоже не доказывает действующий replay-pin:
MAT-008 должен выполнить handoff/replay после C, подключить последние цены и
только затем переключить рабочее поколение/readiness. Исторические цены здесь
не читаются, workflow не создаются. Нагрузочные гарантии не измерялись.

Проверки: `go test -race ./internal/matcher/snapshotload` проверяет валидацию,
SQL-сценарии без POSTGRES_TEST пропускаются. `python3 scripts/check_postgres.py`
запускает disposable PostgreSQL и полный Go-набор с race detector. Проверяются
две страницы/все незавершённые состояния, пустой снимок, обрыв и повтор с другого
pool, конкурентные повторы, полные after-images, независимость live checkpoint,
SQL-откат после записи строк, нарушение ID/C/expiry/порядка/cursor и отмена.
Loopback-проверка использует настоящий SUB-016 Client и готовый grpctest harness;
права mTLS проверяет пакет snapshotrpc. Рабочая БД не изменяется.
