# Фазы API (OPS-024)

Один живущий compiled Go test executable используется как инфраструктурный driver
для готовых `testing.TB` helpers `grpctest` и `httptest`. Это не functional suite:
обычный go test пропускает driver; только JSON-lines adapter включает opt-in.
Новые продуктовые endpoints, SQL-переходы, лимиты и общий harness не добавляются.

Из корня, последовательно, каждый output отсутствует:

```sh
GOCACHE=/tmp/binance-ops024-build python3 scripts/measure_joint.py --output docs/measurements/OPS-024/run-a2 --manifest infra/measure/api.json --timeout 120 --command python3 infra/measure/api_adapter.py docs/measurements/OPS-024/run-a2/identities.json
GOCACHE=/tmp/binance-ops024-build python3 scripts/measure_joint.py --output docs/measurements/OPS-024/run-b2 --manifest infra/measure/api.json --timeout 120 --command python3 infra/measure/api_adapter.py docs/measurements/OPS-024/run-b2/identities.json
python3 scripts/verify_api_measurement.py docs/measurements/OPS-024/run-a2 docs/measurements/OPS-024/run-b2
```

`api.json` — immutable входной manifest v1 с размерами fixture и coverage.
Обязательный runtime-контракт ID случайных UUID не подменяется фиксированными ID:
`identities.json` экспортирует реальные идентичности, ответы каждой фазы, исходные
journal/outbox bytes (hex), admission response и read_committed records.
Verifier создаёт `resolved-manifest.json` с SHA256 входного manifest/evidence,
position → phase/event/subscription/trigger/attempt и размерами wire payload.
Пустые links в шаблоне означают позднее связывание в resolved manifest, не отсутствие
бизнес-связей. OPS-025/026 используют исходные bytes и ID из одного выбранного run,
не смешивают продолжения разных чистых прогонов. Состояние candidate price
имеет source/stream `OPS-024-api-v1`, generation=1, sequence=1 для candidate/создания, 2 для false, 3 для admission; trade/received time фиксированы в setup. Времена реального
опыта сохраняются в bytes. Живые Docker volumes после проверки удаляются.

## Размеры и границы

Все числа ниже описывают fixture, а не квалифицированные максимумы. `size` — число
бизнес-вызовов; `max_batch=1` у фаз — размер SQL-пакета **этого fixture**. У drain
size — число успешно опубликованных событий; есть ещё один idle Step. У journal
и subscription relay нет конечного продуктового max batch: `limits` хранит null,
это блокирует квалификацию. kgo MaxBufferedRecords=1000 не ограничивает целый
SQL-пакет: relay обязан читать и отправлять полный пакет.

| Фазы | Вход fixture / ограничения готового контракта | Вызовов / batch |
| --- | --- | --- |
| empty | Нет операции | 0 / 1 |
| create | Тело 49 UTF-8 B, BTCUSDT 7 B, price=100 3 B, up 2 B; один объект HTTP. Каталог из одной пары, котировка 101. На owner ≤15 незавершённых. Общий предел body отсутствует; percent lexical text и pair не имеют согласованного конечного предела | 5 / 1 |
| workflow, workflow_replay | owner/subscription — UUID по 36 ASCII B, expected_version=1; latest RPC читает 101. Первый атомарный workflow пишет одну subscription, один AlertTriggered, один journal. Replay использует исходную version, не создаёт новый workflow | 4 / 1 |
| cancel, cancel_replay | Один UUID в DELETE path, Bearer-сессия setup. Отмена active без workflow, повтор с тем же ID. Создаёт один journal и освобождает один слот только в первый раз | 1 / 1 |
| prepare | owner/subscription/workflow — 3×36 B; без записи/цены. Получает chat=24001, binding_version=1 | 4 / 1 |
| admission_wait | Те же 3 UUID, request_id=wait 4 B, CAS chat/version; latest=99. Сохраняет ответ и новую версию active, без attempt | 1 / 1 |
| admission, admission_replay | 3 UUID, request_id=permit-N 8 B, CAS chat=24001/version=1, latest=101. TTL=60000000 мкс. Допустимый TTL [1,60000000] мкс, request_id ≤200 B. Replay исторический, HTTP не выполняется | 4 / 1 |
| succeeded | 4 UUID (owner/subscription/workflow/attempt), outcome=succeeded, message_id=24001. Положительный signed int64; первый success освобождает слот | 1 / 1 |
| retryable, uncertain | 4 UUID, соответствующая enum, message_id=0, без reason; отдельные subscription и attempt | 1 / 1 |
| blocked | 4 UUID, enum blocked, permanent_failure 17 B; допустимы также channel_unavailable/attempts_exhausted, message_id=0 | 1 / 1 |
| results_replay | Те же четыре исхода/ID/данные без нового перехода | 4 / 1 |
| subscription_first/lost/retry | Готовый relay Step: claim/read целого пакета, новая producer epoch, publish, fenced mark только при известном commit; второй journal event в lost и retry. Значения точных bytes в resolved manifest | 1 / нет конечного контрактного batch (fixture=1) |
| subscription_drain | Оставшиеся 17 из 19 journal событий и один idle Step | 17 / fixture=1 |
| delivery_first/lost/retry | Готовый deliveryrelay Step, один AlertTriggered; lost/retry выбирают тот же outbox ID. SQL не задаёт UUID как commit-порядок; какой ID выбран, устанавливают records/дубль | 1 / 1 |
| delivery_drain | Оставшиеся два из четырёх outbox событий и один idle Step | 2 / 1 |

UUID генерирует готовый API, не driver. Цена numeric — максимум 18 целых/18 дробных
цифр, без округления; версии и journal C — положительный signed bigint.
Размеры envelope/payload после генерации (UUID, версия, времена, JSON escaping)
экспортируются точно, а не оцениваются длиной входа. Pair/source/stream ID в trusted
интерфейсах не имеют общего конечного предела; ASCII fixture не покрывает TOAST,
длинный percent/pair, максимальные numeric/версии, большие batches, историю retry,
late success/cancel с workflow, channel recovery и все варианты blocked. Эти классы
не объявляются бесплатными или покрытыми: OPS-018 обязан блокировать квалификацию.
Предел read journal=1000 **транзакций**, не число строк/bytes.

## Границы опыта

Setup: миграции четырёх владельцев runner, account, канал, сессия, каталог/quote
fixtures, HTTP/TLS соединения и инициализация stable producer IDs. Только runtime
DSN API используется driver; SELECT current_user проверяет api_runtime. Чужой SQL
снимают только collectors; market-data reader — локальный fixture за штатным
mTLS Latest. Quote/каталог не обращаются к Binance. Telegram не вызывается:
результаты являются заданным входом API от synthetic notifications identity.

Каждая фаза включает только вызовы указанного класса, ответы сохраняются в памяти.
Сверка собственного SQL, read_committed чтение, экспорт hex и запись evidence
выполняются в verification после последних after/checkpoint. Control empty
не вычитается. В стоимость каждого relay Step входят claim/read/новая epoch/
Publish/SQL-mark/условное освобождение lease; claims/marks не заменяются ручным SQL.
Два EndTxn fault используют существующий kafkafault; брокерный code=0, клиентский
ErrCommitUnknown и повторные bytes на разных offsets проверяются независимо.

Опыты квалифицируют только полноту fixture/report. `qualified=false`: unsupported
PG temp peak/device IO, Kafka fsync, отсутствие конечных bounds, максимальных
размеров и downstream continuations остаются блокерами OPS-018. Схемы/версии/
образы/source hashes/commands и operation/background/checkpoint deltas сохраняет
OPS-023. Полный функциональный/race suite не запускается параллельно измерениям.
