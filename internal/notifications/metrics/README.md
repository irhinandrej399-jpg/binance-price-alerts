# Метрики доставки — OPS-003

`jobs.Store.QueueMetrics(ctx)` читает notifications DB в read-only
repeatable-read snapshot. `metrics.New(store)` реализует `http.Handler`
с Prometheus text 0.0.4. При SQL/cancellation/commit ошибке — HTTP 503 без
частичных метрик, старых значений и текста ошибки. Runtime должен задать
ограниченный context/HTTP timeout. Коллектор не меняет задания и не делает RPC.

| Метрика | Смысл |
| --- | --- |
| `delivery_backlog` | Незавершённые jobs, включая ожидания, leases и blocked |
| `delivery_oldest_seconds` | Возраст самого старого незавершённого job от created_at; 0 при пустой очереди |
| `delivery_jobs{state}` / `delivery_state_oldest_seconds{state}` | Число/возраст по effective state |
| `delivery_blocks{reason}` | Текущие блокировки workflow, включая historical block текущего fence |
| `delivery_pending_reports{kind}` | Четыре вида pending API reports, включая terminal/stopped jobs |
| `delivery_outcomes{kind}` | Durable evidence: success, uncertain, rate_limited (429), server, channel_unavailable, permanent_failure, attempts_exhausted |
| `delivery_possible_duplicates` | Число уникальных uncertain attempts, как jobs.UncertainAttempts; не доказанные внешние дубли |
| `delivery_completed` | Число уникальных workflows с API-acknowledged success report |
| `delivery_admission_rpc_seconds{result}` | Histogram длительности admission RPC: response/error, включая replay |

Все SQL-показатели — **database-wide gauges**. Повторы Save*/ack и restart
не увеличивают их. Они живут столько же, сколько underlying evidence;
retention/reset может уменьшить totals. Не складывать одинаковые значения от
реплик! Выбрать один scrape target на notifications DB либо дедуплицировать
копии по меткам target средствами мониторинга. Histogram локален процессу:
агрегируются rates bucket/sum/count всех процессов; после рестарта сбрасывается.

Effective state: наличие success любого attempt имеет приоритет над stale
job state; затем block текущего fence для незавершённых состояний; затем
сохранённый state. Старые blocks прежнего fence не блокируют новый цикл.
Cancelled/stopped/delivered и succeeded не входят в backlog; неизвестный state
попадает в один `other` и консервативно входит в backlog. Возраст — SQL clock
минус created_at, ограниченный снизу нулём; отсрочка retry/limiter/recovery его
не сбрасывает. Success исключает job из очереди ещё до API ack; оставшаяся
обязанность видна в pending reports. Исторические исходы пересекаются: uncertain
может позднее получить success. Нельзя суммировать их как независимые attempts.

`delivery_completed` подтверждает durable Telegram success и получение API
этого свидетельства по готовому reporter-протоколу. До ack значение не растёт,
в том числе после потери ответа API. Несколько success attempts одного workflow
считаются один раз. Это локальное подтверждение доставки, не чтение канонической
API-таблицы: поздний success отменённой подписки остаётся аудитом API.
`DrainRate(previous, current)` даёт число новых подтверждений в секунду;
требует одну БД, возрастающее время и неубывающий completed. Для Prometheus
можно использовать `clamp_min(deriv(delivery_completed[1m]), 0)` одного DB target,
исключив окна retention/reset. Уменьшение backlog не заменяет throughput:
на него одновременно влияют новые jobs и отмены.

В runtime обернуть admission client перед передачей в handler.Config:

```go
observability := metrics.New(store)
cfg.Admission = observability.Admission(admissionClient)
mux.Handle("/metrics", observability)
```

Decorator передаёт исходные context, аргументы, response и error. Он измеряет
монотонную длительность каждого `AdmitDelivery`, включая transport, ожидание API,
транзакцию и чтение последней цены. Это время операции допуска с проверкой цены,
а не отдельный таймер чистого market-data чтения. Replay может вернуть прежний
результат без нового чтения цены, но остаётся отдельным RPC sample.
PrepareRecipient не проверяет цену и в histogram не входит. Фиксированные
cumulative buckets (секунды): .001, .005, .01, .05, .1, .5, 1, 5, +Inf.
Result означает RPC response/error, не продуктовый исход допуска.

Labels — только фиксированные state/kind/reason/result/le. Нет user/chat/bot,
workflow/attempt/request ID, пары, URL или текста ошибок. SQL snapshot не хранит
ID; постоянные отказы без HTTP видны через blocks даже без attempt evidence.

Проверки:

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./internal/notifications/metrics
GOCACHE=/tmp/binance-go-build python3 scripts/check_postgres.py
```

Проверяются контрольные SQL jobs, waits/429/replay/restart, поздние block/success,
ack и сохраняемая uncertainty, фиксированные labels, HTTP 503, конкурентные
наблюдения, точные buckets/sum/count и скорость дренирования. Production runtime,
Prometheus deployment, реальный Telegram и производительность SQL scan этим
тикетом не проверяются; сборка cmd/runtime ещё предстоит.
