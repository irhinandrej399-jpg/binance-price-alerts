# Ограниченный relay PriceObserved

`Run(ctx, storage.New(pool), kafka.Config, topic, observe)` обслуживает **один
фиксированный publish-шард всех пар**. Все реплики используют один стабильный
transactional.id среды (`local-market-data-relay-shard-0` для local). Несколько
независимых шардов без SQL-фильтра/миграции не поддерживаются. В одном процессе
разрешён один Run; повторный вызов получает ErrRunning.

New producer получает Kafka epoch **до Claim**. Далее строго последовательно:
Claim → Publish исходных байтов → MarkPublished только после nil/commit Kafka.
Нет prefetch, накопительного канала или goroutine на запись. Долгая очередь —
существующий durable SQL-outbox, её принятие по-прежнему определяется commit
Accept, а не наличием свободного места в RAM relay.

Предел resident/in-flight — одна запись ≤4 KiB (строже верхнего предела 1 024).
Заполненность этого слота останавливает дальнейшие Claim. Accept явно возвращает
storage.ErrTooLarge до commit для события >4 KiB; producer также отклоняет его.
Claim ограничивает SQL-ответ 4 KiB, включая старые oversized записи: возвращает
ErrTooLarge, не загружая их целиком в Go и не подтверждая публикацию.

Kafka producer включает SingleRecord профиль helper: MaxBufferedRecords=1,
MaxBufferedBytes=8 KiB (payload + key), batch ≤16 KiB, produce request ≤64 KiB,
ответ брокера ≤1 MiB, compression отключена. Профиль рассчитан на закреплённый
локальный Kafka с одним broker и 16 неизменными партициями prices. Go heap,
metadata и сетевые буферы также потребляют память: реальный интеграционный тест
выводит sampled incremental live heap для 64 максимальных событий и проверяет
16 MiB. Это малая регрессионная проверка, не квалификация RSS/производительности
полного будущего market-data runtime; память SQL-пула и ingest надо учитывать
при его подключении.

Ошибка publish/mark закрывает producer, освобождает resident payload и включает
backoff 250 мс → максимум 5 с. Затем новый epoch и повторный Claim из SQL; DB-clock
lease 30 с, бюджет каждой операции 5 с. Истёкший/stale claim не подтверждается,
после его истечения SQL вернёт те же байты/event_id. Потеря commit ack или сбой
после Kafka commit до SQL mark допускают дубль. Принятые записи не удаляются и
не истекают по числу попыток. ErrFenced завершает Run, старый владелец не пытается
отобрать epoch обратно. Ошибки конфигурации/oversized требуют исправления.

Синхронный observer получает idle/claimed/committed/retry, число resident записей,
размер payload и ошибку. Он обязан возвращаться быстро и учитывать отмену;
его собственные очереди/копии не должны расти. Возврат Run также наблюдаем.
Отмена прерывает операции/backoff; неопубликованная работа остаётся в SQL.
Дисковый admission/ledger и остановка upstream при заполнении диска — OPS-016.

Проверки: `python3 scripts/check_postgres.py` — сначала relay на свежем outbox
(1 030 принятых записей, отказ, отмена/новый relay, unknown commit, stale SQL mark,
повтор исходных байтов, порядок и полное завершение), затем остальные пакеты;
`bash infra/kafka/verify-go.sh` — настоящий Kafka, fencing/отказ/lost ack и память.
`Run` ещё не подключён к cmd/market-data; live Binance не используется.
