# Durable приём market-data

`New(*pgxpool.Pool)` использует только собственную БД `market_data` после отдельного
применения `migrations/market_data`. Схема не создаётся runtime.

1. `BeginGeneration(ctx, pair)` перед каждым новым соединением выдаёт durable
   поколение и отсекает старого ingest worker. Первый поток — unconfirmed без
   известного разрыва; повторный запуск/соединение — unconfirmed с разрывом.
2. `SetStatus(ctx, pair, generation, state, gap)` сохраняет подтверждение/разрыв с
   проверкой поколения. Разрыв исторический и не очищается значением false.
   Подтверждение live — обязанность будущего runtime, отдельная сделка его не задаёт.
3. `Accept(ctx, generation, observation, envelope)` блокирует строку пары, вызывает
   `trade.CheckNext` на последней **committed** котировке и назначает ревизию.
   Поля ревизии в observation — валидные предварительные метаданные декодера;
   адаптер заменяет generation/sequence и состояние потока durable значениями.
   Источник — только raw trade (`trade.StreamID`).
   Успех `true` возвращается только после synchronous commit котировки и outbox.
   Повтор/старая сделка возвращает `false, nil` без записи; ошибка — `false, err`.
   При неопределённом результате commit повтор той же source ID безопасен:
   повторять нужно исходную сделку, не считать ошибку подтверждением приёма.
4. `Latest` читает последнюю котировку с текущим durable состоянием потока;
   её ревизия и исходные времена не меняются. Нет цены — `ErrNotFound`.
5. `Claim(ctx, lease)` выдаёт неизменные байты полного PriceObserved и event_id,
   плюс новый fencing token. Время lease считается PostgreSQL, не часами worker.
   Самая ранняя неопубликованная работа блокирует последующие работы своей пары;
   другие пары могут обрабатываться независимо. Нет доступной работы — ErrNotFound.
6. После подтверждённой брокером публикации будущий relay вызывает
   `MarkPublished(ctx, eventID, token)`. Истёкший/заменённый claim — ErrStale.
   Сбой после публикации до mark может дать дубль с прежним event_id.

Envelope (уникальный event_id, correlation_id и occurred_at) предоставляет caller.
JSON хранится как bytea: все повторы публикуют те же байты. Времена источника
сохраняются в JSON с наносекундной точностью, цена — точное десятичное значение.
Поколения, sequence и claim token ограничены положительным SQL bigint: при
исчерпании операция завершается ошибкой, wraparound запрещён. Sequence начинается
с 1 внутри нового поколения; source ID между поколениями не сбрасывается.

Миграция отзывает общие default privileges для outbox и выдаёт UPDATE только
служебных полей claim/mark. Runtime не может менять payload/event_id или удалять
работу. Mark не удаляет запись: retention/compaction здесь не реализованы.
`BeginGeneration` и статус используют серверный synchronous_commit=on из стенда;
Accept дополнительно устанавливает его локально. Ослаблять durable настройки
DEC-006 для этого pool нельзя.

Проверка: `python3 scripts/check_postgres.py` поднимает только disposable
`binance-go-check`, применяет миграции API и market_data и запускает общий Go race
прогон с раздельными DSN. Тест использует свежую БД; рабочий стенд не меняется.
WebSocket/runtime wiring остаётся последующим тикетам; bounded relay и Kafka producer готовы в MKT-014/MKT-013.

MKT-014 ограничивает сериализованный PriceObserved до 4 KiB: `MaxEventBytes`,
`ErrTooLarge`. Accept отклоняет превышение до commit цены/outbox. Claim не загружает
oversized legacy payload целиком: ошибка сохраняет запись неопубликованной.
Готовый serial relay описан в [relay](../relay/README.md); runtime ещё не подключён.
