# Фиксация исхода доставки — SUB-020

`proto/api/v1/result.proto`: unary `/api.v1.DeliveryResults/SaveDeliveryResult`.
`NewPostgres(pool)` вызывает готовую атомарную `subscription.SaveDeliveryResult`
(SUB-019), `NewServer(store)` регистрируется через сгенерированный контракт.
`NewClient(conn)` использует принадлежащее вызывающему mTLS-соединение.
Бюджет клиента и сервера — 3 с; более ранний deadline и отмена передаются в SQL.
Это начальный предел операции, не измеренный SLA. Автоматических повторов нет.

Все `ServerOptions` нужно установить вместе с регистрацией сервиса: TLS 1.3,
сервисный CA и право только точного URI notifications. Matcher, API и публичный
Bearer этого права не дают. Для нескольких API RPC на одном сервере runtime
должен собрать общую политику методов serviceauth; отдельные unary interceptor
нельзя независимо накладывать друг на друга.

Запрос содержит owner/subscription/workflow/attempt UUID и одну наблюдаемую
категорию результата. API проверяет форму до SQL, storage — принадлежность
подписки владельцу и известность attempt именно в этом workflow:

| outcome | message_id | block_reason |
| --- | --- | --- |
| succeeded | положительный Telegram message_id | пусто |
| retryable / uncertain | 0 | пусто |
| blocked | 0 | channel_unavailable / attempts_exhausted / permanent_failure |

Запрос не принимает fence, chat, версию канала или локальный lease. Эти данные
принадлежат сохранённому permit. Подтверждённый успех передаётся storage и после
expiry/нового допуска/отмены. Storage атомарно сохраняет аудит, канонический
результат, слот и SubscriptionChanged/C. Отменённая подписка остаётся cancelled,
поздний успех аудируется. Первый успешный message_id остаётся каноническим.

Успешный RPC подтверждает durable запись наблюдения и возвращает **текущее**
каноническое состояние/версию/workflow и при наличии причину/версию блокировки.
Это не исторический ответ admission и не разрешение отправить сообщение.
Повтор той же пары attempt/outcome и тех же данных не создаёт новый переход;
между повторами состояние может измениться другой операцией.

Ошибки: неверная форма — InvalidArgument; отсутствующая/чужая подписка —
NotFound; неизвестный attempt или чужой workflow — FailedPrecondition;
противоречащий сохранённому message_id/причине повтор — AlreadyExists;
SQL/недоступная зависимость — Unavailable. DeadlineExceeded и Canceled
сохраняются, внутренние SQL-тексты не выдаются.

Timeout/Unavailable не доказывает отсутствие commit. Notifications сохраняет
исход и повторяет **тот же результат с тем же attempt**, пока не получит
подтверждение; ошибка этого RPC сама по себе не требует нового sendMessage.
При конфликте результата или неверной идентичности нужно разбирать сохранённое задание,
а не создавать новую попытку. Хранилище обязанности notifications — NTF-014.

Проверки: `go test -race ./internal/api/resultrpc -count=1` (нужен loopback);
`python3 scripts/check_postgres.py` — реальный disposable PostgreSQL, mTLS,
конкурентные повторы, поздний успех и timeout после реального commit.
Runtime, Telegram worker и reconciliation в этот пакет не входят.
