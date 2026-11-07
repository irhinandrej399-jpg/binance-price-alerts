# Проверяемое поколение Kafka-источника (OPS-060)

`Identity{ClusterID string, TopicID [16]byte, Topic string}` — один общий
контракт changefeed и pricefeed. Partition и абсолютный next offset задаются
отдельно в `Position`. Нулевой TopicID, пустой ClusterID/topic недопустимы;
нулевой Config.Source не отключает проверку. Имена/адреса broker, consumer group,
replica/member ID, SQL token, ProjectionID и snapshot_id не заменяют identity.

- `Metadata(ctx, client, topic)` читает broker ClusterID и TopicID без
  auto-create, с общим пределом 3 с и отменой. Возвращает также partitions,
  чьи error codes проверены. Это discovery для provisioning **нового** источника.
- `identity.Verify(ctx, client)` сверяет явно ожидаемую identity с новым ответом
  metadata. Ошибка/пустой ответ/неподдерживаемые IDs дают отказ.
- `identity.CheckFetches(fetches)` обходит исходные broker responses, сверяет
  имя и ненулевой TopicID **до** Records/декодирования любых записей, включая
  control records. Даже второй broker response не может затеряться в merge.
  Пустой synthetic fetch от истечения PollRecords wait не является broker metadata.
- Публичные ошибки для `errors.Is`: `ErrIdentity` (нет доказательства),
  `ErrMismatch` (другое поколение/имя), `ErrMetadata` (проверка недоступна).
  Сетевые ошибки не включают сырые broker messages/адреса; отмена/deadline
  сохраняются в цепочке, насколько их сохраняет контекст вызывающего адаптера.

Ожидаемые IDs получает оператор при provisioning нового источника из доверенного
Kafka control plane либо `Metadata` через настроенное доверенное соединение.
При resume они приходят из durable binding **той же позиции**, а конфигурация
содержит разрешённый источник; эти значения должны совпасть. Нельзя прочитать
нынешнюю metadata и присвоить её старому SQL offset/Pending. Исторические данные
без binding остаются unbound. Нужны SQL-контракт OPS-061 и runtime OPS-062.
Транспорт проверяет broker metadata и fetch; он не удостоверяет SQL lineage.

Граница доверия — broker и соединение с ним. Plaintext disposable Kafka не
защищает от подмены broker. Клонирование storage с сохранением обоих IDs этим
контрактом не различимо; перенос/смена источника и автоматический reset не
реализованы. Нужны Kafka с TopicID в metadata/fetch; downgrade до нулевого ID
не принимается. Повторный старт того же topic допускается с прежними IDs.

Оба адаптера дополнительно проверяют metadata периодически (в том числе при
пустом потоке), на регистрации и вокруг получения позиций. До каждой выдачи
новой broker записи проверяется fetch ID. При обнаруженном отказе отменяются
reader/registrations, очищается очередь; Next/Confirm не продолжают работу.
Остаются прежние bounded buffers, read_committed, NoResetOffset и Close/join.
Цена проверки metadata и профиль памяти/нагрузки не квалифицированы.

Проверки:

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./internal/platform/kafkasource ./internal/matcher/changefeed ./internal/matcher/pricefeed
GOCACHE=/tmp/binance-go-build bash infra/kafka/verify-go.sh
```

Runner проверяет настоящий restart и delete/recreate только уникальных topics,
старые readers/resume и совпадающие offsets. Узкая wire fixture changefeed через
Config.Dialer/kmsg меняет metadata между регистрацией и первым fetch и проверяет
отказ **до accept**, отдельно для data и commit marker. Проверки пустой/error/
нулевой metadata и отмены не требуют внешнего broker. SQL и process recovery
в этом тикете не проверяются.
