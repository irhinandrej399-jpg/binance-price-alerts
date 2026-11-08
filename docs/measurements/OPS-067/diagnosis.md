# OPS-067: metadata после CreateTopics

8 ноября 2026. Область: подготовка уникального topic в
`internal/matcher/changefeed/identity_integration_test.go`.

## Подтверждённый дефект и граница доказательства

Fixture считал успешный CreateTopics достаточным основанием для единственного
немедленного Metadata. Это неверное условие готовности: Kafka допускает задержку
распространения metadata после успешного create/delete. Это прямо описано в
[официальном Admin API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/Admin.html#createTopics(java.util.Collection,org.apache.kafka.clients.admin.CreateTopicsOptions)).

В существующей `generationWire` воспроизведена допустимая последовательность:
CreateTopics v7 success → Metadata с topic error 3
(UNKNOWN_TOPIC_OR_PARTITION), без partitions → следующий Metadata с новым
ненулевым TopicID, partition 0, leader 0 в ISR, error 0.
`TestCreatedTopicMetadataMayBeUnavailable` подтверждает: прежний одношаговый
Metadata возвращает ровно нулевую Identity и ErrMetadata, последующее discovery
готового нового topic успешно. Ошибка возникает до создания нового reader;
это не отказ source identity существующего consumer.

**Исторический ответ OPS-065 не установлен.** В исходном сообщении сохранён
только ErrMetadata: он объединяет несколько видов некорректных ответов.
Нельзя утверждать, что тогда брокер вернул именно error 3. Реальные короткие
прогоны в этой сессии не воспроизвели исходное падение. Доказан дефект условия
готовности fixture и его воспроизведение на управляемой wire-границе, а не
конкретное внутреннее расписание Kafka 8 ноября в OPS-065.

## Red → Green

Команда без внешнего брокера:

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -count=1 -run '^TestNewGeneration' ./internal/matcher/changefeed
```

Red с одношаговым discovery: missing → ErrMetadata; zero → ErrIdentity;
partition error → ErrMetadata; old ID, отсутствие leader/ISR принимались
преждевременно; постоянный old ID возвращался вместо истечения deadline.
Все эти случаи упали по проверяемой причине. Green с race прошёл.

Исправление только в тестах: `newGeneration` опрашивает Metadata с интервалом
20 ms внутри общего контекста 5 s. Интервал ограничивает частоту запросов;
доказательство готовности — ответ: новый ненулевой ID, прежний ClusterID при
replacement, ровно partition 0 без ошибки, известный leader в ISR. При deadline
возвращаются нулевая Identity и ошибка контекста. Initial create тоже ждёт
готовность. Уникальное имя позволяет короткую повторную диагностику без
конфликта с оставшимся topic предыдущего вызова теста.

Проверки старого reader после replacement, Next, Confirm, старого resume,
relabel и unbound checkpoint сохранены без retry. Production Go/SQL не менялись.

## Реальные проверки

- Диагностические disposable-прогоны: первый исходный сценарий PASS; ещё девять
  повторов того же имени ошиблись CreateTopics/TOPIC_ALREADY_EXISTS (36), что
  не является исходным дефектом. Затем 10 и 30 последовательных запусков с
  уникальными именами, а также 10 конкурентных сценариев — PASS. Это короткая
  диагностика, не квалификация стабильности или нагрузки. Временные printf и
  конкурентный диагностический тест удалены; в продуктовый код не входят.
- `GOCACHE=/tmp/binance-go-build bash infra/kafka/verify-go.sh`: PASS с race,
  включая настоящий delete/recreate и source guards changefeed/pricefeed.
  Пять negative opt-in на закрытом порту ожидаемо FAIL, runner принимает только
  их отказ. EXIT cleanup удалил disposable ресурсы.
- `GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./...`: PASS;
  реальные opt-in вне runner пропущены. После дополнения wire-проверки create
  выполнен целевой race changefeed повторно — PASS.
- `go vet ./...`, `go build ./...` с теми же env: exit 0.
  Build сообщил предупреждение sandbox о записи stat cache, но завершился
  успешно; полноценная компиляция также выполнена race-прогоном.
- `python3 scripts/generate_proto.py --check`: PASS с доступом к сети;
  исходный запуск в sandbox не смог разрешить DNS, это не считалось успехом.

Рабочие Kafka/PostgreSQL, внешние Binance/Telegram и deployment не проверялись
и не менялись. Проверка не обещает отсутствия любых будущих сбоев брокера.
