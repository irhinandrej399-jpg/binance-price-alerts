# Работа реплики matcher — MAT-010

`New(Config)` и однократный `Run(ctx)` связывают готовые Kafka group, fencing,
полную live-view, priceconnection и pricehandler. Подключение в `cmd` отдельно.
Две реплики используют одну `Environment/Group/Prices.Topic`, разные устойчивые
`ID` и одну БД matcher. Одновременный запуск с одинаковым ID запрещён caller.

Caller передаёт pool matcher, Replay/Latest/Delivery clients, `Snapshot` для
загрузки полного immutable снимка и `Changes(ctx, position)` для нового источника
subscription-changes. При ненулевой позиции регистрация выполняется точно через
RegisterAt; источник/его Close принадлежат регистрации. RPC clients сохраняют
свои mTLS/deadline контракты. Для Snapshot callback projection ID — opaque ID
(`replica:<hash>`), **не** готовый BuildSnapshotRequest.RequestId: callback задаёт
допустимый стабильный request ID. Снимок и replay pin отдельные у каждой полной
проекции; нельзя делить один продвигаемый pin между независимыми writer.

`Bootstrap` — явная копируемая map из 16 абсолютных next offsets. Это позиции
согласованного caller latest-price bootstrap, а не broker commits и не
автоматическое правило «начать с нуля/конца». При наличии SQL Prefix он всегда
приоритетнее Bootstrap. Group проверяет границы log start/LSO; удалённая позиция
не сбрасывается. Первоначальная Latest оценивается без Kafka Position/Prefix.
Выбор новых bootstrap позиций после retention/gap, нового replica ID/поколения
после потери pin и процесс запуска всех сервисов остаются у внешнего runtime.

## Назначение и восстановление

Prepare вызывается реальным group protocol, до чтения цен. Для каждой партиции
CAS выдаёт новый SQL Grant; один неизменный RequestID/ExpectedToken используется
для этого вызова Assign. При неоднозначном результате новый запрос не выдаётся:
реплика завершается, условный cleanup отзывает и ожидаемый Grant. Prepare
возвращает только SQL checkpoint либо явный Bootstrap. Старые полномочия
отзываются условно; отзыв старого token не затрагивает преемника. Ошибки cleanup
возвращаются caller, а не обозначаются успехом.

Scope — hash фиксированной Environment/Group/Topic/Partition, независимый от
replica/PID/token. Полная projection ID — hash Environment/Group/Topic/replica ID
и **набора** партиций. Поэтому смена набора выбирает отдельную полную проекцию,
а повтор прежнего набора восстанавливает прежние C/after-images/cursor через
NewResume. Повтор этого набора с истёкшим pin возвращает ErrBootstrap; скрытого
reset, удаления поколения или замены snapshot нет. Проекция не шардирует C.

После всех Grant создаётся `projection.NewFenced` с полным набором. New/NewResume
получает явные PricePartitions; все Register цен идут только через текущий
Assignment, с production PairPartition. Lifetime view и обработчика ограничен
Assignment.Context. Восстановление Pending не подтверждает транспорт. Пустые
принятые наборы можно завершить без уже исчезнувшего ценового подключения;
кандидаты восстанавливаются через Resume с canonical командой API.

`Ready()` требует живое назначение, готовую view, завершённый проход Pending и
первую оценку всех текущих собственных active пар. `Grants()` возвращает копию
наблюдаемого назначения, не выдаёт новых полномочий. Для активной пары ключ
оценки зависит от projection ID/C/pair/revision: изменения подписок при той же
цене требуют новой оценки. Changed перечитывает текущую view, исторический true
не воспроизводится. SQL/RPC ошибки сохраняют durable Pending и завершают Run;
автоматического бесконечного повтора нет. Только временная смена view/её цены
перечитывается с ограничением частоты и отменой context.

## Полный prices Prefix

Единственный reader Assignment.Next передаёт все read_committed data records
в bounded канал (один элемент); основной цикл сериализует work всех своих
партиций и Changed. Межпартиционный параллелизм не требуется для корректности и
здесь не заявлен. Ни один дополнительный reader пары не конкурирует с MAT-012.

`TransportKey(Position)` устойчив при смене replica/token/projection. Для нового
события активной пары сначала ожидается MAT-012 merge не старше входной revision,
потом handler сохраняет кандидатов/исходы/superseded. Уже принятый ключ продолжает
исходную immutable работу. Новая оценка текущей view выполняется также после
superseded, не требуя следующего рыночного события.

Пара без active подписок в ready полной view получает durable пустой набор и
Complete, а не пропускается. От начального SQL/bootstrap next до каждой следующей
выданной позиции доказывается последовательный полный поток; физические gaps
control/abort records разрешены read_committed контрактом. Complete сохраняет
Prefix внутри защищённой SQL-транзакции. Только после успешного commit вызывается
Assignment.Confirm; следующая позиция обрабатывается после успеха Confirm.
Ни поиск кандидатов, ни Latest, ни subscription C не являются этим Prefix.

При eager rebalance прежняя view теряет readiness, закрывает подключения и
завершает readers; новая сессия получает новые tokens и восстанавливается.
Транспортные/SQL/RPC ошибки возвращаются, данные не сбрасываются. Fault takeover,
запоздалый старый владелец и политика повторного запуска — MAT-011.

## Проверка

`GOPATH=/tmp/binance-set009-go GOCACHE=/tmp/binance-set009-cache KAFKA_RUN_POSTGRES=1 bash infra/kafka/verify-go.sh`

`internal/api/replayrpc/matcher_replica_test.go` использует две реальные группы,
16 партиций, PostgreSQL через независимые pool, готовые snapshot fixtures и
mTLS Replay. Управляемый Delivery возвращает canonical ответы, subscription
feed использует готовую deterministic fixture. Реальные SUB-014/mTLS исходы
проверяются существующими тестами pricehandler/deliveryrpc.

Проверяются отсутствие пересечения, монотонные tokens после обычного rebalance,
полные проекции обеих пар, Pending перед readiness, первая Latest без cursor,
порядок двух цен в каждой партиции через commit/abort gaps (aborted работы
отсутствуют в SQL), все пары без подписок, запрет broker commit
впереди SQL при задержанном API и сходимость всех broker/SQL prefixes. Отмена
останавливает/join-ит workers. Нагрузочная квалификация и failover не заявлены.
