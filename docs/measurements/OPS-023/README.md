# OPS-023 — совместный фазовый runner

Контроль интерфейса двух collectors на disposable Docker-профиле, 5 ноября 2026.
Это не квалификация стоимости или профиль LOAD DEC-008. Протокол и команды —
[infra/measure/README.md](../../../infra/measure/README.md).

| Опыт | API accounts | API relations empty / write, B | WAL empty / write, B | Kafka event / transaction log, B |
| --- | --- | --- | --- | --- |
| run-a2 | 0 → 10 | 0 / 32768 | 0 / 5400 | 224 / 548 |
| run-b | 0 → 10 | 0 / 32768 | 48832 / 5528 | 224 / 548 |
| run-final | 0 → 10 | 0 / 32768 | 184 / 190784 | 224 / 548 |

У каждого опыта собственные project/prefix/порты/volumes. Setup применяет миграции
четырёх владельцев, готовит transactional producer/offsets, затем идут baseline,
empty, write и verification. Write вызывает готовые account.Repository.Create
и kafka.New/Publish; verification штатного kafkameasure проверяет read_committed,
точные bytes/key и две записи (prepare + payload). Control manifest связывает
публикацию с event_id ops020-probe-1; trigger/attempt в этом контроле отсутствуют.
Идентичности и единицы бизнес-цепочки — отдельные manifests OPS-024–026/022.

Фон не вычитается. Большой empty WAL run-b показывает, почему нельзя проверять
малую запись условием WAL(write) > WAL(empty). Число строк и рост app relations
подтверждены независимо; все operation/checkpoint/background deltas сохранены.
Kafka metadata log write +504 B отдельно от event/transaction. Нулевой allocated
рост в уже выделенных блоках не означает нулевую физическую стоимость.

Каждый каталог содержит manifest/report v1 и полные raw обоих collectors,
schema dumps, hashes исходников/миграций/контрактов, версии/digest образов,
durability/config, время и cleanup. report.source_commit + dirty обозначают
базовый commit; фактически измеренную редакцию задают source_sha256.
Предварительные успешные run-a2/run-b предшествуют исправлению close завершённого
adapter; run-final проверяет окончательную редакцию runner и успешно очищает ресурсы. Ошибка close не влияла на их
успешные данные/cleanup.

В failure adapter намеренно завершается без ack на write после setup/empty;
в timeout он не отвечает на write 60 с. Оба опыта должны закончиться exit 1,
без ack write/verification, с failed обоими collectors и пустыми cleanup lists.
Неполные отчёты не пригодны для стоимости.

```sh
python3 scripts/verify_joint_measurement.py docs/measurements/OPS-023/run-a2 docs/measurements/OPS-023/run-b docs/measurements/OPS-023/run-final --failed docs/measurements/OPS-023/failure docs/measurements/OPS-023/timeout
```

format-check.json сохраняет результаты проверки изменённых временных копий:
удалённый ack фазы, удалённая числовая метрика, unknown/null вместо числа
отвергаются. Исходные raw не изменялись. При проверке JSON-lines ack, переданный
двумя фрагментами, принят в пределах общего deadline.

Все успешные отчёты имеют qualified=false, collected-not-qualified.
Kafka fsync count/time, PostgreSQL temp peak/device IO unsupported; отсутствуют
бизнес-максимумы/roll/ledger/нагрузочная квалификация. OPS-018 и OPS-015/016
этим контролем не разблокированы. Полный functional/race suite не запускался.
