# Бэклог Binance Price Alerts

Обновлён 8 ноября 2026 по [DEC-011](../decisions/DEC-011.md).
Цель — рабочий production ready пет-проект без тяжёлой нагрузочной квалификации.
Функциональный E2E на fake готов; реальный deployment ещё не квалифицирован.

[Требования](../requirements.md) · [Очередь](ORDER.md) · [Правила проверки](workflow.md) ·
[Модели](MODELS.md) · [Интеграции](../integration-facts.md)

## Порядок работ

В очереди 6 незавершённых практических задач выпуска. MKT-019 завершён:
общий каталог и динамический durable ingest подключены; feed остаётся unconfirmed.
Предпосылки OPS-074 готовы: OPS-078 (явный source provisioning) и E2E-009
(listener существующих fake). Следующий — сессионная проверка OPS-074
(постоянный запуск). Затем HTTPS/секреты, backup/restore, базовое наблюдение,
малый restart smoke и реальный сценарий Binance → свой Telegram.
Подробные критерии — в карточках.

22 бывшие planned задачи сняты с плана как `cancelled`: kernel/inode/temp peak,
квалификация дисковых ledger, measurement collector race и все оставшиеся LOAD.
Они остаются в таблицах для истории, отсутствуют в ORDER и не выдаются `next`.
Отмена не означает успешную проверку. Готовые артефакты и код сохраняются.

Карточки — источник статуса; индекс повторяет статус и зависимости.
`python3 scripts/backlog.py next` выбирает одну задачу из ORDER;
`check` проверяет весь бэклог. Каждая реализация выполняется отдельной сессией.

## Архитектурные ограничения плана

Четыре процесса и владение данными утверждены в [DEC-003](../decisions/DEC-003.md): API владеет аккаунтами, подписками, лимитом, срабатываниями и outbox; market-data — котировками; matcher — проекцией; notifications — доставкой. Matcher вызывает операцию API, прямой доступ к чужим таблицам запрещён. Последняя цена, условная доставка и гонки определены DEC-004; очередь не освобождает слот, результаты доставки сохраняет API. Хранилище определено DEC-006: PostgreSQL с четырьмя изолированными БД; общий журнал API — SUB-021, атомарный приём рынка — MKT-017. Брокер выбран DEC-007: Kafka/KRaft; SQL-replay — SUB-022, безопасная очистка Kafka — OPS-014. Обсуждение снимка, версии, cursor и проекции нельзя пропустить ради скорости написания кода.

Миллионный fixture, p99 и burst DEC-008 сняты с требований к выпуску по DEC-011.
Ранее выполненные измерения сохраняются как история, без обещания ёмкости.

## Исполняемые карточки и открытые решения

### Решения

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [DEC-001](tickets/DEC-001.md) | Определить точную семантику достижения цены | — | done |
| [DEC-002](tickets/DEC-002.md) | Установить критерий свежести цены | — | done |
| [DEC-003](tickets/DEC-003.md) | Зафиксировать владельцев данных четырёх сервисов | — | done |
| [DEC-004](tickets/DEC-004.md) | Определить границу активации подписки | DEC-001, DEC-002, DEC-003 | done |
| [DEC-005](tickets/DEC-005.md) | Определить поведение без привязанного Telegram | — | done |
| [DEC-006](tickets/DEC-006.md) | Выбрать постоянное хранилище | DEC-003, DEC-004 | done |
| [DEC-007](tickets/DEC-007.md) | Выбрать брокер событий | DEC-003, DEC-004, DEC-006 | done |
| [DEC-008](tickets/DEC-008.md) | Зафиксировать воспроизводимый профиль нагрузки | — | done |
| [DEC-009](tickets/DEC-009.md) | Закрепить транспорт межсервисных вызовов | DEC-003, DEC-004, DEC-007 | done |
| [DEC-010](tickets/DEC-010.md) | Поставить рабочий локальный запуск перед квалификацией нагрузки | DEC-008, DEC-009 | done |
| [DEC-011](tickets/DEC-011.md) | Упростить выпуск пет-проекта до практической production готовности | DEC-010 | done |

### Каркас и локальный стенд

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [SET-001](tickets/SET-001.md) | Создать каркас Go-репозитория | — | done |
| [SET-002](tickets/SET-002.md) | Добавить проверку Go в CI | SET-001 | done |
| [SET-003](tickets/SET-003.md) | Поднять локальное хранилище | DEC-006, DEC-008 | done |
| [SET-004](tickets/SET-004.md) | Добавить запуск миграций | SET-001, SET-003 | done |
| [SET-005](tickets/SET-005.md) | Поднять локальный брокер | DEC-007, DEC-008 | done |
| [SET-006](tickets/SET-006.md) | Подготовить подключение и интеграционные проверки Go с PostgreSQL | SET-001, SET-003, SET-004 | done |
| [SET-007](tickets/SET-007.md) | Подготовить генерацию Protobuf и сетевые проверки unary gRPC | SET-001, SET-002, DEC-009 | done |
| [SET-008](tickets/SET-008.md) | Подготовить сервисные полномочия внутренних gRPC-вызовов | SET-007, DEC-009 | done |
| [SET-009](tickets/SET-009.md) | Подготовить Go-клиент и транзакционные проверки Kafka | SET-001, SET-005, DEC-007 | done |
| [SET-010](tickets/SET-010.md) | Подготовить PostgreSQL harness matcher | SET-006, SET-004 | done |

### Контракты

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [CON-001](tickets/CON-001.md) | Определить общий конверт события | DEC-007, SET-001 | done |
| [CON-002](tickets/CON-002.md) | Определить событие изменения подписки | CON-001, DEC-004 | done |
| [CON-003](tickets/CON-003.md) | Определить событие рыночной цены | CON-001, DEC-002, DEC-004 | done |
| [CON-004](tickets/CON-004.md) | Определить событие срабатывания | CON-001, CON-002, CON-003, DEC-004 | done |

### Домен

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [DOM-001](tickets/DOM-001.md) | Ввести точный тип цены (коэффициент и масштаб) | SET-001 | done |
| [DOM-002](tickets/DOM-002.md) | Преобразовать процент в фиксированный порог | DOM-001, DEC-001 | done |
| [DOM-003](tickets/DOM-003.md) | Реализовать предикат срабатывания | DOM-001, DEC-001 | done |
| [DOM-004](tickets/DOM-004.md) | Реализовать одноразовый жизненный цикл | SET-001, DEC-004, DEC-005, DOM-002, DOM-003 | done |
| [DOM-005](tickets/DOM-005.md) | Реализовать проверку свежести котировки | SET-001, DEC-002 | done |

### Аккаунты и Telegram

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [IAM-001](tickets/IAM-001.md) | Сохранить учётную запись пользователя | SET-004, SET-006 | done |
| [IAM-002](tickets/IAM-002.md) | Реализовать хранение и проверку пароля | SET-001 | done |
| [IAM-003](tickets/IAM-003.md) | Реализовать регистрацию по email | IAM-001, IAM-002 | done |
| [IAM-004](tickets/IAM-004.md) | Реализовать вход с ограниченным сроком сессии | IAM-001, IAM-002 | done |
| [IAM-005](tickets/IAM-005.md) | Проверять сессию на защищённом endpoint | IAM-004 | done |
| [IAM-006](tickets/IAM-006.md) | Выдать одноразовый код привязки Telegram | IAM-005, SET-004 | done |
| [IAM-007](tickets/IAM-007.md) | Проверять подлинность Telegram webhook | SET-001 | done |
| [IAM-008](tickets/IAM-008.md) | Поглотить код и привязать Telegram chat | IAM-006, IAM-007, DEC-005 | done |

### Подписки

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [SUB-001](tickets/SUB-001.md) | Сохранить подписку с версией состояния | SET-004, DOM-001, DOM-004, DEC-003 | done |
| [SUB-002](tickets/SUB-002.md) | Обеспечить лимит 15 при конкурентном создании | SUB-001, DEC-004 | done |
| [SUB-003](tickets/SUB-003.md) | Создать подписку через HTTP | SUB-002, IAM-005, DOM-002, DOM-005, MKT-001, MKT-006, DEC-005, IAM-008, MKT-018 | done |
| [SUB-004](tickets/SUB-004.md) | Вернуть активные подписки владельца | SUB-001, IAM-005 | done |
| [SUB-005](tickets/SUB-005.md) | Отключить свою активную подписку | SUB-001, SUB-002, IAM-005, DOM-004 | done |
| [SUB-007](tickets/SUB-007.md) | Опубликовать subscription outbox | SUB-011, SUB-012, SET-005 | done |
| [SUB-009](tickets/SUB-009.md) | Опубликовать outbox срабатываний | SUB-013, SUB-014, SET-005, SUB-007 | done |
| [SUB-021](tickets/SUB-021.md) | Добавить транзакционный журнал изменений API | SET-004, CON-002, DEC-006 | done |
| [SUB-022](tickets/SUB-022.md) | Открыть защищённый SQL-replay подписок | SUB-021, SUB-015, SUB-016, DEC-007, DEC-009 | done |
| [SUB-011](tickets/SUB-011.md) | Сохранить outbox при создании подписки | SUB-002, CON-002, SUB-021 | done |
| [SUB-012](tickets/SUB-012.md) | Сохранить outbox при отмене подписки | SUB-005, CON-002, SUB-021 | done |
| [SUB-013](tickets/SUB-013.md) | Сохранить атомарный запрос работы доставки | SUB-001, DOM-003, DOM-004, CON-004, DEC-004, SUB-021 | done |
| [SUB-014](tickets/SUB-014.md) | Открыть внутренний запрос работы доставки | SUB-013, DEC-003, MKT-006, DEC-009 | done |
| [SUB-015](tickets/SUB-015.md) | Прочитать согласованный снимок из хранилища | SUB-001, SUB-011, SUB-012, DEC-004, SUB-021 | done |
| [SUB-023](tickets/SUB-023.md) | Создать снимок идемпотентно по request ID | SUB-015, DEC-009 | done |
| [SUB-016](tickets/SUB-016.md) | Открыть внутренний интерфейс страниц снимка | SUB-015, SUB-023, DEC-003, DEC-009 | done |
| [SUB-025](tickets/SUB-025.md) | Передать владельца подписки в live-проекцию matcher | CON-002, SUB-011, SUB-012, SUB-013, SUB-017, SUB-019, SUB-014, MAT-014 | done |
| [SUB-024](tickets/SUB-024.md) | Исследовать нестабильный expiry в SQL snapshot RPC тестах | SUB-016, SET-006 | done |
| [SUB-027](tickets/SUB-027.md) | Подготовить адресата до лимитеров и проверить его при допуске | SUB-018, NTF-005, DEC-005 | done |
| [SUB-026](tickets/SUB-026.md) | Сохранить блокировку после промежуточного исхода той же попытки | SUB-020 | done |
| [SUB-017](tickets/SUB-017.md) | Сохранить допуск одной попытки доставки | SUB-013, DOM-004, DEC-004, IAM-008, DEC-005, SUB-021 | done |
| [SUB-018](tickets/SUB-018.md) | Открыть допуск доставки с проверкой последней цены | SUB-017, MKT-006, DEC-009 | done |
| [SUB-019](tickets/SUB-019.md) | Сохранить исход попытки в подписке | SUB-017, DOM-004, SUB-021 | done |
| [SUB-020](tickets/SUB-020.md) | Открыть фиксацию исхода доставки | SUB-019, DEC-009 | done |

### Рыночные данные

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [MKT-018](tickets/MKT-018.md) | Выдать каталог пар через внутренний gRPC | MKT-001, DEC-003, DEC-009, SET-007, SET-008 | done |
| [MKT-001](tickets/MKT-001.md) | Получить каталог доступных spot-пар USDT | SET-001 | done |
| [MKT-002](tickets/MKT-002.md) | Декодировать одно событие сделки Binance | CON-003, DOM-001 | done |
| [MKT-005](tickets/MKT-005.md) | Публиковать только монотонные цены пары | MKT-002, CON-003 | done |
| [MKT-017](tickets/MKT-017.md) | Сохранить котировку и работу публикации атомарно | SET-004, MKT-005, CON-003, DEC-006 | done |
| [MKT-006](tickets/MKT-006.md) | Выдать последнюю известную котировку с возрастом | MKT-005, DOM-005, DEC-003, DEC-004, MKT-017, DEC-009, SET-007, SET-008 | done |
| [MKT-009](tickets/MKT-009.md) | Прочитать цены по одному WebSocket-соединению | MKT-002 | done |
| [MKT-010](tickets/MKT-010.md) | Ограничить управляющие команды WebSocket | MKT-009 | done |
| [MKT-011](tickets/MKT-011.md) | Переподключиться после обрыва WebSocket | MKT-009, MKT-010 | done |
| [MKT-012](tickets/MKT-012.md) | Планово заменить WebSocket-соединение | MKT-011, MKT-005 | done |
| [MKT-013](tickets/MKT-013.md) | Опубликовать одну цену с подтверждением брокера | MKT-005, SET-005, CON-003, DEC-007, SET-009 | done |
| [MKT-014](tickets/MKT-014.md) | Ограничить буфер принятых цен | MKT-013, DEC-007, MKT-017 | done |
| [MKT-015](tickets/MKT-015.md) | Построить план уникальных рыночных потоков | MKT-001 | done |
| [MKT-016](tickets/MKT-016.md) | Применить план потоков к действующим соединениям | MKT-015, MKT-011, MKT-012 | done |

### Проверка условий

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [MAT-001](tickets/MAT-001.md) | Применить версионированное изменение подписки | CON-002, DEC-003, SET-005, SET-004, SET-010 | done |
| [MAT-002](tickets/MAT-002.md) | Находить кандидатов в индексе порогов | MAT-001, DOM-003 | done |
| [MAT-004](tickets/MAT-004.md) | Обработать цену и запросить фиксацию кандидатов | MAT-002, MAT-007, MAT-008, MAT-009, MKT-013, MKT-014, SUB-013, SUB-014, MAT-012, MAT-016, SUB-025 | done |
| [MAT-016](tickets/MAT-016.md) | Сохранить durable результат обработки цены | SET-010, CON-003, MAT-014, DEC-007 | done |
| [MAT-005](tickets/MAT-005.md) | Восстановить обработку цены после частичного сбоя | MAT-004 | done |
| [MAT-007](tickets/MAT-007.md) | Загрузить исходный снимок в проекцию matcher | MAT-001, SUB-016, DEC-004 | done |
| [MAT-014](tickets/MAT-014.md) | Подготовить durable проекцию снимка для SQL-replay | MAT-001, MAT-007, SUB-022, DEC-007 | done |
| [MAT-015](tickets/MAT-015.md) | Подготовить Kafka-источник изменений подписок | SET-009, CON-002, SUB-007, DEC-007 | done |
| [MAT-008](tickets/MAT-008.md) | Догнать изменения и переключить matcher в live | MAT-002, MAT-007, DEC-004, DEC-007, MAT-012, SUB-022, MAT-014, MAT-015 | done |
| [MAT-009](tickets/MAT-009.md) | Возобновить проекцию с сохранённого cursor | MAT-008 | done |
| [MAT-013](tickets/MAT-013.md) | Подготовить источник ценовых обновлений matcher | SET-009, CON-003, MKT-014, DEC-007 | done |
| [MAT-012](tickets/MAT-012.md) | Подключить подписку к последней цене | MAT-001, MAT-002, MKT-006, MKT-013, MKT-014, DEC-004, MAT-013 | done |
| [MAT-017](tickets/MAT-017.md) | Защитить SQL-записи matcher fencing token | MAT-005, MAT-014, SET-010, DEC-007 | done |
| [MAT-018](tickets/MAT-018.md) | Подготовить групповой источник цен matcher | MAT-013, SET-009, DEC-007 | done |
| [MAT-019](tickets/MAT-019.md) | Ограничить live-подключения назначенными ценовыми партициями | MAT-009, MAT-018, DEC-007 | done |
| [MAT-010](tickets/MAT-010.md) | Назначить партиции двум работающим matcher | MAT-005, DEC-007, MAT-017, MAT-018, MAT-019 | done |
| [MAT-011](tickets/MAT-011.md) | Передать партицию после отказа matcher | MAT-010 | done |

### Доставка

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [NTF-001](tickets/NTF-001.md) | Создать durable задание из срабатывания | CON-004, SUB-009, SET-004, DEC-003 | done |
| [NTF-002](tickets/NTF-002.md) | Сформировать текст Telegram-уведомления | CON-004, DOM-001 | done |
| [NTF-003](tickets/NTF-003.md) | Вызвать Telegram sendMessage | NTF-002, IAM-008 | done |
| [NTF-004](tickets/NTF-004.md) | [Ограничить отправку по боту](../../internal/notifications/botlimit/README.md) | NTF-003, DEC-003 | done |
| [NTF-005](tickets/NTF-005.md) | [Ограничить отправку в отдельный чат](../../internal/notifications/chatlimit/README.md) | NTF-004 | done |
| [NTF-006](tickets/NTF-006.md) | Назначить повтор по retry_after и временной ошибке | NTF-003, NTF-001 | done |
| [NTF-007](tickets/NTF-007.md) | Обработать неоднозначный timeout отправки | NTF-003, NTF-006, NTF-009, NTF-010 | done |
| [NTF-008](tickets/NTF-008.md) | Завершить задание при постоянной ошибке | NTF-001, NTF-003, NTF-006, NTF-009, NTF-010, NTF-007 | done |
| [NTF-009](tickets/NTF-009.md) | [Выдать worker задание с ограниченной арендой](../../internal/notifications/jobs/README.md) | NTF-001, NTF-006 | done |
| [NTF-010](tickets/NTF-010.md) | Сохранить успешную доставку | NTF-003, NTF-009 | done |
| [NTF-016](tickets/NTF-016.md) | Сохранить временный отказ и обязанность отчёта | NTF-006, NTF-009, NTF-010, NTF-007, NTF-008 | done |
| [NTF-017](tickets/NTF-017.md) | Закрыть изменение исторических свидетельств ролью notifications | NTF-008, NTF-016 | done |
| [NTF-018](tickets/NTF-018.md) | Исследовать сбой восстановления после потери ответа регистрации | NTF-012 | done |
| [NTF-019](tickets/NTF-019.md) | Устранить ложный отказ проверки Telegram payload по подстроке возраста | NTF-012 | done |
| [NTF-020](tickets/NTF-020.md) | Исследовать состояние bootstrap после отмены SQL | OPS-064 | done |
| [NTF-014](tickets/NTF-014.md) | Надёжно передать исход попытки владельцу подписки | NTF-010, SUB-020, NTF-007, NTF-008, NTF-016, SUB-026 | done |
| [NTF-015](tickets/NTF-015.md) | Возобновить задание после восстановления Telegram | NTF-008, NTF-009, SUB-018, DEC-005 | done |
| [NTF-012](tickets/NTF-012.md) | Выполнить одну попытку обработки готового задания | NTF-004, NTF-005, NTF-006, NTF-007, NTF-008, NTF-009, NTF-010, SUB-018, NTF-014, NTF-002, NTF-015, NTF-003, NTF-016, SUB-027 | done |
| [NTF-013](tickets/NTF-013.md) | Запустить ограниченный цикл notification worker | NTF-012 | done |

### Эксплуатационное поведение

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [OPS-001](tickets/OPS-001.md) | Добавить health и readiness API | SET-001, DEC-003, DEC-010 | done |
| [OPS-002](tickets/OPS-002.md) | Измерять задержку от цены до trigger | MAT-004, SUB-013, SUB-014, OPS-045 | done |
| [OPS-003](tickets/OPS-003.md) | Измерять состояние очереди доставки | NTF-012, NTF-013 | done |
| [OPS-047](tickets/OPS-047.md) | Подготовить disposable harness отдельного процесса API | SET-009, SUB-005, SUB-012, IAM-005, SUB-009, DEC-010 | done |
| [OPS-046](tickets/OPS-046.md) | Собрать минимальный процесс API для отмены и relay | SUB-005, SUB-012, IAM-005, SUB-007, SUB-009, OPS-001, DEC-010, OPS-047 | done |
| [OPS-004](tickets/OPS-004.md) | Корректно остановить API | SUB-007, SUB-009, OPS-046 | done |
| [OPS-006](tickets/OPS-006.md) | Добавить health и readiness market-data | MKT-006, MKT-015, MKT-016, DEC-010 | done |
| [OPS-007](tickets/OPS-007.md) | Добавить health и readiness matcher | MAT-007, MAT-008, MAT-009 | done |
| [OPS-008](tickets/OPS-008.md) | Добавить health и readiness notifications | NTF-012, NTF-013 | done |
| [OPS-049](tickets/OPS-049.md) | Подготовить disposable окружение процесса market-data | SET-009, MKT-017, MKT-013, OPS-047, DEC-010 | done |
| [OPS-050](tickets/OPS-050.md) | Открыть границу каждой попытки WebSocket reconnect | MKT-011, MKT-012 | done |
| [OPS-051](tickets/OPS-051.md) | Настроить стартовую HTTP-пробу process harness | OPS-047, OPS-049, OPS-006 | done |
| [OPS-048](tickets/OPS-048.md) | Собрать минимальный процесс market-data | MKT-005, MKT-017, MKT-013, MKT-014, MKT-011, OPS-006, DEC-010, OPS-049, OPS-050, OPS-051 | done |
| [OPS-009](tickets/OPS-009.md) | Корректно остановить market-data | MKT-013, MKT-014, MKT-015, MKT-016, OPS-048 | done |
| [OPS-052](tickets/OPS-052.md) | Подготовить disposable окружение процесса matcher | OPS-047, MAT-011, SUB-016, MKT-006, DEC-010 | done |
| [OPS-053](tickets/OPS-053.md) | Собрать минимальный процесс matcher | MAT-011, MAT-015, SUB-016, OPS-007, OPS-052, DEC-010 | done |
| [OPS-010](tickets/OPS-010.md) | Корректно остановить matcher | MAT-005, OPS-053 | done |
| [OPS-054](tickets/OPS-054.md) | Подготовить disposable окружение процесса notifications | OPS-047, NTF-001, NTF-012, NTF-013, SUB-018, SUB-020, DEC-010 | done |
| [OPS-055](tickets/OPS-055.md) | Собрать минимальный процесс notifications | NTF-001, NTF-012, NTF-013, OPS-008, OPS-054, DEC-010 | done |
| [OPS-011](tickets/OPS-011.md) | Корректно остановить notification worker | NTF-012, NTF-013, OPS-055 | done |
| [OPS-012](tickets/OPS-012.md) | Ограничить размер тела публичного запроса | IAM-003, IAM-004, SUB-003 | done |
| [OPS-013](tickets/OPS-013.md) | Ограничить частоту входа и регистрации между репликами | IAM-003, IAM-004, DEC-003, DEC-006 | done |
| [OPS-060](tickets/OPS-060.md) | Проверить поколение Kafka-источника на входах matcher | MAT-015, MAT-018, SET-009 | done |
| [OPS-061](tickets/OPS-061.md) | Сохранить source binding Kafka checkpoints matcher в SQL | OPS-060, MAT-017, MAT-016, MAT-009 | done |
| [OPS-063](tickets/OPS-063.md) | Проверить provenance prices scope до первого transport checkpoint | OPS-061 | done |
| [OPS-062](tickets/OPS-062.md) | Подключить source binding к runtime и восстановлению matcher | OPS-060, OPS-061, OPS-053, OPS-010, OPS-063 | done |
| [OPS-056](tickets/OPS-056.md) | Экспортировать durable Kafka-прогресс matcher | MAT-011, MAT-009, SET-007, SET-008, OPS-053, OPS-062 | done |
| [OPS-064](tickets/OPS-064.md) | Сохранить source binding intake notifications в SQL | NTF-001, OPS-060 | done |
| [OPS-065](tickets/OPS-065.md) | Проверить поколение Kafka-источника на входе notifications | OPS-064, OPS-060, SET-009 | done |
| [OPS-067](tickets/OPS-067.md) | Исследовать нестабильную metadata после recreate в changefeed fixture | OPS-060, SET-009 | done |
| [OPS-066](tickets/OPS-066.md) | Подключить source binding к runtime notifications | OPS-064, OPS-065, OPS-055, OPS-011, OPS-054 | done |
| [OPS-057](tickets/OPS-057.md) | Экспортировать durable Kafka-прогресс notifications | NTF-001, SET-007, SET-008, OPS-055, OPS-066 | done |
| [OPS-068](tickets/OPS-068.md) | Управлять отказами OffsetCommit и DeleteRecords в Kafka fault proxy | SET-009 | done |
| [OPS-058](tickets/OPS-058.md) | Подготовить disposable окружение проверки Kafka cleaner | OPS-056, OPS-057, OPS-052, OPS-054, SET-009, OPS-068 | done |
| [OPS-014](tickets/OPS-014.md) | Удалить безопасный префикс журнала Kafka | SET-005, SUB-022, MAT-011, NTF-001, DEC-007, OPS-056, OPS-057, OPS-058 | done |
| [OPS-019](tickets/OPS-019.md) | Собрать физические метрики PostgreSQL на disposable стенде | SET-006, DEC-008 | done |
| [OPS-020](tickets/OPS-020.md) | Собрать физические метрики Kafka на disposable стенде | SET-009, DEC-008 | done |
| [OPS-022](tickets/OPS-022.md) | Измерить manifest источника market-data и свести покрытие затрат | OPS-019, OPS-020, OPS-026, MKT-014 | done |
| [OPS-023](tickets/OPS-023.md) | Подключить внешние фазы к совместному измерительному runner | OPS-019, OPS-020 | done |
| [OPS-024](tickets/OPS-024.md) | Измерить manifest операций владельца API | OPS-023, SUB-009, SUB-020, SUB-027 | done |
| [OPS-025](tickets/OPS-025.md) | Измерить продолжение API в matcher | OPS-024, MAT-011 | done |
| [OPS-026](tickets/OPS-026.md) | Измерить продолжение API в notifications и свести цепочку | OPS-025, NTF-013, NTF-014, NTF-015, OPS-028 | done |
| [OPS-028](tickets/OPS-028.md) | Подготовить API fixture продолжения notifications с исходными ID | OPS-024, OPS-025, NTF-014, NTF-012 | done |
| [OPS-029](tickets/OPS-029.md) | Измерить количество и время fsync процесса Kafka | OPS-020, OPS-023 | done |
| [OPS-033](tickets/OPS-033.md) | Сохранять и проверять передачу FD через ancillary data | OPS-019 | done |
| [OPS-038](tickets/OPS-038.md) | Сохранять независимый census и полные inode inventories | OPS-033 | done |
| [OPS-039](tickets/OPS-039.md) | Сохранять lifecycle evidence при shared state и nonleader exec | OPS-038 | done |
| [OPS-040](tickets/OPS-040.md) | Проверить reference witnesses и повторить PostgreSQL capture | OPS-038, OPS-039 | done |
| [OPS-041](tickets/OPS-041.md) | Сохранить shared-fileset PG spill в новом raw-контракте | OPS-040 | done |
| [OPS-042](tickets/OPS-042.md) | Определить доказуемый lifetime envelope и capture предпосылки | OPS-041 | done |
| [OPS-045](tickets/OPS-045.md) | Подготовить наблюдения границ задержки цена → durable workflow | MKT-014, SUB-013, SUB-014, DEC-008, DEC-010 | done |

| [OPS-069](tickets/OPS-069.md) | Подключить Catalog и Latest RPC к процессу market-data | OPS-009, MKT-006, MKT-018, SET-008 | done |
| [OPS-070](tickets/OPS-070.md) | Подключить RPC matcher к процессу API | OPS-004, SUB-016, SUB-022, SUB-014, SET-008 | done |
| [OPS-071](tickets/OPS-071.md) | Подключить RPC notifications к процессу API | OPS-070, SUB-018, SUB-020, SUB-027, MKT-006 | done |
| [OPS-072](tickets/OPS-072.md) | Подключить Telegram-привязку к процессу API | OPS-004, OPS-012, IAM-006, IAM-007, IAM-008, E2E-004 | done |
| [OPS-073](tickets/OPS-073.md) | Подключить создание и список подписок к процессу API | OPS-071, OPS-072, SUB-003, SUB-004, SUB-011, MKT-018 | done |

Исследовательские OPS-017/021/030/031/037 и готовые измерения выше — история.
Незавершённые зависимости kernel/inode/temp peak и физической квалификации
сняты с плана по DEC-011. Их прежние критерии не объявляются выполненными;
[raw OPS-031](../measurements/OPS-031/README.md) сохраняет unknown/peak=null.

### Сквозная проверка

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [E2E-001](tickets/E2E-001.md) | Проверить один полный пользовательский сценарий | SUB-003, SUB-004, SUB-011, SUB-012, SUB-007, SUB-009, MAT-004, NTF-012, NTF-013, E2E-002 | done |
| [E2E-002](tickets/E2E-002.md) | Запустить четыре процесса в одном локальном стенде | E2E-003, E2E-004, OPS-001, OPS-006, OPS-007, OPS-008, OPS-069, OPS-070, OPS-071, OPS-072, OPS-073, E2E-005 | done |
| [E2E-003](tickets/E2E-003.md) | Поднять управляемый fake Binance для стенда | MKT-001, MKT-002, CON-003 | done |
| [E2E-004](tickets/E2E-004.md) | Поднять управляемый fake Telegram для стенда | IAM-007, NTF-003 | done |
| [E2E-005](tickets/E2E-005.md) | Подготовить общее disposable окружение четырёх сервисов | OPS-047, OPS-049, OPS-052, OPS-054 | done |

## Практический выпуск — DEC-011

| ID | Один результат | Зависимости | Статус |
| --- | --- | --- | --- |
| [MKT-020](tickets/MKT-020.md) | Добавить lifecycle попытки в stream reconciler | MKT-016, OPS-050 | done |
| [E2E-008](tickets/E2E-008.md) | Расширить fake Binance для плана каталога | E2E-003, MKT-016 | done |
| [MKT-019](tickets/MKT-019.md) | Подключить каталог торговых пар к runtime market-data | DEC-011, MKT-016, MKT-017, OPS-069, MKT-020, E2E-008 | done |
| [OPS-078](tickets/OPS-078.md) | Подготовить явный source provisioning постоянного стенда | OPS-062, OPS-066, E2E-005 | done |
| [E2E-009](tickets/E2E-009.md) | Разрешить явный listener существующих vendor fake | E2E-008, E2E-004 | done |
| [OPS-074](tickets/OPS-074.md) | Собрать постоянный запуск четырёх сервисов | DEC-011, MKT-019, E2E-002, OPS-078, E2E-009 | planned |
| [OPS-075](tickets/OPS-075.md) | Защитить публичный вход и секреты развёртывания | DEC-011, OPS-074, OPS-013, OPS-072 | planned |
| [OPS-076](tickets/OPS-076.md) | Подготовить резервную копию и восстановление стенда | DEC-011, OPS-074, OPS-014 | planned |
| [OPS-077](tickets/OPS-077.md) | Оформить наблюдение и обслуживание постоянного стенда | DEC-011, OPS-074, OPS-014, OPS-002, OPS-003 | planned |
| [E2E-007](tickets/E2E-007.md) | Проверить восстановление малого рабочего стенда | DEC-011, OPS-074, NTF-018, SUB-024, OPS-067 | planned |
| [E2E-006](tickets/E2E-006.md) | Подтвердить реальный запуск и доставку перед выпуском | DEC-011, OPS-075, OPS-076, OPS-077, E2E-007, NTF-017, NTF-019 | planned |

## Снято с плана — исторический архив

Эти карточки не входят в маршрут выпуска. Причины — DEC-011; старые
передачи внутри карточек не разрешают их возобновление. Сохранены для
истории, а не как список следующих задач.

| ID | Прежний результат | Прежние зависимости | Статус |
| --- | --- | --- | --- |
| [OPS-015](tickets/OPS-015.md) | Остановить новый приём API при исчерпании бюджета | SUB-003, SUB-005, SUB-020, SUB-007, SUB-009, DEC-008, OPS-018 | cancelled |
| [OPS-016](tickets/OPS-016.md) | Остановить приём рынка при исчерпании бюджета | MKT-017, MKT-014, DEC-008, OPS-018 | cancelled |
| [OPS-018](tickets/OPS-018.md) | Квалифицировать таблицу стоимости и квоты durable приёма | OPS-022, DEC-008, OPS-029, OPS-032 | cancelled |
| [OPS-027](tickets/OPS-027.md) | Устранить гонку описи временных Kafka-файлов | OPS-020, OPS-023 | cancelled |
| [OPS-032](tickets/OPS-032.md) | Подключить и проверить temp peak observer в фазовом runner | OPS-036, OPS-023 | cancelled |
| [OPS-034](tickets/OPS-034.md) | Замкнуть модель ссылок и идентичности work inode | OPS-033, OPS-040, OPS-041, OPS-042, OPS-043 | cancelled |
| [OPS-035](tickets/OPS-035.md) | Восстановить доказанные границы apparent peak из raw | OPS-034 | cancelled |
| [OPS-036](tickets/OPS-036.md) | Проверить самостоятельный observer на PostgreSQL | OPS-035 | cancelled |
| [OPS-043](tickets/OPS-043.md) | Сохранить incarnation binding и terminal retirement witness | OPS-042, OPS-044 | cancelled |
| [OPS-044](tickets/OPS-044.md) | Подготовить проверенный kernel tracing toolchain | OPS-042 | cancelled |
| [LOAD-001](tickets/LOAD-001.md) | Сгенерировать воспроизводимые данные нагрузки | DEC-008, SUB-001, IAM-001 | cancelled |
| [LOAD-002](tickets/LOAD-002.md) | Создать источник цен для нагрузочного стенда | DEC-008, CON-003, MKT-013, MKT-014 | cancelled |
| [LOAD-003](tickets/LOAD-003.md) | Измерить p99 на миллионе подписок | LOAD-001, LOAD-002, OPS-002, E2E-001, LOAD-009 | cancelled |
| [LOAD-004](tickets/LOAD-004.md) | Проверить burst 10000 срабатываний/с | LOAD-003, OPS-003, NTF-012, NTF-013 | cancelled |
| [LOAD-006](tickets/LOAD-006.md) | Проверить восстановление брокера под нагрузкой | LOAD-004, OPS-004, OPS-009 | cancelled |
| [LOAD-007](tickets/LOAD-007.md) | Проверить восстановление matcher под нагрузкой | LOAD-004, MAT-010, MAT-011, OPS-010 | cancelled |
| [LOAD-008](tickets/LOAD-008.md) | Проверить восстановление notifications под нагрузкой | LOAD-004, OPS-011 | cancelled |
| [LOAD-009](tickets/LOAD-009.md) | Собрать команду воспроизводимого нагрузочного прогона | E2E-002, LOAD-001, LOAD-002, OPS-002, OPS-003 | cancelled |
| [LOAD-010](tickets/LOAD-010.md) | Измерить обычный профиль на двух matcher | LOAD-003, LOAD-009, MAT-011 | cancelled |
| [LOAD-011](tickets/LOAD-011.md) | Измерить обычный профиль на четырёх matcher | LOAD-003, LOAD-009, MAT-011 | cancelled |
| [LOAD-012](tickets/LOAD-012.md) | Измерить горячую пару на четырёх matcher | LOAD-011, DEC-008 | cancelled |
| [LOAD-013](tickets/LOAD-013.md) | Сравнить готовые отчёты масштабирования | LOAD-003, LOAD-010, LOAD-011, LOAD-012 | cancelled |

## Контейнеры: не выполнять как одну задачу

| Исходный ID | Дочерние задачи | Статус |
| --- | --- | --- |
| [SUB-006](tickets/SUB-006.md) | [SUB-011](tickets/SUB-011.md), [SUB-012](tickets/SUB-012.md) | split |
| [SUB-008](tickets/SUB-008.md) | [SUB-013](tickets/SUB-013.md), [SUB-014](tickets/SUB-014.md) | split |
| [SUB-010](tickets/SUB-010.md) | [SUB-015](tickets/SUB-015.md), [SUB-016](tickets/SUB-016.md) | split |
| [MKT-003](tickets/MKT-003.md) | [MKT-009](tickets/MKT-009.md), [MKT-010](tickets/MKT-010.md) | split |
| [MKT-004](tickets/MKT-004.md) | [MKT-011](tickets/MKT-011.md), [MKT-012](tickets/MKT-012.md) | split |
| [MKT-007](tickets/MKT-007.md) | [MKT-013](tickets/MKT-013.md), [MKT-014](tickets/MKT-014.md) | split |
| [MKT-008](tickets/MKT-008.md) | [MKT-015](tickets/MKT-015.md), [MKT-016](tickets/MKT-016.md) | split |
| [MAT-003](tickets/MAT-003.md) | [MAT-007](tickets/MAT-007.md), [MAT-008](tickets/MAT-008.md), [MAT-009](tickets/MAT-009.md) | split |
| [MAT-006](tickets/MAT-006.md) | [MAT-010](tickets/MAT-010.md), [MAT-011](tickets/MAT-011.md) | split |
| [NTF-011](tickets/NTF-011.md) | [NTF-012](tickets/NTF-012.md), [NTF-013](tickets/NTF-013.md) | split |
| [OPS-005](tickets/OPS-005.md) | [OPS-012](tickets/OPS-012.md), [OPS-013](tickets/OPS-013.md) | split |
| [LOAD-005](tickets/LOAD-005.md) | [LOAD-010](tickets/LOAD-010.md), [LOAD-011](tickets/LOAD-011.md), [LOAD-012](tickets/LOAD-012.md), [LOAD-013](tickets/LOAD-013.md) | split |
| [OPS-017](tickets/OPS-017.md) | [OPS-019](tickets/OPS-019.md), [OPS-020](tickets/OPS-020.md), [OPS-021](tickets/OPS-021.md), [OPS-022](tickets/OPS-022.md) | split |
| [OPS-021](tickets/OPS-021.md) | [OPS-023](tickets/OPS-023.md), [OPS-024](tickets/OPS-024.md), [OPS-025](tickets/OPS-025.md), [OPS-026](tickets/OPS-026.md) | split |
| [OPS-030](tickets/OPS-030.md) | [OPS-031](tickets/OPS-031.md), [OPS-032](tickets/OPS-032.md) | split |
| [OPS-031](tickets/OPS-031.md) | [OPS-033](tickets/OPS-033.md), [OPS-034](tickets/OPS-034.md), [OPS-035](tickets/OPS-035.md), [OPS-036](tickets/OPS-036.md) | split |
| [OPS-037](tickets/OPS-037.md) | [OPS-038](tickets/OPS-038.md), [OPS-039](tickets/OPS-039.md), [OPS-040](tickets/OPS-040.md) | split |
| [OPS-059](tickets/OPS-059.md) | [OPS-060](tickets/OPS-060.md), [OPS-061](tickets/OPS-061.md), [OPS-062](tickets/OPS-062.md) | split |
