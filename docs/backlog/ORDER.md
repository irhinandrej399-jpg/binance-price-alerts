# Очередь выполнения тикетов

Единый маршрут для одного агента. `next` продолжает начатую работу, затем берёт
первую незавершённую карточку; planned проверяется перед ready. `done`
пропускается, `split` и `cancelled` не входят в очередь. История отменённых
карточек остаётся в индексе, отмена не означает прохождение критериев.

По [DEC-011](../decisions/DEC-011.md) цель — рабочий production ready
пет-проект без тяжёлой нагрузочной квалификации. Нужны корректность,
безопасный запуск, реальные интеграции, backup/restore и базовое наблюдение.
Milestone E2E-001 подтвердил fake-сценарий; он не подтверждает готовность
публичного deployment. Следующий этап заканчивается E2E-006.

`python3 scripts/backlog.py next --limit 5` — предварительный просмотр, не
поручение выполнить пять задач. `check` проверяет статусы, индекс, полноту
очереди и порядок зависимостей. Менять их вместе при разделении задачи.
Прямое поручение имеет приоритет; автоматически к следующей задаче не переходить.

## Выполненный этап

1. [DEC-001](tickets/DEC-001.md) — Определить точную семантику достижения цены.
2. [DEC-002](tickets/DEC-002.md) — Установить критерий свежести цены.
3. [DEC-003](tickets/DEC-003.md) — Зафиксировать владельцев данных четырёх сервисов.
4. [DEC-004](tickets/DEC-004.md) — Определить границу активации подписки; после DEC-001/002/003, последняя цена и проверка перед отправкой, пересмотр исходного условия.
5. [DEC-005](tickets/DEC-005.md) — Определить поведение без привязанного Telegram.
6. [DEC-006](tickets/DEC-006.md) — Выбрать постоянное хранилище.
7. [DEC-007](tickets/DEC-007.md) — Выбрать брокер событий.
8. [DEC-008](tickets/DEC-008.md) — Зафиксировать воспроизводимый профиль нагрузки; также предпосылка SET-003/005: ресурсы, durability, фиксированные партиции и бюджеты.
9. [DEC-009](tickets/DEC-009.md) — Закрепить транспорт: gRPC внутри, REST/JSON снаружи, Kafka для событий.
10. [DEC-010](tickets/DEC-010.md) — Рабочий локальный профиль перед квалификацией нагрузки.
11. [SET-001](tickets/SET-001.md) — Создать каркас Go-репозитория.
12. [SET-002](tickets/SET-002.md) — Добавить проверку Go в CI.
13. [DOM-001](tickets/DOM-001.md) — Ввести точный тип цены.
14. [DOM-002](tickets/DOM-002.md) — Преобразовать процент в фиксированный порог.
15. [DOM-003](tickets/DOM-003.md) — Реализовать предикат срабатывания.
16. [DOM-004](tickets/DOM-004.md) — Реализовать одноразовый жизненный цикл.
17. [DOM-005](tickets/DOM-005.md) — Реализовать проверку свежести котировки.
18. [SET-003](tickets/SET-003.md) — Поднять локальное хранилище.
19. [SET-004](tickets/SET-004.md) — Добавить запуск миграций.
20. [SET-005](tickets/SET-005.md) — Поднять локальный брокер.
21. [CON-001](tickets/CON-001.md) — Определить общий конверт события.
22. [CON-002](tickets/CON-002.md) — Определить событие изменения подписки.
23. [CON-003](tickets/CON-003.md) — Определить событие рыночной цены.
24. [CON-004](tickets/CON-004.md) — Определить событие срабатывания.
25. [SET-006](tickets/SET-006.md) — Подготовить подключение и интеграционные проверки Go с PostgreSQL.
26. [IAM-001](tickets/IAM-001.md) — Сохранить учётную запись пользователя.
27. [IAM-002](tickets/IAM-002.md) — Реализовать хранение и проверку пароля.
28. [IAM-003](tickets/IAM-003.md) — Реализовать регистрацию по email.
29. [IAM-004](tickets/IAM-004.md) — Реализовать вход с ограниченным сроком сессии.
30. [IAM-005](tickets/IAM-005.md) — Проверять сессию на защищённом endpoint.
31. [IAM-006](tickets/IAM-006.md) — Выдать одноразовый код привязки Telegram.
32. [IAM-007](tickets/IAM-007.md) — Проверять подлинность Telegram webhook.
33. [IAM-008](tickets/IAM-008.md) — Поглотить код и привязать Telegram chat.
34. [MKT-001](tickets/MKT-001.md) — Получить каталог доступных spot-пар USDT.
35. [MKT-002](tickets/MKT-002.md) — Декодировать одно событие сделки Binance.
36. [MKT-009](tickets/MKT-009.md) — Прочитать цены по одному WebSocket-соединению.
37. [MKT-010](tickets/MKT-010.md) — Ограничить управляющие команды WebSocket.
38. [MKT-011](tickets/MKT-011.md) — Переподключиться после обрыва WebSocket.
39. [MKT-005](tickets/MKT-005.md) — Публиковать только монотонные цены пары.
40. [MKT-012](tickets/MKT-012.md) — Планово заменить WebSocket-соединение.
41. [MKT-017](tickets/MKT-017.md) — Сохранить котировку и работу публикации атомарно.
42. [SET-007](tickets/SET-007.md) — Подготовить генерацию Protobuf и сетевые проверки unary gRPC.
43. [SET-008](tickets/SET-008.md) — Подготовить сервисные полномочия внутренних gRPC-вызовов.
44. [MKT-006](tickets/MKT-006.md) — Выдать последнюю известную котировку с возрастом.
45. [SET-009](tickets/SET-009.md) — Подготовить Go-клиент и транзакционные проверки Kafka.
46. [MKT-013](tickets/MKT-013.md) — Опубликовать одну цену с подтверждением брокера.
47. [MKT-014](tickets/MKT-014.md) — Ограничить буфер принятых цен.
48. [MKT-015](tickets/MKT-015.md) — Построить план уникальных рыночных потоков.
49. [MKT-016](tickets/MKT-016.md) — Применить план потоков к действующим соединениям.
50. [SUB-001](tickets/SUB-001.md) — Сохранить подписку с версией состояния.
51. [SUB-002](tickets/SUB-002.md) — Обеспечить лимит 15 при конкурентном создании.
52. [MKT-018](tickets/MKT-018.md) — Выдать каталог пар через внутренний gRPC.
53. [SUB-003](tickets/SUB-003.md) — Создать подписку через HTTP.
54. [SUB-004](tickets/SUB-004.md) — Вернуть активные подписки владельца.
55. [SUB-005](tickets/SUB-005.md) — Отключить свою активную подписку (после квоты SUB-002).
56. [SUB-021](tickets/SUB-021.md) — Добавить транзакционный журнал изменений API.
57. [SUB-011](tickets/SUB-011.md) — Сохранить outbox при создании подписки.
58. [SUB-012](tickets/SUB-012.md) — Сохранить outbox при отмене подписки.
59. [SUB-007](tickets/SUB-007.md) — Опубликовать subscription outbox.
60. [SUB-013](tickets/SUB-013.md) — Сохранить атомарный запрос работы доставки.
61. [SUB-014](tickets/SUB-014.md) — Открыть внутренний запрос работы доставки.
62. [SUB-017](tickets/SUB-017.md) — Сохранить допуск одной попытки доставки.
63. [SUB-018](tickets/SUB-018.md) — Открыть допуск доставки с проверкой последней цены.
64. [SUB-019](tickets/SUB-019.md) — Сохранить исход попытки в подписке.
65. [SUB-020](tickets/SUB-020.md) — Открыть фиксацию исхода доставки.
66. [SUB-009](tickets/SUB-009.md) — Опубликовать outbox срабатываний.
67. [SUB-015](tickets/SUB-015.md) — Прочитать согласованный снимок из хранилища.
68. [SUB-023](tickets/SUB-023.md) — Создать снимок идемпотентно по request ID.
69. [SUB-016](tickets/SUB-016.md) — Открыть внутренний интерфейс страниц снимка.
70. [SUB-022](tickets/SUB-022.md) — Открыть защищённый SQL-replay подписок.
71. [SET-010](tickets/SET-010.md) — Подготовить PostgreSQL harness matcher.
72. [MAT-001](tickets/MAT-001.md) — Применить версионированное изменение подписки.
73. [MAT-002](tickets/MAT-002.md) — Находить кандидатов в индексе порогов.
74. [MAT-013](tickets/MAT-013.md) — Подготовить источник ценовых обновлений matcher.
75. [MAT-012](tickets/MAT-012.md) — Подключить подписку к последней цене.
76. [MAT-007](tickets/MAT-007.md) — Загрузить исходный снимок в проекцию matcher.
77. [MAT-014](tickets/MAT-014.md) — Подготовить durable проекцию снимка для SQL-replay.
78. [MAT-015](tickets/MAT-015.md) — Подготовить Kafka-источник изменений подписок.
79. [MAT-008](tickets/MAT-008.md) — Догнать изменения и переключить matcher в live.
80. [MAT-009](tickets/MAT-009.md) — Возобновить проекцию с сохранённого cursor.
81. [MAT-016](tickets/MAT-016.md) — Сохранить durable результат обработки цены.
82. [SUB-025](tickets/SUB-025.md) — Передать владельца подписки в live-проекцию matcher.
83. [MAT-004](tickets/MAT-004.md) — Обработать цену и запросить фиксацию кандидатов.
84. [MAT-005](tickets/MAT-005.md) — Восстановить обработку цены после частичного сбоя.
85. [MAT-017](tickets/MAT-017.md) — Защитить SQL-записи matcher fencing token.
86. [MAT-018](tickets/MAT-018.md) — Подготовить групповой источник цен matcher.
87. [MAT-019](tickets/MAT-019.md) — Ограничить live-подключения назначенными ценовыми партициями.
88. [MAT-010](tickets/MAT-010.md) — Назначить партиции двум работающим matcher; интеграция готовых SQL fencing и consumer group.
89. [MAT-011](tickets/MAT-011.md) — Передать партицию после отказа matcher.
90. [NTF-001](tickets/NTF-001.md) — Создать durable задание из срабатывания.
91. [NTF-002](tickets/NTF-002.md) — Сформировать текст Telegram-уведомления.
92. [NTF-003](tickets/NTF-003.md) — Вызвать Telegram sendMessage.
93. [NTF-004](tickets/NTF-004.md) — Ограничить отправку по боту.
94. [NTF-005](tickets/NTF-005.md) — Ограничить отправку в отдельный чат.
95. [NTF-006](tickets/NTF-006.md) — Назначить повтор по retry_after и временной ошибке.
96. [NTF-009](tickets/NTF-009.md) — Выдать worker задание с ограниченной арендой.
97. [NTF-010](tickets/NTF-010.md) — Сохранить успешную доставку.
98. [NTF-007](tickets/NTF-007.md) — Обработать неоднозначный timeout отправки.
99. [NTF-008](tickets/NTF-008.md) — Завершить задание при постоянной ошибке.
100. [NTF-016](tickets/NTF-016.md) — Сохранить временный отказ и обязанность отчёта.
101. [SUB-026](tickets/SUB-026.md) — Сохранить блокировку после промежуточного исхода той же попытки.
102. [NTF-014](tickets/NTF-014.md) — Надёжно передать исход попытки владельцу подписки.
103. [NTF-015](tickets/NTF-015.md) — Возобновить задание после восстановления Telegram.
104. [SUB-027](tickets/SUB-027.md) — Подготовить адресата до лимитеров и проверить его при допуске.
105. [NTF-012](tickets/NTF-012.md) — Выполнить одну попытку обработки готового задания.
106. [NTF-013](tickets/NTF-013.md) — Запустить ограниченный цикл notification worker.
107. [OPS-019](tickets/OPS-019.md) — Собрать физические метрики PostgreSQL на disposable стенде.
108. [OPS-020](tickets/OPS-020.md) — Собрать физические метрики Kafka на disposable стенде.
109. [OPS-023](tickets/OPS-023.md) — Подключить внешние фазы к совместному измерительному runner.
110. [OPS-024](tickets/OPS-024.md) — Измерить manifest операций владельца API.
111. [OPS-025](tickets/OPS-025.md) — Измерить продолжение API в matcher.
112. [OPS-028](tickets/OPS-028.md) — Подготовить API fixture продолжения notifications с исходными ID.
113. [OPS-026](tickets/OPS-026.md) — Измерить продолжение API в notifications и свести цепочку.
114. [OPS-022](tickets/OPS-022.md) — Измерить manifest источника market-data и свести покрытие затрат.
115. [OPS-029](tickets/OPS-029.md) — Измерить количество и время fsync процесса Kafka.
116. [OPS-033](tickets/OPS-033.md) — Сохранять и проверять передачу FD через ancillary data.
117. [OPS-038](tickets/OPS-038.md) — Сохранять независимый census и полные inode inventories.
118. [OPS-039](tickets/OPS-039.md) — Сохранять lifecycle evidence при shared state и nonleader exec.
119. [OPS-040](tickets/OPS-040.md) — Проверить reference witnesses и повторить PostgreSQL capture.
120. [OPS-041](tickets/OPS-041.md) — Сохранить shared-fileset PG spill в новом raw-контракте.
121. [OPS-042](tickets/OPS-042.md) — Определить доказуемый lifetime envelope и capture предпосылки.
122. [OPS-001](tickets/OPS-001.md) — Добавить health и readiness API.
123. [OPS-045](tickets/OPS-045.md) — Подготовить наблюдения границ задержки цена → durable workflow.
124. [OPS-002](tickets/OPS-002.md) — Измерять задержку от цены до trigger.
125. [OPS-003](tickets/OPS-003.md) — Измерять состояние очереди доставки.
126. [OPS-047](tickets/OPS-047.md) — Подготовить disposable harness отдельного процесса API.
127. [OPS-046](tickets/OPS-046.md) — Собрать минимальный процесс API для отмены и relay.
128. [OPS-004](tickets/OPS-004.md) — Корректно остановить API.
129. [OPS-012](tickets/OPS-012.md) — Ограничить размер тела публичного запроса.
130. [OPS-013](tickets/OPS-013.md) — Ограничить частоту входа и регистрации между репликами.
131. [OPS-006](tickets/OPS-006.md) — Добавить health и readiness market-data.
132. [OPS-007](tickets/OPS-007.md) — Добавить health и readiness matcher.
133. [OPS-008](tickets/OPS-008.md) — Добавить health и readiness notifications.
134. [OPS-049](tickets/OPS-049.md) — Подготовить disposable окружение процесса market-data.
135. [OPS-050](tickets/OPS-050.md) — Открыть границу каждой попытки WebSocket reconnect.
136. [OPS-051](tickets/OPS-051.md) — Настроить стартовую HTTP-пробу process harness.
137. [OPS-048](tickets/OPS-048.md) — Собрать минимальный процесс market-data.
138. [OPS-009](tickets/OPS-009.md) — Корректно остановить market-data.
139. [OPS-052](tickets/OPS-052.md) — Подготовить disposable окружение процесса matcher.
140. [OPS-053](tickets/OPS-053.md) — Собрать минимальный процесс matcher.
141. [OPS-010](tickets/OPS-010.md) — Корректно остановить matcher.
142. [OPS-054](tickets/OPS-054.md) — Подготовить disposable окружение процесса notifications.
143. [OPS-055](tickets/OPS-055.md) — Собрать минимальный процесс notifications.
144. [OPS-011](tickets/OPS-011.md) — Корректно остановить notification worker.
145. [OPS-060](tickets/OPS-060.md) — Проверить поколение Kafka-источника на входах matcher.
146. [OPS-061](tickets/OPS-061.md) — Сохранить source binding Kafka checkpoints matcher в SQL.
147. [OPS-063](tickets/OPS-063.md) — Проверить provenance prices scope до первого transport checkpoint.
148. [OPS-062](tickets/OPS-062.md) — Подключить source binding к runtime и восстановлению matcher.
149. [OPS-056](tickets/OPS-056.md) — Экспортировать durable Kafka-прогресс matcher.
150. [OPS-064](tickets/OPS-064.md) — Сохранить source binding intake notifications в SQL.
151. [OPS-065](tickets/OPS-065.md) — Проверить поколение Kafka-источника на входе notifications.
152. [OPS-066](tickets/OPS-066.md) — Подключить source binding к runtime notifications.
153. [OPS-057](tickets/OPS-057.md) — Экспортировать durable Kafka-прогресс notifications.
154. [OPS-068](tickets/OPS-068.md) — Управлять отказами OffsetCommit и DeleteRecords в Kafka fault proxy.
155. [OPS-058](tickets/OPS-058.md) — Подготовить disposable окружение проверки Kafka cleaner.
156. [OPS-014](tickets/OPS-014.md) — Удалить безопасный префикс журнала Kafka.
157. [E2E-003](tickets/E2E-003.md) — Поднять управляемый fake Binance для стенда.
158. [E2E-004](tickets/E2E-004.md) — Поднять управляемый fake Telegram для стенда.
159. [OPS-069](tickets/OPS-069.md) — Подключить Catalog и Latest RPC к процессу market-data.
160. [OPS-070](tickets/OPS-070.md) — Подключить RPC matcher к процессу API.
161. [OPS-071](tickets/OPS-071.md) — Подключить RPC notifications к процессу API.
162. [OPS-072](tickets/OPS-072.md) — Подключить Telegram-привязку к процессу API.
163. [OPS-073](tickets/OPS-073.md) — Подключить создание и список подписок к процессу API.
164. [E2E-005](tickets/E2E-005.md) — Подготовить общее disposable окружение четырёх сервисов.
165. [E2E-002](tickets/E2E-002.md) — Запустить четыре процесса в одном локальном стенде.
166. [E2E-001](tickets/E2E-001.md) — Проверить один полный пользовательский сценарий.
167. [DEC-011](tickets/DEC-011.md) — Упростить выпуск пет-проекта до практической production готовности.

## Доведение до реального выпуска

168. [NTF-017](tickets/NTF-017.md) — Закрыть изменение исторических свидетельств ролью notifications.
169. [NTF-018](tickets/NTF-018.md) — Исследовать сбой восстановления после потери ответа регистрации.
170. [SUB-024](tickets/SUB-024.md) — Исследовать нестабильный expiry в SQL snapshot RPC тестах.
171. [NTF-019](tickets/NTF-019.md) — Устранить ложный отказ проверки Telegram payload по подстроке возраста.
172. [NTF-020](tickets/NTF-020.md) — Исследовать состояние bootstrap после отмены SQL.
173. [OPS-067](tickets/OPS-067.md) — Исследовать нестабильную metadata после recreate в changefeed fixture.
174. [MKT-020](tickets/MKT-020.md) — Добавить lifecycle попытки в stream reconciler.
175. [E2E-008](tickets/E2E-008.md) — Расширить fake Binance для плана каталога.
176. [MKT-019](tickets/MKT-019.md) — Подключить каталог торговых пар к runtime market-data.
177. [OPS-078](tickets/OPS-078.md) — Подготовить явный source provisioning постоянного стенда.
178. [E2E-009](tickets/E2E-009.md) — Разрешить явный listener существующих vendor fake.
179. [OPS-074](tickets/OPS-074.md) — Собрать постоянный запуск четырёх сервисов.
180. [OPS-075](tickets/OPS-075.md) — Защитить публичный вход и секреты развёртывания.
181. [OPS-076](tickets/OPS-076.md) — Подготовить резервную копию и восстановление стенда.
182. [OPS-079](tickets/OPS-079.md) — Подключить метрики очереди к процессу notifications.
183. [OPS-077](tickets/OPS-077.md) — Оформить наблюдение и обслуживание постоянного стенда.
184. [E2E-007](tickets/E2E-007.md) — Проверить восстановление малого рабочего стенда.
185. [OPS-080](tickets/OPS-080.md) — Исследовать ранний выход matcher в source process fixture.
186. [E2E-006](tickets/E2E-006.md) — Подтвердить реальный запуск и доставку перед выпуском.
