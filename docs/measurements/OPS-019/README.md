# Контрольные измерения OPS-019

4 ноября 2026. PostgreSQL 18.6, linux/arm64, закреплённый Compose image/digest;
лимиты 1 CPU, 1 GiB RAM, Docker Desktop. Это проверка collector, не квалификация
бизнес-стоимости/нагрузки или 200 GiB профиля DEC-008.

Команды воспроизведения и семантика метрик:
[инструкция collector](../../../infra/postgres/measure/README.md).
В каждом каталоге — `report.json`, семь сырых JSON.gz снимков четырёх БД
(warmup, before/after/checkpoint для empty и adapter), четыре schema-only dump.
Все три опыта используют разные новые volumes; интервалы измерений последовательны.
Failure — четвёртый отдельный volume, ошибка сразу после up.

```sh
python3 scripts/measure_postgres.py --output docs/measurements/OPS-019/run-1000-a --rows 1000
python3 scripts/measure_postgres.py --output docs/measurements/OPS-019/run-1000-b --rows 1000
python3 scripts/measure_postgres.py --output docs/measurements/OPS-019/run-2000 --rows 2000
python3 scripts/measure_postgres.py --output docs/measurements/OPS-019/failure --fail-after-up
python3 scripts/verify_postgres_measurement.py docs/measurements/OPS-019
```

Первые три exit 0; failure ожидаемо exit 1; verifier exit 0.
Для повторения выбрать новые output directories; существующие не перезаписываются.
Verifier сверяет runtime-роль/число строк, единичный учёт relation/fork, durability,
reset stamps, числовые дельты с raw, одинаковые hashes collector и текущие файлы,
различные проекты, отсутствие ресурсов после успеха/ошибки и различимость сигнала.
Машинная сводка — [summary.json](summary.json).

| Опыт | Строк | WAL insert, байт | API relations, байт | WAL segment pool Δ, байт | Client WAL fsync, count / ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Empty (каждый опыт) | 0 | 264 | 0 | 0 | 0 / 0 |
| Adapter A | 1000 | 453504 | 286720 | 0 | 1000 / 78.052 |
| Adapter B, новый volume | 1000 | 453528 | 286720 | 0 | 1000 / 79.529 |
| Adapter C, новый volume | 2000 | 900856 | 516096 | 0 | 2000 / 125.407 |

Показаны inclusive окна, включая финальный checkpoint. Числа не очищены
вычитанием empty. Разброс WAL двух одинаковых входов 24 байта; увеличение входа
дало +447328 байт относительно большего из них. Рост API relations отличим от
нулевого empty, но квантован страницами: удвоение строк не требует точного
удвоения выделенных байт. Нулевой рост пула WAL не означает нулевую стоимость:
сегменты уже выделены, insert LSN растёт внутри них. Raw и report отдельно
хранят heap/index/TOAST forks, temp и все backend IO, checkpoint/bgwriter.

В разведочном проходе без warmup empty содержал 341536 байт WAL от первых
диагностических чтений/инициализации; он не смешан с финальными контролями.
После добавления сохраняемого warmup пустой контроль стабилен на 264 байтах.
Первоначальный запуск без доступа sandbox к Docker не стартовал; измерения
выполнены с разрешённым доступом к Docker. Ошибки не объявлены успешными опытами.

Все cleanup lists (container/volume/network) пустые, в том числе после injected
failure. Удалены только уникальные disposable ресурсы; рабочий/load volume не
адресовались. Credentials не сохранялись.

Ограничения: задержки cumulative stats, последовательные окна снимков,
фоновые записи и влияние диагностики; файловые длины вместо filesystem allocated
blocks/устройственных writes; transient temp peak и device fsync latency —
unsupported и блокируют соответствующую квалификацию. NULL IO cells также
сохраняются как unsupported, а не ноль. На этих controls нет temp spill:
нулевой temp_bytes не доказывает бесплатный максимальный batch.
Report остаётся `collected-not-qualified`. Таблица/квоты, manifests, Kafka и ledger
этим тикетом не реализованы.
