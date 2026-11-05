# Диагностика запуска OPS-040

Первый sandbox-запуск run-a не смог подключиться к Docker socket; Docker
доступен через разрешённый escalation. Контейнер не создавался.

Run-b: readiness polling `pg_isready` создавал дополнительные backend под
ptrace. Ordinary SQL получил timeout 60 s, полный capture не подтверждён.
[Исходный report](run-b-report.json) сохраняет команды, hashes partial raw,
источников и пустой cleanup. Частичные raw перенесены в локальный
`/tmp/binance-ops040-run-b`, в Git не включены; без этих локальных файлов
run-b невоспроизводим как сохранённый артефакт и не считается acceptance evidence.
Причина timeout не сведена к одному фактору; влияние polling видно по backend
процессам и startup logs. Runner теперь ждёт готовность по stderr PostgreSQL,
использует 2000 строк с настоящим spill и SQL timeout 180 s. Итоговый run-c
с чистым volume прошёл ordinary/parallel SQL и shutdown.
