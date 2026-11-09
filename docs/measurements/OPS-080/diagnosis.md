# OPS-080: ранний source startup, 9 ноября 2026

Историческая причина exit 1 из E2E-007 **не установлена**. Нынешние проверки
не воспроизвели первый startup failure; исправление его причины не заявляется.
Таймаут, конкуренция и SQL dataset остаются гипотезами.

## Что проверено

Исходное дерево содержало только прежние untracked исследования. OPS-062/067
подтверждены runtime source guards, source-bound SQL и готовыми runners.
Использованы исключительно уникальные disposable PostgreSQL/Kafka проекты;
рабочие БД/topics не менялись. Тесты не запускались параллельно внутри пакета.

При первичной диагностике stderr временно сохранялся в приватный файл 0600,
а `run.record` временно писал исходные component errors. В вывод исследования
извлекались только совпадения с известными константами, grpc codes и SQLSTATE.
Эта временная запись удалена из кода; приватный файл удалён после исследования.
Сырые ошибки и credentials в репозитории не сохранялись.

| Проверка исходного поведения с временной диагностикой | Результат |
| --- | --- |
| Только TestMatcherSourceProcess | PASS, 39.87 s |
| Полный cmd/matcher: TestMatcherProcess | PASS, 90.51 s; graceful 29.74 s |
| Следующий TestMatcherSourceProcess в том же полном пакете | PASS, 39.23 s |

Отдельный и полный runners выполнялись на разных проектах с перекрытием
по времени. Это не доказывает отсутствие влияния более высокой конкуренции
в E2E-007, но гипотеза «после recovery всегда ломается dataset» не подтвердилась.
В обоих случаях cleanup registry/private inputs/containers/volumes/networks PASS.
Сохранённый `/tmp/E2E-007-matcher.log` подтверждает прежнюю ошибку harness,
но не содержит stderr child и не позволяет восстановить исходную причину.

## Подтверждённый пробел диагностики

`apiprocess.With` отбрасывал stderr полностью. `cmd/matcher.run` сохранял только
source-категории; для других dependency failures возвращал общий `required
component stopped`. Поэтому очередной ранний exit мог снова не дать причины.

Изменения текущего рабочего дерева:

- Существующий harness принимает доверенный `Config.DiagnosticLines`: до 32
  фиксированных строк по 256 bytes. В ErrEarlyExit входят exit code и только
  точные полные совпадения, по одному разу. Остальные строки, длинные строки,
  незавершённые строки и stdout не возвращаются. Буфер ограничен одной строкой.
  По умолчанию весь child output по-прежнему отбрасывается.
- Matcher классифицирует известные snapshot/live/price/fencing ошибки и
  RPC/SQL/deadline в фиксированные сообщения. SQL messages, RPC descriptions
  и неизвестный текст не возвращаются. Неизвестный отказ остаётся общей
  категорией и ещё потребует локализации. Семантика shutdown/source guards
  не изменялась, TTL/budgets не увеличивались.
- Process-тесты matcher включают этот allowlist. Source process дополнительно
  требует точные legacy/mismatch категории при ожидаемых отказах; assertions
  readiness, source identity, recreate, SQL history/scopes/grants сохранены.

Red harness: после добавления только поля Config, но до collector,
`go test ./internal/testutil/apiprocess -run '^TestEarlyExitDiagnostics$'`
FAIL: exit status 23 без `safe startup category`. Отдельная предварительная
попытка в sandbox дала ErrStart из-за запрета loopback и Red не считается.
После разрешённого запуска наблюдалось именно отсутствие категории.
Green/race пакетов apiprocess/cmd/matcher PASS. Проверяется также отсутствие
DSN/secret, exact matching, split writes, duplicate, oversized/default discard
и bounds конфигурации.

Red classifier: `go test ./cmd/matcher -run '^TestComponentDiagnostic$'`
FAIL: пустая категория вместо Latest unavailable. Green PASS: known error,
joined cancellation, snapshot, RPC deadline/unavailable, SQL permission/other,
unknown secret и обычная cancellation. Это проверка диагностики, а не
доказательство причины прежнего случайного startup failure.

## Команды

Все Go-команды: `GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local`.

Полный процессный runner:

```sh
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py --package ./cmd/matcher
```

Для отдельного сценария используется тот же runner, с временным Python wrapper,
который добавляет Go-фильтр только к тесту выбранного пакета; инфраструктура
и finally cleanup остаются штатными:

```sh
GOCACHE=/tmp/binance-go-build python3 - --package ./cmd/matcher <<'PY'
import sys
sys.path.insert(0, 'scripts')
import check_api_process as runner
original = runner.run
def targeted(command, env, **kwargs):
    if command[:2] == ['go', 'test'] and command[-1] == './cmd/matcher':
        command = command[:-1] + ['-run', '^TestMatcherSourceProcess$', command[-1]]
    return original(command, env, **kwargs)
runner.run = targeted
runner.main(owner='matcher')
PY
```

Обычные `go test -race ./...`, `go vet ./...`, `go build ./...`,
`python3 scripts/generate_proto.py --check` и
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s infra/deploy -p test_observe.py -v`
— PASS. Protobuf current; четыре deployment unit tests PASS.

Финальный полный runner после изменений диагностики — exit 0:
TestMatcherProcess PASS 92.57 s (graceful 29.84 s), следующий
TestMatcherSourceProcess PASS 39.82 s. Он также проверил точные legacy/mismatch
категории при отказах. Cleanup registry/private inputs и обоих Compose
проектов/volumes/networks PASS. `gofmt -l .` пуст, `git diff --check` и
`python3 scripts/backlog.py check` PASS (184 done, 1 in-progress, 1 planned).
Неожиданный ранний exit 1 не повторился и в этом запуске.

## Продолжение проверки

Продолжение 9 ноября: diff и доступный исходный лог повторно сверены;
runtime/fixture не менялись. Выполнены:

```sh
GOCACHE=/tmp/binance-go-build GOTOOLCHAIN=local go test -race ./internal/testutil/apiprocess ./cmd/matcher
GOCACHE=/tmp/binance-go-build python3 scripts/check_matcher_process.py --package ./cmd/matcher
```

Целевые пакеты PASS (4.097 s и 2.683 s; opt-in только во второй команде).
Полный runner exit 0: TestMatcherProcess 90.21 s (graceful 29.79 s),
TestMatcherSourceProcess 40.10 s. Cleanup registry/private inputs и обоих
Compose проектов/containers/volumes/networks PASS. Локальный вывод:
`/tmp/OPS-080-resume-matcher.log`. Первоначальный sandbox запуск завершился
PermissionError до тестов; разрешённый повтор выше прошёл. Повторных общих
CI без изменения кода не было; их прежние результаты указаны выше.
`git diff --check` и `python3 scripts/backlog.py check` PASS.
Новых фактов о причине исторического сбоя нет.

## Согласованное завершение — 9 ноября 2026

Пользователь согласовал принять безопасную диагностику и успешную регрессию,
оставив историческую причину неизвестной. Критерии карточки обновлены; OPS-080
завершён. После согласования менялись только документы, suite не повторялся.
Успех нынешних запусков не устанавливает причину E2E-007 и не доказывает её
исправление. Guards/timeouts сохранены. При новом отказе штатной проверки
сохранить безопасную категорию и завести узкий тикет с воспроизведением;
дополнительные прогоны ради исторического отказа без новых фактов не нужны.
