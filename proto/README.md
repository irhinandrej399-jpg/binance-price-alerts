# Внутренние unary gRPC-контракты

Основа SET-007 следует [DEC-009](../docs/decisions/DEC-009.md). Схемы находятся
в `proto/<service>/v1/`, имя Protobuf package включает сервис и версию.
`go_package` указывает на `internal/gen/<service>/v1`; `*.pb.go` коммитятся
и изменяются только генерацией. Удалённые номера и имена полей резервируются,
несовместимое изменение требует новой версии. Цены передаются точно, без float/double.

Из чистого checkout нужны Go из `.go-version`, Python 3 и доступ к GitHub,
Go module proxy и checksum database. Глобальный `protoc` и Go-плагины не нужны:

```sh
python3 scripts/generate_proto.py
python3 scripts/generate_proto.py --check
```

Скрипт загружает официальный `protoc` **36.2**, проверяет SHA-256 архива,
устанавливает во временный каталог `protoc-gen-go` **v1.36.12** и
`protoc-gen-go-grpc` **v1.6.2**, вызывает их по абсолютным путям и удаляет
временные инструменты. Поддерживаются macOS/Linux на arm64 и x86_64.
Go-библиотеки: `google.golang.org/grpc` **v1.85.0** и
`google.golang.org/protobuf` **v1.36.12**; зависимости и checksums — в go.mod/go.sum.
Go проверяет скачанные модули штатным механизмом checksum database.
Версии скрипта меняются явно вместе с проверенной перегенерацией.
Сейчас схемы не импортируют well-known types, поэтому извлекается только бинарник protoc.

`--check` генерирует во временный каталог, сравнивает имена и содержимое всех
`*.pb.go` в `internal/gen` (включая лишние и отсутствующие файлы) и завершается
с ошибкой при расхождении, не изменяя checkout. Этот шаг включён в Go CI.
Обычная генерация также удаляет устаревшие `*.pb.go`; рукописный код размещается
вне `internal/gen`. Повторный запуск даёт те же файлы.

`probe/v1` — только неприкладной fixture транспорта, не API и не health endpoint.
Он не регистрируется в `cmd/*`. Проверки используют настоящий TCP loopback,
без БД, Kafka и live сервисов:

```sh
go test -race ./internal/testutil/grpctest -count=1 -timeout=60s
```

`grpctest.Start(t, register, serverOptions...)` возвращает `*grpc.ClientConn`,
поднимает сервер на `127.0.0.1:0`, закрывает клиент и listener, останавливает
сервер и ожидает Serve через `t.Cleanup`. В register передаётся функция
регистрации конкретного сгенерированного сервиса. Каждый RPC задаёт конечный
deadline через `context.WithTimeout`; сервер использует переданный context
для вложенной работы и отмены. Helper намеренно использует insecure transport
только на loopback в тестах; это не конфигурация runtime.

Прикладная схема `marketdata/v1/latest.proto` и adapter последней котировки
готовы в [MKT-006](../internal/marketdata/latest/README.md), используя эти генерацию и helper. Каталог `marketdata/v1/catalog.proto` и Go-клиент/сервер готовы в [MKT-018](../internal/marketdata/catalogrpc/README.md). Сервисные полномочия SET-008 готовы:
[mTLS и права unary RPC](../internal/platform/serviceauth/README.md).
Для защищённых проверок `grpctest.StartWithClientOptions(t, register,
clientOptions, serverOptions...)` принимает явные клиентские credentials и options
сервера при том же lifecycle loopback. Прикладные retries, бюджет deadline,
runtime и отображение доменных ошибок определяются в интерфейсных тикетах.

Источники инструментов: [gRPC Go quick start](https://grpc.io/docs/languages/go/quickstart/),
[protoc 36.2](https://github.com/protocolbuffers/protobuf/releases/tag/v36.2),
[gRPC Go](https://github.com/grpc/grpc-go),
[Protobuf Go](https://github.com/protocolbuffers/protobuf-go).

Запрос работы доставки API: `api/v1/delivery.proto`, matcher-only mTLS и
[клиент/сервер SUB-014](../internal/api/deliveryrpc/README.md).

Допуск попытки notifications: `api/v1/admission.proto`, notifications-only mTLS,
актуальная цена через MKT-006 и [клиент/сервер SUB-018](../internal/api/admissionrpc/README.md).
Схема импортирует `marketdata/v1/latest.proto`, чтобы сохранить точный полный
снимок котировки в разрешении; Kafka-контракты остаются JSON.

Фиксация исхода notifications: `api/v1/result.proto`, notifications-only mTLS и
[клиент/сервер SUB-020](../internal/api/resultrpc/README.md) поверх SUB-019.
Повторяется исход известного attempt; ошибка RPC не разрешает повтор Telegram.
