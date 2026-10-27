# Сервисные полномочия unary gRPC

Контракт принят в [SET-008](../../../docs/decisions/SET-008.md) по
[DEC-009](../../../docs/decisions/DEC-009.md). Общая основа не регистрирует
прикладные RPC и не запускает runtime.

mTLS использует TLS 1.3 и явно заданные доверенные CA с обеих сторон.
Сервер требует сертификат клиента с назначением ClientAuth; клиент проверяет
цепочку и имя сервера (DNS SAN, назначение ServerAuth). Системное хранилище CA
не используется. Идентичность клиента берётся из единственного URI SAN
проверенного листового сертификата. CN и metadata, включая пользовательский
Bearer, не дают сервисных прав. URI должен иметь scheme и host без userinfo,
query и fragment; сравнение идентичностей точное, регистрозависимое.

Политика `map[string][]string` сопоставляет URI с полными именами unary RPC
`/package.v1.Service/Method`. Конструктор копирует политику; её последующее
изменение вызывающим кодом не меняет действующий сервер. Wildcard не принимается,
пустая политика запрещает всё. Только подписанный сертификат без явного права
не разрешает вызов. Неизвестные RPC и все streaming-вызовы запрещены отдельными
handlers/interceptors, даже если ошибочно перечислены в политике.

Пример подключения будущим сервисом (PEM прочитаны из внешних файлов):

```go
options, err := serviceauth.ServerOptions(serverCertPEM, serverKeyPEM, clientCAPEM,
    map[string][]string{
        apiServiceURI: {generated.Service_Method_FullMethodName},
    })
if err != nil {
    return err
}
server := grpc.NewServer(options...)
// Зарегистрировать здесь сгенерированный сервис с прикладным adapter.

creds, err := serviceauth.ClientCredentials(clientCertPEM, clientKeyPEM, serverCAPEM)
if err != nil {
    return err
}
conn, err := grpc.NewClient(target,
    grpc.WithTransportCredentials(creds),
    grpc.WithAuthority(expectedServerDNSName))
// Проверить err, закрыть conn при остановке.
// Каждый RPC выполняется с context.WithTimeout и defer cancel().
```

`generated.Service_Method_FullMethodName` — условное имя: брать константу
конкретного generated-контракта. У probe есть `Probe_Call_FullMethodName`,
но probe не подключается в runtime. TLS options, проверку прав и
UnknownServiceHandler из ServerOptions устанавливать вместе и не переопределять;
другие middleware подключать через ChainUnaryInterceptor после проверки прав.
Они должны передавать исходный context. В режиме TLS passthrough mTLS доходит до
самого сервиса; TLS termination на proxy с неподтверждёнными заголовками не подходит.

`expectedServerDNSName` должен быть фиксирован доверенной конфигурацией, а не
пользовательским вводом. Имя проверяется даже при соединении с IP или alias
через resolver. Не использовать InsecureSkipVerify. Политика MKT-006 и её
бюджеты времени задаются в MKT-006, а не в общей библиотеке.

Отсутствующий/недоверенный/истёкший сертификат отклоняется при handshake;
клиент обычно получает Unavailable. Непригодная URI-идентичность после handshake
даёт Unauthenticated; отсутствие права — PermissionDenied. При таких отказах
прикладной handler не исполняется. Ошибки конфигурации фиксированные, без PEM,
ключей, идентичностей и исходных ошибок парсера. Библиотека ничего не логирует.

Пути к CA, сертификату и ключу, ожидаемое имя сервера и URI-права должны быть
заданы отдельно для каждого окружения. Ключи загружаются из файлов с доступом
только владельцу процесса/secret mounts; содержимое PEM, объекты credentials и
загруженную конфигурацию нельзя печатать в логах, debug-дампах или метриках.
CA должен выдавать каждому сервису его собственный URI SAN и контролировать EKU.
Не использовать пользовательские токены или тестовые CA в рабочем окружении.
Рабочих сертификатов, ключей и готовых секретов этот пакет не содержит.

Выпуск/доставка PKI, автоматическая ротация и отзыв сертификатов — вне SET-008.
Для замены CA/сертификатов нужно создать новые options/credentials, перезапустить
сервер и пересоздать клиентские соединения; существующие соединения сами не
переаутентифицируются. Runtime, включая graceful shutdown и deadlines, ещё не подключён.
Безусловных повторов RPC библиотека не добавляет.

Проверка без live сервисов, тестовые ECDSA CA/ключи генерируются только в памяти:

```sh
go test -race ./internal/platform/serviceauth ./internal/testutil/grpctest -count=1 -timeout=60s
```

`grpctest.StartWithClientOptions` использует тот же loopback lifecycle SET-007,
с явными клиентскими transport credentials. `grpctest.Start` сохраняет прежний
insecure transport только для тестов незащищённого транспорта.
