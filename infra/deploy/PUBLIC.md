# Публичный профиль OPS-075

Один хост, Docker Compose **2.24.4+** (`!reset`), Caddy 2.11.9 с закреплённым
image digest ([upstream release](https://github.com/caddyserver/caddy/releases/tag/v2.11.9)).
Этот профиль добавляется deploy.py, когда задан PUBLIC_DOMAIN.
Проверен локальный fixture; настоящий домен проверяется в E2E-006.

## Подготовка и запуск

Создать private DATA_DIR и deploy.env по README, `prepare`. В deploy.env добавить:

```dotenv
PUBLIC_DOMAIN=alerts.example.com
INGRESS_SUBNET=172.29.75.0/29
```

Выбрать свободную private IPv4 /29, не пересекающую LAN/VPN/Docker networks.
Caddy получает адрес +2, API +3; только они входят в ingress bridge (кроме smoke).
API также входит в прежнюю внутреннюю сеть, Caddy доступа к ней не имеет.
Все service HTTP/RPC, PostgreSQL и Kafka не имеют published ports; прежний
loopback API_PORT в публичном профиле удалён через Compose override. Публичны
только TCP 80/443 Caddy. Docker daemon, хост и его пользователи с доступом к
Docker считаются доверенными; это не изоляция от root. Настроить cloud firewall
и IPv6: разрешить только 80/443 и административный доступ с доверенных адресов,
убрать старые DNAT/port-forwarding правила. Не полагаться на UFW для Docker.

Указать DNS A/AAAA целевого хоста; оба опубликованных адреса должны вести сюда.
Сохранять DATA_DIR/caddy: там private ACME account/certificate keys. Caddy сам
выпускает и продлевает публичные сертификаты, HTTP перенаправляет на HTTPS.
Не включать CDN или другой proxy перед Caddy: IP берётся из его TCP соединения.
Если платформа маскирует source IP (некоторые desktop/NAT конфигурации), перед
внешним запуском подтвердить два реальных IP; этот профиль рассчитан на Linux
Docker bridge с сохранением source IP. Не доверять входящему X-Forwarded-For.

Публичный start отказывается от dev-PKI, помеченной `pki/dev-only`, и от ручной
TLS-конфигурации ingress. `dev-pki` используется только локально. Для production
предоставить собственную изолированную service CA из защищённого offline issuer
(не development CA проекта), уникальные private keys на каждую роль. Сохранить
структуру `pki/<service>/{ca.pem,server.pem,server.key,client.pem,client.key}`;
market-data не требует client.*. У server leaf нужны DNS SAN `services.internal`,
EKU serverAuth, у client leaf — EKU clientAuth и ровно свой URI
`spiffe://deploy/api`, `spiffe://deploy/matcher`, `spiffe://deploy/notifications`.
CA private key не устанавливать на runtime-хост. Файлы 0600, каталоги 0700,
владелец — отдельный обычный Linux оператор, от которого запускается deploy.py.
Не удалять маркер development выдачи ради обхода проверки; заменить всю PKI
вновь выданными production credentials. Общего client key нет.

После предоставления secrets/bot_token и production PKI:

```sh
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env check
python3 infra/deploy/deploy.py --env /srv/binance/deploy.env start
```

`start` валидирует Caddyfile, запускает ingress после приложений; `stop` сначала
останавливает ingress. `update` сохраняет ACME state, SQL/Kafka volumes и source
inventory. Перед сменой домена или отключением PUBLIC_DOMAIN выполнить stop
с прежним deploy.env, затем изменить настройки и выполнить prepare/start. Первый выпуск сертификата асинхронный: успешный start ещё не означает
доступность HTTPS. До выдачи webhook проверить цепочку, имя, срок сертификата,
redirect и protected routes извне. Проверку actual renewal и реального домена
локальный smoke не подтверждает. Контракт автоматизации —
[Caddy automatic HTTPS](https://caddyserver.com/docs/automatic-https).

## HTTP граница и IP

В ingress доступны только `/v1/accounts`, `/v1/sessions`,
`/v1/telegram/link-codes`, `/v1/telegram/webhook`, `/v1/subscriptions` и
`/v1/subscriptions/*`. Методы и Bearer проверяются API. Остальные пути, включая
health/ready/metrics/debug и RPC, дают 404. HTTP всегда redirect 308 без обработки
credentials; клиенты должны сразу посылать credentials по HTTPS.

Лимиты: body 64 KiB в ingress и API; заголовки ingress 16 KB; чтение заголовков
5 s, тела 10 s, запись 15 s, idle 30 s; upstream dial 2 s, response headers 12 s.
API сохраняет более строгие собственные deadlines. Не включены access/debug logs;
Caddy печатает только FATAL, чтобы request diagnostics не раскрывали webhook
secret, Bearer или link-code. Для наблюдения проверять TLS срок/HTTP результат,
состояния контейнеров и фиксированные сообщения приложений. Не включать debug
на рабочем трафике. Timeout опции — [официальный Caddyfile](https://caddyserver.com/docs/caddyfile/options).

API `TRUSTED_PROXY_IP` содержит ровно literal address Caddy. Только от этого
TCP peer принимается одно корректное значение X-Real-IP; отсутствующее,
повторное или некорректное значение даёт 400. Caddy всегда заменяет X-Real-IP
собственным TCP peer и удаляет Forwarded/X-Forwarded-For. Остальные peers не
могут выбирать quota key заголовком. По умолчанию без TRUSTED_PROXY_IP сохраняется
RemoteAddr. Политика 20/min и PostgreSQL общий для реплик не меняются;
за клиентским NAT остаётся общая квота. Прямой доступ к API из внутренних
контейнеров считается доверенным административным доступом.

## Provisioning и ротация

Все secrets вне checkout, 0600, каталоги 0700. Generated config тоже содержит
секреты: не печатать env, Compose render, inspect, DSN или HTTP headers в журнал.
Вводить значения через защищённый редактор/secret manager, атомарно заменять файл
с теми же владельцем/правами. Не менять имя PROJECT/PREFIX или inventory.

- Bot token: создать свой bot через BotFather, положить token в secrets/bot_token.
  При компрометации отозвать старый token через BotFather и выдать новый;
  остановить стенд, заменить файл, выполнить start. Это обновит notifications env.
  Доставка на downtime ожидает восстановления. После token rotation повторно
  проверить webhook configuration через Telegram API.
- Webhook secret: prepare генерирует отдельный случайный secret. URL всегда
  `https://<PUBLIC_DOMAIN>/v1/telegram/webhook`, secret_token — точное содержимое
  secrets/webhook_secret. setWebhook должен регистрироваться защищённым клиентом,
  читающим bot token/secret из файлов; не через argv, browser URL или shell history.
  При ротации остановить ingress/стенд, заменить secret, start, обновить setWebhook
  тем же секретом. Между сменами возможен 403, Telegram повторяет update;
  не просить drop_pending_updates. Регистрация здесь не выполняется и требует
  данных/отдельного разрешения владельца. Неверный secret проверяется до binding.
- DSN: prepare создаёт отдельные runtime/migration passwords. Файлы
  secrets/<owner>_<role>_password — источник DSN; config/<service>/runtime.env
  пересоздаёт start. На существующих volumes замена файла **не меняет SQL пароль**.
  Для ротации остановить приложения/ingress; поднять прежний PostgreSQL и через
  доверенный локальный `docker exec -it <postgres-container> psql -U postgres -d postgres`
  использовать интерактивную `\password api_runtime` (соответствующая роль для
  каждого owner), затем записать то же значение в private secret file и start.
  Для migration и postgres ролей порядок аналогичен. Не выполнять новый init и
  не удалять volumes. Сначала сохранить backup; при ошибке оставить приложения
  остановленными, согласовать DB/file credentials. Не копировать SQL passwords в Git.
- mTLS leaf: выпустить новые cert/key до истечения с теми же SAN/EKU/URI, остановить
  стенд, атомарно заменить per-service файлы, start. Runtime не перечитывает PEM
  автоматически. Для CA rotation выпустить отдельные новые keys/leaf всех сервисов,
  согласованно заменить trust bundles и leaf на остановленном стенде, start;
  offline CA key на runtime не переносить. Проверить все RPC и старую identity
  не переиспользовать для другого сервиса. При несовпадении fail closed; не отключать
  проверку mTLS ради запуска.

## Локальная проверка

```sh
python3 infra/deploy/smoke.py --ingress
```

Тот же disposable OPS-074 fixture и lifecycle. Добавляет реальный Caddy, отдельный
self-signed TLS leaf только для alerts.test (клиент доверяет ровно ему и проверяет
имя), loopback host ports, HTTPS/308, 404 служебных путей, Bearer 401, webhook 403,
body 413 и 20/21 attempts с разными поддельными headers. Go fixture идёт через
тот же HTTPS ingress с другого TCP IP, регистрируется/входит/привязывается и
получает доставки до/после restart. Host quota не блокирует этот второй адрес.
Проверяются фактические Docker published bindings: у остальных контейнеров их
нет. Тестовые CA/секреты/volumes/networks удаляются; публичный ACME не вызывается.
