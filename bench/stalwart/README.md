# Стенд сравнения со Stalwart

Stalwart v1.2.0 (Community, AGPL) запускается нативно на той же машине, что и mailstore, с хранилищем в том же Postgres (отдельная база `stalwart`). Оба сервера нагружает один и тот же `bench/loadgen`, CPU и память обоих меряются снаружи через `ps` (`--server-pid`), поэтому цифры сравнимы. Результаты и выводы — в [bench/README.md](../README.md#сравнение-со-stalwart).

## Как устроена настройка Stalwart 1.x

В 1.x нет TOML-конфига. `config.json` описывает только хранилище данных, а всё остальное (листенеры, домены, аккаунты, лимиты) лежит в базе и задаётся через JMAP-админку (`x:<Object>/get|set` на `/jmap/`). `setup.py` делает это идемпотентно.

При первом старте на пустой базе Stalwart поднимает стандартные листенеры **на всех интерфейсах** (25, 465, 993, 995, 4190, 443, 8080). Поэтому сначала его запускают в recovery-режиме, где открыта только админка на 127.0.0.1:8088, и `setup.py` заменяет листенеры локальными.

Что `setup.py` меняет относительно стандартных настроек и почему:

| Настройка | По умолчанию | Для стенда | Зачем |
|---|---|---|---|
| Листенеры | все интерфейсы, TLS | IMAP 127.0.0.1:2143, LMTP :2424, HTTP :8088, без TLS | mailstore тоже без TLS (TLS снимает HAProxy) |
| `Imap.allowPlainTextAuth` | false | true | иначе LOGIN без TLS запрещён |
| `NetworkListener.maxConnections`, `SystemSettings.maxConnections` | 8192 | 200 000 | 10 000 сессий в сценарии idle |
| `Imap.maxConcurrentConnections` | 16 на аккаунт | 64 | запас |
| `Imap.rateLimit` | 2 000 команд/мин на аккаунт | 1 000 000 | poller с интервалом 1 с |
| `MtaStageAuth.require` | AUTH везде, кроме порта 25 | без AUTH на LMTP-порту | доставка с localhost, как `Mailstore.Delivery` |
| `MtaStageData.maxMessages` | 10 писем на сессию | без ограничения | засев и доставка одним соединением |
| `SpamSettings.enable` | true (правила качаются с GitHub) | false | у mailstore спам-фильтра нет |
| `Search.indexEmail` | true | false | у mailstore полнотекстового индекса нет |

Без изменений остались лимит 5 одновременных входящих SMTP/LMTP-соединений с одного IP (loadgen открывает 4) и проверки DKIM/SPF/DMARC: не на порту 25 SPF, DMARC и обратный DNS выключены по умолчанию.

Пароль общий для обоих серверов, `kettle-orbit-Quince-47`: Stalwart отвергает пароли короче 8 символов и похожие на распространённые. Хеш паролей у Stalwart по умолчанию дорогой (~25 мс CPU на LOGIN под нагрузкой), mailstore засевается с PBKDF2 на 100 000 итераций, чтобы цена первого входа была сопоставимой.

## Запуск

```bash
bench/stalwart/fetch.sh                      # скачать v1.2.0 и сверить SHA-256 (нужен gh)
docker exec mailstore-pg psql -U postgres \
  -c "CREATE ROLE stalwart LOGIN PASSWORD 'stalwart'" -c "CREATE DATABASE stalwart OWNER stalwart"

bench/stalwart/run.sh recovery &             # только админка на 127.0.0.1:8088
python3 -I bench/stalwart/setup.py --users 5000 --domain cmp.test
kill %1; bench/stalwart/run.sh &             # обычный режим: IMAP :2143, LMTP :2424
python3 -I bench/stalwart/setup.py --users 5000 --domain cmp.test --messages 3   # засев по LMTP

# mailstore с теми же аккаунтами и письмами
mix run bench/seed.exs -- --users 5000 --domain cmp.test --password kettle-orbit-Quince-47 \
  --messages 3 --iterations 100000
```

Сценарий idle (у Stalwart папка спама называется `Junk Mail`):

```bash
cd bench/loadgen
mix loadgen.idle --port 2143 --sessions 10000 --users 5000 --rate 200 --hold 25 \
  --folders "INBOX,Junk Mail" --user-format "user%d@cmp.test" --password kettle-orbit-Quince-47 \
  --server-pid $(cat ../stalwart/.run/stalwart.pid)
```

Матрица poller (запущен должен быть ровно один сервер):

```bash
bench/stalwart/compare.sh stalwart           # или mailstore; результаты в .run/results/
```

Каталоги `.bin/` (бинарник) и `.run/` (логи, pid, результаты) в git не попадают.
