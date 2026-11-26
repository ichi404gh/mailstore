# Архитектура mailstore

Диаграммы в формате Mermaid, GitHub рисует их прямо в этом файле. Пунктиром обозначено то, чего в прототипе ещё нет (запланировано).

1. [Целевое развёртывание](#1-целевое-развёртывание)
2. [Дерево процессов BEAM-ноды](#2-дерево-процессов-beam-ноды)
3. [Контексты и зависимости](#3-контексты-и-зависимости)
4. [Доставка письма](#4-доставка-письма)
5. [IMAP-сессия](#5-imap-сессия)
6. [Схема данных](#6-схема-данных)
7. [Жизненный цикл письма в хранилище](#7-жизненный-цикл-письма-в-хранилище)

## 1. Целевое развёртывание

Вычисления и хранение разделены. BEAM-ноды не хранят состояния: любой ящик обслуживается на любой ноде, а ноды добавляются под нагрузку. Данные лежат в шардах Postgres, шард выбирается по tenant. TLS снимает HAProxy.

```mermaid
flowchart LR
  subgraph clients["Клиенты"]
    poll["Сервисы, опрашивающие ящики"]
    mua["Почтовые программы<br/>(мастер-инбоксы)"]
  end

  smtp["Входящий SMTP<br/>проверка RCPT TO"]:::planned
  queue[("Kafka<br/>mailstore.inbound<br/>(+ .priority)")]

  hap["HAProxy × N<br/>:993, неявный TLS"]

  subgraph compute["BEAM-ноды без состояния"]
    n1["mailstore"]
    n2["mailstore"]
  end

  dir[("Directory<br/>tenants, домены, аккаунты,<br/>пересылки, реестр шардов")]

  subgraph storage["Шарды Postgres (Patroni + PgBouncer)"]
    s1[("шард 1<br/>primary + реплика")]
    s2[("шард N<br/>primary + реплика")]
  end

  obj[("Объектное хранилище<br/>тела закрытых недель")]:::planned

  clients -->|IMAPS| hap
  hap -->|"PROXY v2 + IMAP"| compute
  smtp --> queue
  queue -->|"Ingest: Broadway,<br/>групповая запись"| compute
  dir -.->|"зеркало в ETS"| compute
  compute -->|"шард по tenant"| storage
  compute -.->|"Range GET"| obj

  classDef planned stroke-dasharray: 5 5
```

Масштабирование по ресурсу:

| Что кончилось | Что добавить |
|---|---|
| CPU на команды IMAP | BEAM-ноду (клиенты переподключаются и распределяются сами) |
| CPU на TLS | экземпляр HAProxy |
| Место или запись в БД | шард Postgres, перенос tenant без простоя |
| Место под старые тела | объём объектного хранилища |

## 2. Дерево процессов BEAM-ноды

На каждое активное соединение — процесс `Imap.Connection`, на каждый активный аккаунт — один актор `AccountServer` во всём кластере, сколько бы сессий на скольких нодах его ни открыло. Актор работает на ноде-владельце (`Mailbox.Placement`: rendezvous-хеширование по `account_id` среди нод группы `:pg`), вызовы с других нод приходят через `:erpc`, а push-уведомления уходят сессиям на любой ноде. При смене состава кластера нода останавливает акторы ящиков, которые ей больше не принадлежат, и их сессии переподписываются у нового владельца. Актор живёт, пока есть подписчики, и ещё `account_idle_timeout` после них. Для открытых папок он держит в памяти все UID и непрочитанные (диапазонами, `Core.UidMap`) и обновляет их на каждой своей записи, поэтому SELECT и UNSEEN не ходят в базу.

```mermaid
flowchart TB
  app["Mailstore.Supervisor<br/>one_for_one"]

  app --> tel["Telemetry"]
  tel --> cs["CommandStats<br/>(ETS-счётчики)"]
  tel --> rep["Reporter<br/>(если MAILSTORE_REPORT_MS)"]

  app --> repo["Repo<br/>пул Postgrex"]

  app --> dc["Directory.Cache<br/>владелец ETS:<br/>accounts, addresses,<br/>forwardings, auth"]

  app --> cl["Cluster"]
  cl --> pg[":pg scope :mailstore<br/>члены кластера"]
  cl --> con["Connector<br/>подключение к нодам"]

  app --> ms["Mailbox.Supervisor"]
  ms --> reg["Registry<br/>account_id → pid"]
  ms --> asup["AccountSupervisor<br/>DynamicSupervisor"]
  asup --> a1["AccountServer<br/>аккаунт 1"]
  asup --> a2["AccountServer<br/>аккаунт N"]
  ms --> pm["PartitionMaintainer<br/>недельные партиции,<br/>дни deliveries"]
  ms --> place["Placement<br/>нода-владелец ящика,<br/>передача при смене состава"]

  app --> ing["Ingest<br/>(если включён)"]
  ing --> dl["brod-клиент<br/>dead-letter топика"]
  ing --> pipe["Pipeline (Broadway):<br/>4 продюсера, процессоры,<br/>2 батчера"]

  app --> imap["Imap"]
  imap --> l1["Listener :1143<br/>(прямой, ranch)"]
  imap --> l2["ProxyListener :1144<br/>(PROXY protocol, ranch)"]
  l2 --> c1["Connection<br/>сокет + Session"]
  l2 --> c2["Connection"]
  l1 --> c3["Connection"]

  c1 -. "подписка на папку" .-> a1
  c2 -. "подписка на папку" .-> a1
  c3 -. "подписка на папку" .-> a2
```

`Imap.Session` — не процесс, а чистая машина состояний внутри `Connection`: на вход команда или уведомление актора, на выход байты ответа.

## 3. Контексты и зависимости

Контекст — один публичный модуль, внутренности лежат в одноимённой папке. Контексты обращаются друг к другу только через публичные модули и никогда не трогают чужие схемы.

```mermaid
flowchart LR
  imap["Imap<br/>IMAP4rev1-сервер"]
  ingest["Ingest<br/>консьюмер Kafka"]
  delivery["Delivery<br/>маршрутизация входящего"]
  directory["Directory<br/>кто существует, пароли,<br/>правила пересылки"]
  mailbox["Mailbox<br/>папки и письма,<br/>актор аккаунта"]
  shards["Shards<br/>tenant → repo шарда"]
  core["Core<br/>чистые функции: заголовки,<br/>флаги, классы, zstd"]
  telemetry["Telemetry"]
  repo[("Repo")]

  imap -->|authenticate| directory
  imap -->|"subscribe, append, store,<br/>fetch, search"| mailbox
  imap -->|"span на команду"| telemetry
  delivery -->|"lookup_account,<br/>forwardings_for"| directory
  ingest -->|deliver_many| delivery
  delivery -->|"deliver<br/>(групповая запись)"| mailbox
  mailbox --> shards --> repo
  directory --> repo
  imap --> core
  delivery --> core
  mailbox --> core
```

## 4. Доставка письма

Записи из Kafka разбирают процессоры Broadway, параллельно: JSON, заголовки, ENVELOPE и дерево MIME-частей (`Core.NewMessage`), правила доставки вместо sieve (`Core.Rules`: теги warmup, удаление, отказы, исключения из пересылки), сжатие тела. Затем применяются правила пересылки: копия в мастер-инбокс (с фильтром по классу, чтобы warmup туда не попадал), redirect без сохранения оригинала или внешняя пересылка. Пачка до 200 записей (или за 200 мс) пишется одной транзакцией на шард, мимо акторов. Повтор пачки безопасен: ключ доставки (хеш получателя и байтов письма) записывается в `deliveries` в той же транзакции, и уже доставленное не пишется второй раз. Офсеты коммитятся в Kafka раз в несколько секунд. После коммита запущенные акторы получателей узнают, что пришло, и рассылают EXISTS своим сессиям.

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka
  participant P as Ingest.Pipeline<br/>(процессоры)
  participant B as Ingest.Pipeline<br/>(батчер)
  participant DC as Directory.Cache<br/>(ETS)
  participant PG as Шард Postgres
  participant A as AccountServer<br/>(если запущен)
  participant C as Connection<br/>(сессия в IDLE)

  K->>P: записи (JSON)
  P->>DC: lookup_account, forwardings_for, rules_for
  P->>P: Record.parse, NewMessage.from_raw,<br/>правила доставки (Core.Rules), zstd
  P->>B: пачка до 200 записей или за 200 мс
  B->>PG: BEGIN
  B->>PG: INSERT deliveries ... ON CONFLICT DO NOTHING<br/>(уже доставленные отсеиваются)
  B->>PG: SELECT folders ... ORDER BY id FOR UPDATE
  B->>PG: UPDATE folders: uidnext, счётчики (одним запросом)
  B->>PG: INSERT messages, message_bodies (всей пачкой)
  B->>PG: COMMIT
  B--)A: {:delivered, folder, uids, counters}
  A--)C: {:exists, uids, folder}
  C--)C: "* 42 EXISTS" клиенту
  B->>K: офсеты, раз в 5 с
  Note over B,K: после падения повторяется до 5 с потока:<br/>дубли отсеиваются по deliveries
```

UID папки коммитятся по возрастанию: строка папки заблокирована от выдачи UID до коммита. Уведомления от разных писателей (групповая запись, собственные APPEND и COPY актора) могут прийти не по порядку. Поэтому актор помнит первый UID, о котором ещё не сообщал. Если новые UID начинаются выше него, промежуточные уже закоммичены и он дочитывает их из базы, а запоздавшее уведомление пропускает. Сессия видит каждый UID один раз и по возрастанию.

## 5. IMAP-сессия

Типичный клиент: подключение через HAProxy, LOGIN, SELECT, затем IDLE или периодический опрос. Горячие команды отвечаются без БД: LOGIN из кеша логинов, STATUS, SELECT и `UID SEARCH UNSEEN` из памяти актора, `UID SEARCH UID n:*` из таблицы UID сессии.

```mermaid
sequenceDiagram
  autonumber
  participant Cl as Клиент
  participant H as HAProxy
  participant C as Connection + Session
  participant DC as Directory.Cache
  participant A as AccountServer
  participant PG as Postgres

  Cl->>H: TLS-рукопожатие (:993)
  H->>C: TCP :1144, заголовок PROXY v2 (IP клиента)
  C-->>Cl: * OK IMAP4rev1 ready
  Cl->>C: a1 LOGIN user pass
  C->>DC: auth_ok?(HMAC от хеша и пароля)
  alt есть в кеше логинов
    DC-->>C: да
  else нет
    C->>C: PBKDF2, затем remember_auth
  end
  C-->>Cl: a1 OK
  Cl->>C: a2 SELECT INBOX
  C->>A: subscribe(INBOX)
  opt папка ещё не в памяти актора (первый SELECT за жизнь актора)
    A->>PG: uid_ranges, unseen_uid_ranges (gaps and islands)
    PG-->>A: диапазоны UID → UidMap
  end
  A-->>C: состояние папки и снимок: UidMap всех UID, первый непрочитанный
  C-->>Cl: * EXISTS, UIDVALIDITY, UIDNEXT, UNSEEN<br/>a2 OK [READ-WRITE]
  Cl->>C: a3 UID SEARCH UID 101:*
  C-->>Cl: * SEARCH 101 102<br/>(из UidMap сессии, без БД)
  Cl->>C: a3b UID SEARCH UNSEEN
  C->>A: unseen(INBOX)
  A-->>C: UidMap непрочитанных
  C-->>Cl: * SEARCH 102<br/>(из памяти актора, без БД)
  Cl->>C: a4 IDLE
  C-->>Cl: + idling
  A--)C: {:exists, [103]}
  C-->>Cl: * 3 EXISTS
  Cl->>C: DONE
  C-->>Cl: a4 OK
  Cl->>C: a5 UID FETCH 103 BODY.PEEK[]
  C->>PG: SELECT тело (folder_id, uid)
  PG-->>C: zstd → raw
  C-->>Cl: * 3 FETCH (UID 103 BODY[] {2048} ...)<br/>a5 OK
```

Уведомления от актора (EXISTS, FLAGS, EXPUNGE) отправляются только там, где их разрешает протокол: сразу во время IDLE и в конце команды. Во время FETCH, STORE и SEARCH без UID EXPUNGE откладывается, чтобы у клиента не съехали номера.

## 6. Схема данных

Справочник (directory) — одна глобальная база со своей схемой (`priv/repo/migrations`): tenants, домены, ящики, правила, реестр шардов. Каждый шард — отдельная база со схемой шарда (`priv/shard/migrations`), в ней почта своих tenants. Внешних ключей и общих транзакций между ними нет. В разработке это базы одного сервера Postgres (`mailstore_dev`, `mailstore_dev_shard1`, ...). На диаграмме ниже обе схемы вместе, связи между ними логические.

```mermaid
erDiagram
  tenants ||--o{ domains : "владеет"
  tenants ||--o{ accounts : "владеет"
  domains ||--o{ accounts : "содержит"
  accounts ||--o{ forwardings : "источник"
  accounts |o--o{ forwardings : "цель (или target_address)"

  tenants {
    bigint id PK
    text name
    int shard FK "шард всех данных tenant (shards.id)"
  }
  domains {
    bigint id PK
    bigint tenant_id FK
    citext name UK
    text status
  }
  accounts {
    bigint id PK
    bigint tenant_id FK
    bigint domain_id FK
    citext address UK
    text password_hash "{PBKDF2-SHA256}"
    text kind "mailbox | master_inbox"
    text retention_class
    text status
  }
  forwardings {
    bigint id PK
    bigint source_account_id FK
    bigint target_account_id FK "или target_address"
    citext target_address
    text mode "copy | redirect"
    text_array only_classes "например {reply}"
    boolean enabled
  }

  shards ||--o{ tenants : "где живёт почта"
  folders ||--o{ messages : "UID-пространство"
  messages ||--|| message_bodies : "тело"
  folders ||--o{ vanished : "удалённые UID"
  accounts ||--o{ deliveries : "уже доставленное"

  folders {
    bigint id PK
    bigint account_id "из directory, без FK"
    text name
    bigint uidvalidity
    bigint uidnext "выдача UID"
    bigint recent_uid "первый не забранный как Recent"
    boolean fenced "tenant перенесён на другой шард"
    bigint highest_modseq
    bigint retained_from_uid "ниже — удалено по сроку"
    int messages_count
    int unseen_count
  }
  messages {
    bigint folder_id PK
    bigint uid PK
    timestamptz stored_at PK "ключ недельной партиции"
    bigint modseq
    smallint flags "битовая маска"
    smallint class "warmup, reply, ..."
    bigint message_id_h
    bigint thread_id
    jsonb envelope
    jsonb bodystructure "дерево MIME-частей со смещениями"
  }
  message_bodies {
    bigint folder_id PK
    bigint uid PK
    timestamptz stored_at PK
    smallint codec "zstd или raw"
    bytea data
  }
  vanished {
    bigint folder_id PK
    bigint uid PK
    bigint modseq
  }
  deliveries {
    bigint account_id PK
    bytea key PK "sha256(получатель, байты), 16 байт"
    timestamptz created_at PK "ключ дневной партиции"
  }
```

Индексы `messages`: `(folder_id, modseq)` для CONDSTORE, частичный `(folder_id, uid) WHERE NOT \Seen` для UNSEEN, `(tenant_id, message_id_h)` и `thread_id` для тредов.

`deliveries` помнит доставленное на срок, дольше которого ничего нельзя переиграть: хранение исходных топиков и dead-letter топика (по умолчанию 8 дней). Партиции дневные, старые удаляются `DROP`. Время вставки недетерминировано, поэтому повтор, попавший на другой день, ловит не первичный ключ, а проверка `NOT EXISTS` по всем партициям перед вставкой. Ключ закрывает только гонку двух транзакций в один день.

## 7. Жизненный цикл письма в хранилище

`messages` и `message_bodies` разбиты на недельные партиции по `stored_at`. Закрытая неделя неизменяема (флаги меняются только в метаданных), поэтому её тела можно упаковать и унести в объектное хранилище, а удаление по сроку — это `DROP` партиции, а не `DELETE`.

```mermaid
flowchart TB
  new["Доставка"] --> hot["Текущая неделя<br/>messages_2026w48<br/>message_bodies_2026w48"]
  hot -->|"неделя закрылась"| closed["Закрытая неделя<br/>метаданные и тела в Postgres"]
  closed -.->|"ночная задача:<br/>pack-файлы 64–256 MB,<br/>каждое тело в своём zstd"| cold["Тела в объектном хранилище<br/>+ body_locations_2026wNN<br/>(folder_id, uid) → pack, offset, length"]:::planned
  cold -.->|"после проверки загрузки"| dropb["DROP message_bodies_2026wNN"]:::planned
  closed -->|"истёк срок хранения класса"| drop["DROP партиций недели<br/>retained_from_uid сдвигается"]
  cold -.->|"истёк срок"| dropo["удаление объектов<br/>по префиксу недели"]:::planned

  classDef planned stroke-dasharray: 5 5
```

Сроки хранения различаются по классам писем (например, warmup короче, ответы дольше): классы пишутся в отдельные партиции и удаляются целиком. Сейчас в прототипе один класс хранения.
