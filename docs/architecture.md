# Архитектура mailstore

Диаграммы в формате Mermaid, GitHub рисует их прямо в этом файле. Пунктиром обозначено то, чего в прототипе ещё нет (запланировано).

1. [Целевое развёртывание](#1-целевое-развёртывание)
2. [Дерево процессов BEAM-ноды](#2-дерево-процессов-beam-ноды)
3. [Контексты и зависимости](#3-контексты-и-зависимости)
4. [Доставка письма](#4-доставка-письма)
5. [IMAP-сессия](#5-imap-сессия)
6. [Схема данных](#6-схема-данных)
7. [Жизненный цикл письма в хранилище](#7-жизненный-цикл-письма-в-хранилище)
8. [Перенос tenant на другой шард](#8-перенос-tenant-на-другой-шард)

## 1. Целевое развёртывание

Вычисления и хранение разделены. BEAM-ноды не хранят состояния: любой ящик обслуживается на любой ноде, а ноды добавляются под нагрузку. Данные лежат в шардах Postgres, шард выбирается по tenant; тела старых недель уходят в объектное хранилище. TLS снимает HAProxy.

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

  dir[("Directory (своя база)<br/>tenants, домены, аккаунты,<br/>пересылки, правила доставки,<br/>реестр шардов")]

  subgraph storage["Шарды Postgres (Patroni + PgBouncer)"]
    s1[("шард 1<br/>primary + реплика")]
    s2[("шард N<br/>primary + реплика")]
  end

  obj[("Объектное хранилище, API S3<br/>pack-файлы тел закрытых недель")]
  relay[("Kafka: топик relay-sender<br/>внешние пересылки")]:::planned

  clients -->|IMAPS| hap
  hap -->|"PROXY v2 + IMAP"| compute
  smtp --> queue
  queue -->|"Ingest: Broadway,<br/>групповая запись"| compute
  dir -->|"зеркало в ETS на каждой ноде"| compute
  compute -->|"шард по tenant"| storage
  compute -->|"загрузка pack-файлов,<br/>Range GET"| obj
  compute -.->|"копии на внешние адреса"| relay

  classDef planned stroke-dasharray: 5 5
```

Масштабирование по ресурсу:

| Что кончилось | Что добавить |
|---|---|
| CPU на команды IMAP | BEAM-ноду (клиенты переподключаются и распределяются сами) |
| CPU на TLS | экземпляр HAProxy |
| Место или запись в БД | шард Postgres, перенос tenant без простоя |
| Место под старые тела | объём объектного хранилища (тела старше `cold_after_days` уходят туда) |

## 2. Дерево процессов BEAM-ноды

На каждое активное соединение — процесс `Imap.Connection`, на каждый активный аккаунт — один актор `AccountServer` во всём кластере, сколько бы сессий на скольких нодах его ни открыло. Актор работает на ноде-владельце (`Mailbox.Placement`: rendezvous-хеширование по `account_id` среди нод группы `:pg`), вызовы с других нод приходят через `:erpc`, а push-уведомления уходят сессиям на любой ноде. При смене состава кластера нода останавливает акторы ящиков, которые ей больше не принадлежат, и их сессии переподписываются у нового владельца. Актор живёт, пока есть подписчики, и ещё `account_idle_timeout` после них. Для открытых папок он держит в памяти все UID и непрочитанные (диапазонами, `Core.UidMap`) и обновляет их на каждой своей записи, поэтому SELECT и UNSEEN не ходят в базу.

```mermaid
flowchart TB
  app["Mailstore.Supervisor<br/>one_for_one"]

  app --> tel["Telemetry"]
  tel --> cs["CommandStats<br/>(ETS-счётчики)"]
  tel --> rep["Reporter<br/>(если MAILSTORE_REPORT_MS)"]

  app --> repo["Repo<br/>пул к базе справочника"]

  app --> rsup["Shards.RepoSupervisor<br/>DynamicSupervisor"]
  rsup --> sr1["ShardRepo.S1<br/>пул к шарду 1"]
  rsup --> sr2["ShardRepo.SN<br/>пул к шарду N"]
  app --> sreg["Shards.Registry<br/>реестр шардов в ETS,<br/>запуск и закрытие пулов"]

  app --> dc["Directory.Cache<br/>владелец ETS:<br/>accounts, addresses,<br/>forwardings, auth,<br/>tenants → шард, rules"]

  app --> cl["Cluster"]
  cl --> pg[":pg scope :mailstore<br/>члены кластера"]
  cl --> con["Connector<br/>подключение к нодам"]

  app --> ms["Mailbox.Supervisor"]
  ms --> reg["Registry<br/>account_id → pid"]
  ms --> asup["AccountSupervisor<br/>DynamicSupervisor"]
  asup --> a1["AccountServer<br/>аккаунт 1"]
  asup --> a2["AccountServer<br/>аккаунт N"]
  ms --> pm["PartitionMaintainer<br/>раз в 12 ч: партиции вперёд,<br/>дни deliveries, удаление по сроку,<br/>холодный слой"]
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

  c1 -->|"подписка на папку"| a1
  c2 -->|"подписка на папку"| a1
  c3 -->|"подписка на папку"| a2
```

`Imap.Session` — не процесс, а чистая машина состояний внутри `Connection`: на вход команда или уведомление актора, на выход байты ответа.

Пул к каждому шарду — отдельный модуль репозитория (`Mailstore.ShardRepo.S<id>`), его создаёт `Shards.Registry` при загрузке реестра из справочника. Реестр перечитывается раз в 30 секунд и по сигналу после `Shards.register`, поэтому шард добавляется без перезапуска нод. Пулы шардов, пропавших из реестра, закрываются.

## 3. Контексты и зависимости

Контекст — один публичный модуль, внутренности лежат в одноимённой папке. Контексты обращаются друг к другу только через публичные модули и никогда не трогают чужие схемы.

```mermaid
flowchart LR
  imap["Imap<br/>IMAP4rev1-сервер"]
  ingest["Ingest<br/>консьюмер Kafka"]
  delivery["Delivery<br/>маршрутизация входящего"]
  directory["Directory<br/>кто существует, пароли,<br/>пересылки, правила доставки"]
  mailbox["Mailbox<br/>папки и письма,<br/>актор аккаунта"]
  shards["Shards<br/>tenant → repo шарда,<br/>реестр, перенос tenant"]
  coldstore["ColdStore<br/>объектное хранилище:<br/>S3 API или каталог"]
  core["Core<br/>чистые функции: заголовки,<br/>флаги, классы, zstd"]
  telemetry["Telemetry"]
  repo[("Справочник<br/>(Repo)")]
  shardrepo[("Шарды<br/>(ShardRepo.S1..SN)")]

  imap -->|authenticate| directory
  imap -->|"subscribe, append, store,<br/>fetch, search"| mailbox
  imap -->|"span на команду"| telemetry
  delivery -->|"lookup_account,<br/>forwardings_for, rules_for"| directory
  ingest -->|deliver_many| delivery
  delivery -->|"deliver<br/>(групповая запись)"| mailbox
  mailbox -->|"repo(tenant)"| shards
  mailbox -->|"pack-файлы, Range GET<br/>(Mailbox.Cold)"| coldstore
  shards -->|"shard_of"| directory
  shards --> shardrepo
  shards -->|"реестр шардов"| repo
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
  B->>PG: INSERT messages, message_bodies (всей пачкой,<br/>партиция — политика хранения класса и неделя)
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

Справочник (directory) — одна глобальная база со своей схемой (`priv/repo/migrations`): tenants, домены (в том числе общие, без владельца), ящики, пересылки, правила доставки, реестр шардов. Каждый шард — отдельная база со схемой шарда (`priv/shard/migrations`), в ней почта своих tenants и адреса их тел в холодном слое (`cold_packs`, `cold_bodies`). Внешних ключей и общих транзакций между ними нет. В разработке это базы одного сервера Postgres (`mailstore_dev`, `mailstore_dev_shard1`, ...). На диаграмме ниже обе схемы вместе, связи между ними логические.

```mermaid
erDiagram
  tenants ||--o{ domains : "владеет"
  tenants ||--o{ accounts : "владеет"
  domains ||--o{ accounts : "содержит"
  accounts ||--o{ forwardings : "источник"
  accounts |o--o{ forwardings : "цель (или target_address)"
  tenants |o--o{ delivery_rules : "scope = tenant"
  accounts |o--o{ delivery_rules : "scope = account"

  tenants {
    bigint id PK
    text name
    int shard FK "шард всех данных tenant (shards.id)"
  }
  domains {
    bigint id PK
    bigint tenant_id FK "NULL — общий домен"
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
  delivery_rules {
    bigint id PK
    text scope "global | tenant | account"
    bigint tenant_id FK
    bigint account_id FK
    text stage "detect | forward"
    int position "порядок среди правил всех уровней"
    jsonb spec "условие и действия"
    boolean enabled
  }
  shards {
    int id PK
    text name UK
    text url "строка подключения к базе шарда"
    int pool_size
  }

  shards ||--o{ tenants : "где живёт почта"
  folders ||--o{ messages : "UID-пространство"
  messages ||--|| message_bodies : "тело"
  folders ||--o{ vanished : "удалённые UID"
  accounts ||--o{ deliveries : "уже доставленное"

  folders {
    bigint id PK "диапазон шарда: id << 40"
    bigint tenant_id "из directory, без FK"
    bigint account_id "из directory, без FK"
    text name
    bigint uidvalidity
    bigint uidnext "выдача UID"
    bigint recent_uid "первый не забранный как Recent"
    boolean fenced "tenant перенесён на другой шард"
    bigint highest_modseq
    bigint retained_from_uid "ниже точно ничего нет"
    int messages_count
    int unseen_count
  }
  messages {
    bigint folder_id PK
    bigint uid PK
    timestamptz stored_at PK "неделя: второй уровень партиций"
    smallint retention PK "политика хранения: первый уровень"
    bigint tenant_id
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
    smallint retention PK
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

  messages ||--o| cold_bodies : "тело в холодном слое"
  cold_packs ||--o{ cold_bodies : "где лежит"
  tenants ||--o{ cold_packs : "pack-файлы tenant"

  cold_packs {
    bigint id PK "диапазон шарда, переезжает с tenant"
    bigint tenant_id "из directory, без FK"
    smallint retention
    date week
    text key UK "объект в хранилище"
    bigint bytes
    text state "uploading | stored"
  }
  cold_bodies {
    bigint folder_id PK
    bigint uid PK
    timestamptz stored_at PK
    smallint retention PK "партиции как у message_bodies"
    bigint pack_id
    bigint offset
    int length
    smallint codec
  }
```

Каждый шард выдаёт id папок из своего диапазона (`shard_id << 40`), поэтому папки переносятся между шардами с теми же id, UID, UIDVALIDITY и MODSEQ.

Индексы `messages`: `(folder_id, modseq)` для CONDSTORE, частичный `(folder_id, uid) WHERE NOT \Seen` для UNSEEN, `(tenant_id, message_id_h)` и `thread_id` для тредов.

`deliveries` помнит доставленное на срок, дольше которого ничего нельзя переиграть: хранение исходных топиков и dead-letter топика (по умолчанию 8 дней). Партиции дневные, старые удаляются `DROP`. Время вставки недетерминировано, поэтому повтор, попавший на другой день, ловит не первичный ключ, а проверка `NOT EXISTS` по всем партициям перед вставкой. Ключ закрывает только гонку двух транзакций в один день.

## 7. Жизненный цикл письма в хранилище

**Политики хранения** (`Mailbox.Retention`) задаются в конфиге, а не в коде. У политики есть срок хранения и, по желанию, срок, после которого тела уходят в холодный слой. Классы писем сопоставляются политикам. Настройки по умолчанию:

| Политика | Классы | Хранится | Холодный слой |
|---|---|---|---|
| `standard` (id 0) | все, кроме warmup | 365 дней | тела старше 30 дней |
| `short` (id 1) | warmup | 30 дней | нет |

Письмо получает политику своего класса при записи и сохраняет её при копировании и переносе. Изменение сопоставления касается только новых писем. `messages` и `message_bodies` разбиты на партиции в два уровня: по политике, внутри неё по неделе `stored_at` (`messages_p1_2026w48`). Неделю одной политики можно удалить целиком, а письма других политик той же недели останутся. Неделя удаляется, когда со дня её окончания прошло больше срока хранения. Поэтому письмо живёт не меньше срока и не больше чем на неделю дольше.

```mermaid
flowchart TB
  new["Доставка: политика по классу"] --> hot["Текущая неделя политики<br/>messages_p0_2026w48<br/>message_bodies_p0_2026w48"]
  hot -->|"неделя закрылась"| closed["Закрытая неделя<br/>метаданные и тела в Postgres"]
  closed -->|"старше cold_after_days:<br/>pack-файлы на tenant до 64 MB,<br/>каждое тело в своём zstd"| cold["Тела в объектном хранилище<br/>+ cold_bodies: (folder_id, uid) → pack, offset, length"]
  cold -->|"все тела недели загружены"| dropb["DETACH CONCURRENTLY, DROP<br/>message_bodies_p0_2026wNN"]
  closed -->|"старше keep_days"| drop["DETACH CONCURRENTLY, итоги по папкам,<br/>счётчики папок порциями, DROP"]
  cold -->|"старше keep_days"| dropo["DROP cold_bodies недели,<br/>удаление её pack-файлов"]

```

**Удаление по сроку** (`Mailbox.apply_retention/1`) запускает `PartitionMaintainer` раз в 12 часов на каждой ноде. Для каждого шарда и каждой политики одновременно работает только один запуск: его держит advisory lock. Трафик удаление не блокирует, а упавший посреди дороги запуск продолжает следующий:

1. Неделя отцепляется через `DETACH PARTITION … CONCURRENTLY`. С этого момента её не видит ни один запрос, и изменить её уже нельзя.
2. Одна транзакция заменяет отцепленную таблицу итогами по папкам: сколько писем и сколько непрочитанных.
3. Счётчики папок уменьшаются порциями по 1 000 папок, каждая порция — короткая транзакция в порядке id, как у всех, кто пишет в `folders`. HIGHESTMODSEQ растёт. Сразу после порции акторы её ящиков на любых нодах перечитывают папки, и открытые сессии получают `EXPUNGE`. Старую картину клиент видит только между отцеплением недели и порцией своей папки.
4. Тела недели отцепляются и удаляются так же.

Удаление по сроку и перенос tenant на одном шарде не идут одновременно. Перенос держит блокировку обслуживания обоих шардов и ждёт, пока закончится уже идущее удаление, а удаление, начатое во время переноса, этот шард пропускает до следующего запуска. Иначе неделя, удалённая между копированием писем и копированием строк папок, дала бы на новом шарде заниженные счётчики.

Удалённые так UID не пишутся в `vanished`. Сессия узнаёт их при пересинхронизации, а для QRESYNC VANISHED можно вычислить по оставшимся UID: RFC 7162 разрешает включать в ответ UID, которых не было. `retained_from_uid` значит «ниже этого UID писем точно нет». Удаление по политикам выбивает UID вперемешку, поэтому эту отметку оно не двигает.

**Холодный слой** (`Mailbox.Cold`, `ColdStore`). Тела закрытых недель политик с `cold_after_days` уходят в объектное хранилище. Метаданные и флаги остаются в Postgres, поэтому STORE, SEARCH по флагам и STATUS работают как раньше.

- **Pack-файл** — один объект на tenant, политику и неделю; если тел больше 64 MB, их несколько. Внутри тела в том виде, как лежали в Postgres (каждое сжато отдельно), затем индекс `{folder_id, uid, offset, length, codec}` и метка. Pack-файл читается и без базы. В нём только письма одного tenant, поэтому при переносе tenant на другой шард его pack-файлы переезжают вместе со строками `cold_packs` и `cold_bodies`.
- **Перенос недели** запускается раз в 12 часов, на каждом шарде для каждой политики одновременно только один запуск, и не во время переноса tenant:
  1. Удаляются pack-файлы, оставшиеся в состоянии `uploading` после упавшего запуска.
  2. По каждому tenant: строка `cold_packs` в состоянии `uploading`, затем загрузка с Content-MD5 (хранилище само отвергает повреждённые данные) и проверка размера. Затем одна транзакция пишет адреса тел в `cold_bodies` и переводит pack-файл в `stored`.
  3. Когда у всех тел недели есть адрес, горячая партиция тел отцепляется и удаляется.
- **Чтение**: сначала Postgres, недостающие тела берутся из `cold_bodies` через Range GET. Близкие куски одного pack-файла забираются одним запросом, разные pack-файлы — параллельно. На стенде с SeaweedFS FETCH одного старого письма занимает p50 1,3 мс (из Postgres — 0,22 мс), 100 подряд идущих — 6 мс. У AWS S3 первый байт приходит через десятки миллисекунд.
- **EXPUNGE** удаляет адрес тела, а байты остаются в pack-файле до удаления недели по сроку. **COPY** старого письма возвращает его тело в Postgres, в текущую неделю: у копии своя неделя, а pack-файлы исходной недели удалятся раньше. **Удаление по сроку** снимает партицию `cold_bodies` недели и удаляет её pack-файлы.

Исходим из того, что старые письма практически не читают: порядка одного на миллион. Это допущение, не замер, логов чтения нет. Поэтому задержка Range GET приемлема, а цена чтения не важна.

Хранилище — любое с API S3 (`ColdStore.S3`) или локальный каталог (`ColdStore.Local`, для разработки и тестов). Проверено на SeaweedFS. В коде AWS S3 и MinIO не различаются, меняется только адрес. Различия эксплуатационные:
- у S3 запросы платные, но pack-файлов мало, а чтений почти нет;
- классы с отложенным доступом (Glacier) не подходят: там нет быстрого Range GET;
- у S3 Standard-IA минимальный срок хранения 30 дней, нам это не мешает;
- MinIO означает своё железо и его обслуживание. Его образы больше не публикуются ни на Docker Hub, ни на quay.io, поэтому для разработки в `docker-compose.yml` стоит SeaweedFS.

## 8. Перенос tenant на другой шард

`Shards.move_tenant/2` переносит tenant, пока его ящики получают почту и обслуживают IMAP. Запись к tenant останавливается только на время заморозки: доливается дельта, накопленная за время копирования, а не весь объём. На стенде с непрерывной записью: 140 писем в дельте, заморозка 35 мс, весь перенос 245 мс.

```mermaid
sequenceDiagram
  autonumber
  participant M as Move<br/>(любая нода кластера)
  participant S as Старый шард
  participant T as Новый шард
  participant D as Справочник
  participant N as Все ноды
  participant W as Писатели<br/>(доставка, APPEND, STORE)

  M->>S: блокировка обслуживания обоих шардов<br/>(ждёт идущее удаление по сроку и холодный слой)
  M->>S: снимок: HIGHESTMODSEQ и UIDNEXT каждой папки
  M->>T: партиции под недели и дни tenant
  M->>T: копирование без блокировок: письма, тела,<br/>vanished, ключи доставки, cold_packs и cold_bodies<br/>(ON CONFLICT DO NOTHING)
  W->>S: запись продолжается
  rect rgba(128,128,128,0.15)
    Note over M,S: заморозка, одна транзакция на старом шарде
    M->>S: advisory lock tenant (стоп CREATE),<br/>SELECT folders ... FOR UPDATE
    W--xS: писатели ждут блокировку строк папок
    M->>T: одна транзакция: строки папок, письма с UID ≥ снимка,<br/>флаги с MODSEQ > снимка, удалённые по vanished<br/>(и их адреса в холодном слое), новые ключи доставки
    M->>S: folders: fenced = true, cold_packs tenant удаляются<br/>(pack-файлы теперь принадлежат новому шарду), COMMIT
  end
  S-->>W: TenantMoved (папки fenced)
  M->>D: tenants.shard = новый
  M->>N: moved_local: обновить кеш,<br/>остановить акторы ящиков tenant
  W->>T: повтор записи на новом шарде
  N--)N: сессии переподписываются и пересинхронизируются
  M->>S: позже: cleanup_moved, удаление строк tenant
```

Pack-файлы tenant содержат только его письма, поэтому сами объекты не копируются: переезжают строки, которые на них указывают. Удаление по сроку на старом шарде их больше не видит и не трогает.

Если перенос упал до коммита на старом шарде, tenant остаётся на месте, а частичные данные на новом шарде перезапишет следующая попытка. Если упал после коммита, но до переключения, tenant закрыт на старом шарде и ещё не виден на новом: писатели повторяют попытки, а `Move.finish/2` доводит переключение. Сам он пока не продолжает прерванный перенос: `finish` запускают вручную.
