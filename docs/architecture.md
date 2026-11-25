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
  queue[("Очередь входящих")]:::planned

  hap["HAProxy × N<br/>:993, неявный TLS"]

  subgraph compute["BEAM-ноды без состояния"]
    n1["mailstore"]
    n2["mailstore"]
  end

  dir[("Directory<br/>tenants, домены, аккаунты,<br/>пересылки, реестр шардов")]

  subgraph storage["Шарды Postgres (Patroni + PgBouncer)"]
    s1[("шард 1<br/>primary + реплика")]
    s2[("шард N<br/>primary + реплика")]:::planned
  end

  obj[("Объектное хранилище<br/>тела закрытых недель")]:::planned

  clients -->|IMAPS| hap
  hap -->|"PROXY v2 + IMAP"| compute
  smtp --> queue
  queue -.->|"консьюмер доставки"| compute
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

На каждое активное соединение — процесс `Imap.Connection`, на каждый активный аккаунт — один актор `AccountServer`, сколько бы сессий его ни открыло. Актор живёт, пока есть подписчики, и ещё `account_idle_timeout` после них. Для открытых папок он держит в памяти все UID и непрочитанные (диапазонами, `Core.UidMap`) и обновляет их на каждой своей записи, поэтому SELECT и UNSEEN не ходят в базу.

```mermaid
flowchart TB
  app["Mailstore.Supervisor<br/>one_for_one"]

  app --> tel["Telemetry"]
  tel --> cs["CommandStats<br/>(ETS-счётчики)"]
  tel --> rep["Reporter<br/>(если MAILSTORE_REPORT_MS)"]

  app --> repo["Repo<br/>пул Postgrex"]

  app --> dc["Directory.Cache<br/>владелец ETS:<br/>accounts, addresses,<br/>forwardings, auth"]

  app --> ms["Mailbox.Supervisor"]
  ms --> reg["Registry<br/>account_id → pid"]
  ms --> asup["AccountSupervisor<br/>DynamicSupervisor"]
  asup --> a1["AccountServer<br/>аккаунт 1"]
  asup --> a2["AccountServer<br/>аккаунт N"]
  ms --> pm["PartitionMaintainer<br/>недельные партиции"]

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
  delivery -->|append| mailbox
  mailbox --> shards --> repo
  directory --> repo
  imap --> core
  delivery --> core
  mailbox --> core
```

## 4. Доставка письма

Письмо разбирается один раз (`Core.NewMessage`: заголовки, ENVELOPE и дерево MIME-частей со смещениями из `Core.Mime`), затем применяются правила пересылки: копия в мастер-инбокс (с фильтром по классу, чтобы warmup туда не попадал), redirect без сохранения оригинала или внешняя пересылка. UID выдаёт база атомарным `UPDATE ... RETURNING`, актор только упорядочивает записи и рассылает изменения сессиям.

```mermaid
sequenceDiagram
  autonumber
  participant Q as Очередь входящих<br/>(сейчас — прямой вызов)
  participant D as Delivery
  participant DC as Directory.Cache<br/>(ETS)
  participant A as AccountServer<br/>получателя
  participant PG as Шард Postgres
  participant C as Connection<br/>(сессия в IDLE)

  Q->>D: deliver(raw, rcpt, class)
  D->>DC: lookup_account(rcpt)
  DC-->>D: account (без запроса в БД)
  D->>DC: forwardings_for(account, class)
  DC-->>D: правила
  D->>A: Mailbox.append("INBOX", msg)
  Note over A: актор стартует по требованию<br/>через Registry + DynamicSupervisor
  A->>PG: BEGIN
  A->>PG: UPDATE folders SET uidnext, highest_modseq,<br/>счётчики RETURNING uid
  A->>PG: INSERT messages (метаданные)
  A->>PG: INSERT message_bodies (тело в zstd)
  A->>PG: COMMIT
  A-->>D: {:ok, [uid]}
  A--)C: {:exists, uids, folder}
  C--)C: "* 42 EXISTS" клиенту
  opt правило copy в мастер-инбокс
    D->>A: append в INBOX целевого аккаунта (его актор)
  end
  opt правило redirect
    Note over D: оригинал не сохраняется
  end
  opt внешний адрес
    D-->>Q: {:external, address} — отправка вне прототипа
  end
```

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

Directory — одна глобальная база (в прототипе — та же, что и шард). Шард хранит почту своих tenants. Внешних ключей между ними нет: в целевой схеме это разные базы.

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
    smallint shard "шард всех данных tenant"
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

  folders ||--o{ messages : "UID-пространство"
  messages ||--|| message_bodies : "тело"
  folders ||--o{ vanished : "удалённые UID"

  folders {
    bigint id PK
    bigint account_id "из directory, без FK"
    text name
    bigint uidvalidity
    bigint uidnext "выдача UID"
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
```

Индексы `messages`: `(folder_id, modseq)` для CONDSTORE, частичный `(folder_id, uid) WHERE NOT \Seen` для UNSEEN, `(tenant_id, message_id_h)` и `thread_id` для тредов.

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
