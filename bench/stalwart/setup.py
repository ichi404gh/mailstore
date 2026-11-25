"""Configure a local Stalwart for the load generator, through its JMAP management API.

Run with Stalwart in recovery mode (`bench/stalwart/run.sh recovery`, management HTTP on
127.0.0.1:8088), then restart it normally:

    python3 -I bench/stalwart/setup.py --users 5000
    python3 -I bench/stalwart/setup.py --users 5000 --messages 3   # also seed INBOX over LMTP

What it does, idempotently:
  * replaces the default listeners (all interfaces, TLS) with plain ones on 127.0.0.1:
    IMAP 2143, LMTP 2424, HTTP 8088 (management, so this script also works later);
  * lifts the limits that would throttle the benchmark: 8192 connections per listener,
    16 sessions and 2000 commands/minute per account, no LOGIN without TLS;
  * accepts LMTP from localhost without AUTH and turns off the spam filter and the
    email full-text index, which mailstore does not have either;
  * creates the domain and accounts user1..userN with one password.

Stalwart rejects weak passwords, so the default differs from mailstore's "pw"; seed
mailstore with the same one (bench/seed.exs --password) for a like-for-like LOGIN.
"""
import argparse
import base64
import json
import socket
import urllib.request

p = argparse.ArgumentParser()
p.add_argument("--url", default="http://127.0.0.1:8088/jmap/")
p.add_argument("--admin", default="admin:benchpw")
p.add_argument("--users", type=int, default=5000)
p.add_argument("--domain", default="load.test")
p.add_argument("--password", default="kettle-orbit-Quince-47")
p.add_argument("--messages", type=int, default=0, help="seed N messages per INBOX over LMTP")
p.add_argument("--lmtp", default="127.0.0.1:2424")
p.add_argument("--max-connections", type=int, default=200_000)
args = p.parse_args()

AUTH = "Basic " + base64.b64encode(args.admin.encode()).decode()
ACCOUNT = "d333333"  # the admin's management account id in JMAP


def jmap(*calls):
    body = {
        "using": ["urn:ietf:params:jmap:core", "urn:stalwart:jmap"],
        "methodCalls": [[m, {"accountId": ACCOUNT, **a}, str(i)] for i, (m, a) in enumerate(calls)],
    }
    req = urllib.request.Request(
        args.url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": AUTH},
    )
    out = json.load(urllib.request.urlopen(req, timeout=600))["methodResponses"]
    for name, res, _ in out:
        if name == "error":
            raise SystemExit(f"JMAP error: {res}")
        for key in ("notCreated", "notUpdated", "notDestroyed"):
            if res.get(key):
                raise SystemExit(f"{name} {key}: {json.dumps(res[key])[:2000]}")
    return [res for _, res, _ in out]


def listeners():
    want = {
        "imap": ("127.0.0.1:2143", "imap"),
        "lmtp": (args.lmtp, "lmtp"),
        "http": ("127.0.0.1:8088", "http"),
    }
    (current,) = jmap(("x:NetworkListener/get", {"properties": ["name", "bind", "protocol"]}))
    keep = {
        l["id"]
        for l in current["list"]
        if l["name"] in want and list(l["bind"]) == [want[l["name"]][0]]
    }
    have = {l["name"] for l in current["list"] if l["id"] in keep}
    destroy = [l["id"] for l in current["list"] if l["id"] not in keep]
    create = {
        name: {
            "name": name,
            "bind": {addr: True},
            "protocol": proto,
            "useTls": False,
            "maxConnections": args.max_connections,
        }
        for name, (addr, proto) in want.items()
        if name not in have
    }
    # Two calls: within one /set Stalwart creates before it destroys, and names are unique.
    jmap(("x:NetworkListener/set", {"destroy": destroy}))
    jmap(("x:NetworkListener/set", {"create": create}))
    print(f"listeners: removed {len(destroy)}, created {sorted(create)}")


def limits(dom):
    jmap(
        (
            "x:Imap/set",
            {
                "update": {
                    "singleton": {
                        "allowPlainTextAuth": True,
                        "maxConcurrentConnections": 64,
                        "rateLimit": {"count": 1_000_000, "period": 60000},
                    }
                }
            },
        ),
        (
            "x:SystemSettings/set",
            {
                "update": {
                    "singleton": {
                        "maxConnections": args.max_connections,
                        "defaultHostname": "mail." + args.domain,
                        "defaultDomainId": dom,
                    }
                }
            },
        ),
    )
    print("limits: plain-text LOGIN on, connection and rate limits lifted")


def delivery():
    """Like for like with mailstore's Delivery: LMTP from localhost without AUTH, no spam
    filter, no full-text index (mailstore has neither; SEARCH scans the stored text)."""
    lmtp_port = args.lmtp.rsplit(":", 1)[1]
    jmap(
        (
            "x:MtaStageAuth/set",
            {"update": {"singleton": {"require": {"match": {}, "else": f"local_port != 25 && local_port != {lmtp_port}"}}}},
        ),
        # 10 messages per SMTP/LMTP session by default; the seeder and loadgen keep one open.
        ("x:MtaStageData/set", {"update": {"singleton": {"maxMessages": {"match": {}, "else": "100000000"}}}}),
        ("x:SpamSettings/set", {"update": {"singleton": {"enable": False}}}),
        ("x:Search/set", {"update": {"singleton": {"indexEmail": False}}}),
    )
    print("delivery: LMTP without AUTH, spam filter off, email full-text index off")


def domain_id():
    (q,) = jmap(("x:Domain/query", {}))
    if q["ids"]:
        (got,) = jmap(("x:Domain/get", {"ids": q["ids"], "properties": ["name"]}))
        for d in got["list"]:
            if d["name"] == args.domain:
                return d["id"]
    (res,) = jmap(("x:Domain/set", {"create": {"d": {"name": args.domain}}}))
    return res["created"]["d"]["id"]


def accounts(dom):
    existing = set()
    (q,) = jmap(("x:Account/query", {"limit": 1_000_000}))
    for i in range(0, len(q["ids"]), 500):  # maxObjectsInGet
        (got,) = jmap(("x:Account/get", {"ids": q["ids"][i : i + 500], "properties": ["name"]}))
        existing |= {a["name"] for a in got["list"]}

    todo = [f"user{i}" for i in range(1, args.users + 1) if f"user{i}" not in existing]
    for i in range(0, len(todo), 200):
        batch = todo[i : i + 200]
        create = {
            name: {
                "@type": "User",
                "name": name,
                "domainId": dom,
                "credentials": {"0": {"@type": "Password", "secret": args.password}},
            }
            for name in batch
        }
        jmap(("x:Account/set", {"create": create}))
        print(f"accounts: {i + len(batch)}/{len(todo)} created", end="\r", flush=True)
    print(f"\naccounts: {len(existing)} existed, {len(todo)} created")


def seed():
    host, port = args.lmtp.rsplit(":", 1)
    with socket.create_connection((host, int(port))) as s:
        f = s.makefile("rwb")

        def expect(code):
            while True:
                line = f.readline().decode()
                if not line.startswith(code):
                    raise SystemExit(f"LMTP: expected {code}, got {line!r}")
                if line[3] == " ":
                    return

        expect("220")
        f.write(b"LHLO bench\r\n"); f.flush(); expect("250")
        for u in range(1, args.users + 1):
            for m in range(args.messages):
                rcpt = f"user{u}@{args.domain}"
                # Same messages as bench/seed.exs puts into mailstore.
                msg = (
                    f"From: Sender {m + 1} <sender{m + 1}@example.org>\r\nTo: {rcpt}\r\n"
                    f"Subject: seeded {m + 1}\r\nMessage-ID: <seed-{u}-{m + 1}@load>\r\n\r\n"
                    f"seeded body {m + 1}\r\n.\r\n"
                )
                f.write(f"MAIL FROM:<alice@example.org>\r\nRCPT TO:<{rcpt}>\r\nDATA\r\n".encode())
                f.flush(); expect("250"); expect("250"); expect("354")
                f.write(msg.encode()); f.flush(); expect("250")
            if u % 100 == 0:
                print(f"seed: {u}/{args.users} mailboxes", end="\r", flush=True)
        f.write(b"QUIT\r\n"); f.flush()
    print(f"\nseed: {args.messages} messages in each of {args.users} INBOXes")


listeners()
dom = domain_id()
limits(dom)
delivery()
accounts(dom)
if args.messages:
    seed()
