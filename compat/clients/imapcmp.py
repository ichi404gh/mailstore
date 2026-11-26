"""Seed an IMAP server from mbox files, or compare two servers message by message.

    python3 imapcmp.py seed    HOST PORT USER PASS MBOX...      (IMAPS, certificate not checked)
    python3 imapcmp.py wipe    HOST PORT USER PASS              (plain IMAP: empty every folder)
    python3 imapcmp.py compare A_HOST A_PORT A_TLS B_HOST B_PORT B_TLS USER_A USER_B PASS

`compare` pairs messages by SHA-256 of their bytes and checks, per folder: count,
FLAGS (without \\Recent), INTERNALDATE, RFC822.SIZE, ENVELOPE and BODYSTRUCTURE. It
prints every difference and exits non-zero if there is one. Used to check that a
migration (imapsync) from Dovecot keeps everything, and that mailstore renders
ENVELOPE / BODYSTRUCTURE the way Dovecot does.
"""
import hashlib
import imaplib
import mailbox
import ssl
import sys

FOLDERS = ["INBOX", "Work", "Archive", "Archive/2007"]
FLAG_SETS = ["", "\\Seen", "\\Seen \\Flagged", "\\Answered \\Seen", "$Label1", "\\Draft", "\\Seen $Work $Label2"]


def connect(host, port, tls):
    if tls:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return imaplib.IMAP4_SSL(host, int(port), ssl_context=ctx)
    return imaplib.IMAP4(host, int(port))


def seed(host, port, user, password, paths):
    m = connect(host, port, True)
    m.login(user, password)
    for f in FOLDERS[1:]:
        m.create(f)
    n = 0
    for path in paths:
        box = mailbox.mbox(path, create=False)
        for key in box.keys():
            raw = box.get_bytes(key).replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")
            # imapsync identifies messages by headers (Message-ID by default) and skips
            # those without them or with duplicates; the imaptest corpus has both. A
            # unique header lets it copy everything (imapsync --useheader X-Compat-Seq).
            raw = f"X-Compat-Seq: {n}\r\n".encode() + raw
            folder = FOLDERS[n % len(FOLDERS)]
            flags = FLAG_SETS[n % len(FLAG_SETS)]
            date = imaplib.Time2Internaldate(1_170_000_000 + n * 3_601)
            typ, data = m.append(folder, f"({flags})" if flags else None, date, raw)
            if typ != "OK":
                raise SystemExit(f"APPEND failed for {path}#{key}: {data}")
            n += 1
    m.logout()
    print(f"seeded {n} messages into {len(FOLDERS)} folders")


def wipe(host, port, user, password):
    m = connect(host, port, False)
    m.login(user, password)
    _, listing = m.list()
    names = [line.decode().rsplit(' "/" ', 1)[1].strip('"') for line in listing]
    for name in names:
        if m.select(f'"{name}"')[0] == "OK":
            m.store("1:*", "+FLAGS.SILENT", "(\\Deleted)")
            m.expunge()
    # Extra folders go, children before parents; the default set stays.
    for name in sorted(names, key=len, reverse=True):
        if name not in ("INBOX", "Sent", "Drafts", "Trash", "Junk"):
            m.delete(f'"{name}"')
    m.logout()


## A small IMAP response parser: enough for FETCH data.

def tokenize(data):
    """Yield tokens from raw response bytes: '(' ')' and values (bytes or None for NIL)."""
    i, n = 0, len(data)
    while i < n:
        c = data[i:i + 1]
        if c in b" \r\n":
            i += 1
        elif c in b"()":
            yield c.decode()
            i += 1
        elif c == b'"':
            j, out = i + 1, bytearray()
            while data[j:j + 1] != b'"':
                if data[j:j + 1] == b"\\":
                    j += 1
                out += data[j:j + 1]
                j += 1
            yield bytes(out)
            i = j + 1
        elif c == b"{":
            j = data.index(b"}", i)
            size = int(data[i + 1:j].rstrip(b"+"))
            start = data.index(b"\n", j) + 1
            yield data[start:start + size]
            i = start + size
        else:
            j = i
            while j < n and data[j:j + 1] not in b" ()\r\n":
                if data[j:j + 1] == b"[":  # BODY[...] stays one atom
                    j = data.index(b"]", j)
                j += 1
            atom = data[i:j]
            yield None if atom.upper() == b"NIL" else atom
            i = j


def parse(tokens):
    out = []
    for t in tokens:
        if t == "(":
            out.append(parse(tokens))
        elif t == ")":
            return out
        else:
            out.append(t)
    return out


def fetch_all(m, folder):
    typ, _ = m.select(f'"{folder}"', readonly=True)
    if typ != "OK":
        return None
    typ, data = m.uid("FETCH", "1:*", "(FLAGS INTERNALDATE RFC822.SIZE ENVELOPE BODYSTRUCTURE BODY.PEEK[])")
    raw = bytearray()
    for part in data:
        if part is None:
            continue
        if isinstance(part, tuple):
            head, literal = part
            raw += head + b"\r\n" + literal
        else:
            raw += part + b"\r\n"
    msgs = {}
    tokens = tokenize(bytes(raw))
    for item in iter(lambda: next(tokens, StopIteration), StopIteration):
        if item != "(":
            continue
        kv = parse(tokens)
        d = {kv[i].upper().decode(): kv[i + 1] for i in range(0, len(kv) - 1, 2)}
        body = d.get("BODY[]") or b""
        flags = sorted(f.decode() for f in d.get("FLAGS", []) if f.lower() != b"\\recent")
        msgs[hashlib.sha256(body).hexdigest()] = {
            "FLAGS": flags,
            "INTERNALDATE": d.get("INTERNALDATE"),
            "RFC822.SIZE": d.get("RFC822.SIZE"),
            "ENVELOPE": d.get("ENVELOPE"),
            "BODYSTRUCTURE": d.get("BODYSTRUCTURE"),
            "subject": d.get("ENVELOPE", [None, None])[1],
        }
    return msgs


def show(v):
    """Render a parsed value back as IMAP-ish text, for readable diffs."""
    if v is None:
        return "NIL"
    if isinstance(v, list):
        return "(" + " ".join(show(x) for x in v) + ")"
    return '"' + v.decode("utf-8", "replace") + '"'


def compare(a_host, a_port, a_tls, b_host, b_port, b_tls, user_a, user_b, password):
    a = connect(a_host, a_port, a_tls == "tls")
    b = connect(b_host, b_port, b_tls == "tls")
    a.login(user_a, password)
    b.login(user_b, password)
    problems = 0
    total = 0
    for folder in FOLDERS:
        ma, mb = fetch_all(a, folder), fetch_all(b, folder)
        if ma is None or mb is None:
            print(f"[{folder}] missing on {'A' if ma is None else 'B'}")
            problems += 1
            continue
        if len(ma) != len(mb):
            print(f"[{folder}] {len(ma)} messages on A, {len(mb)} on B")
            problems += 1
        for key, x in ma.items():
            total += 1
            y = mb.get(key)
            if y is None:
                print(f"[{folder}] body {key[:12]} ({x['subject']!r}) missing on B")
                problems += 1
                continue
            for field in ["FLAGS", "INTERNALDATE", "RFC822.SIZE", "ENVELOPE", "BODYSTRUCTURE"]:
                if x[field] != y[field]:
                    problems += 1
                    print(f"[{folder}] {key[:12]} {x['subject']!r} {field} differs:\n"
                          f"  A: {show(x[field]) if field != 'FLAGS' else x[field]}\n"
                          f"  B: {show(y[field]) if field != 'FLAGS' else y[field]}")
    print(f"compared {total} messages in {len(FOLDERS)} folders: {problems} differences")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    cmd, *rest = sys.argv[1:]
    if cmd == "seed":
        seed(rest[0], rest[1], rest[2], rest[3], rest[4:])
    elif cmd == "wipe":
        wipe(*rest)
    elif cmd == "compare":
        compare(*rest)
    else:
        raise SystemExit(__doc__)
