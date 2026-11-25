"""End-to-end check of the IMAP server with Python's stock imaplib.

Usage: python3 -I client.py HOST PORT USER PASSWORD

Prints one line per step. Prints "IDLING" once it is in IDLE and waits for the test
to deliver a message. Exits non-zero on the first failed check.
"""
import imaplib
import sys
import time

host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]


def step(name):
    print("STEP", name, flush=True)


def check(cond, what, data=None):
    if not cond:
        print("FAIL", what, repr(data), flush=True)
        sys.exit(1)


M = imaplib.IMAP4(host, port)

step("capability")
check("IDLE" in M.capabilities and "UIDPLUS" in M.capabilities, "capabilities", M.capabilities)

step("login")
typ, data = M.login(user, password)
check(typ == "OK", "login", data)

step("list")
typ, data = M.list()
check(typ == "OK" and any(b'"INBOX"' in d for d in data), "list INBOX", data)
check(any(b"\\Sent" in d for d in data), "list special-use", data)

step("select")
typ, data = M.select("INBOX")
check(typ == "OK" and data == [b"0"], "select empty INBOX", data)

step("append")
msg = (
    b"From: Alice <alice@example.org>\r\nTo: bob@acme.test\r\n"
    b"Subject: Hello from imaplib\r\nMessage-ID: <imaplib-1@example.org>\r\n"
    b"Date: Fri, 20 Nov 2026 12:00:00 +0000\r\n\r\nHi Bob,\r\nthis came in over APPEND.\r\n"
)
typ, data = M.append("INBOX", r"(\Flagged)", imaplib.Time2Internaldate(time.time()), msg)
check(typ == "OK" and b"[APPENDUID" in data[0], "append", data)

step("uid search unseen")
typ, data = M.uid("SEARCH", None, "UNSEEN")
check(data == [b"1"], "uid search unseen", data)

step("fetch peek header")
typ, data = M.uid("FETCH", "1", "(UID FLAGS RFC822.SIZE BODY.PEEK[HEADER.FIELDS (SUBJECT)])")
check(typ == "OK" and b"Subject: Hello from imaplib" in data[0][1], "peek header", data)
check(b"\\Seen" not in data[0][0], "peek keeps unseen", data)
check(("RFC822.SIZE %d" % len(msg)).encode() in data[0][0], "size matches bytes", data)

step("fetch body sets seen")
typ, data = M.uid("FETCH", "1", "(BODY[])")
check(data[0][1] == msg, "body bytes round-trip", data)
typ, data = M.uid("FETCH", "1", "(FLAGS)")
check(b"\\Seen" in data[0] and b"\\Flagged" in data[0], "seen after fetch", data)

step("store")
typ, data = M.uid("STORE", "1", "+FLAGS", r"(\Answered)")
check(typ == "OK" and b"\\Answered" in data[0], "store", data)

step("search")
typ, data = M.search(None, "SUBJECT", '"from imaplib"')
check(data == [b"1"], "search subject", data)
typ, data = M.search(None, "HEADER", "Message-ID", "<imaplib-1@example.org>")
check(data == [b"1"], "search message-id", data)
typ, data = M.search(None, "FROM", "nobody")
check(data == [b""], "search no match", data)

step("copy")
typ, data = M.uid("COPY", "1", "Trash")
code, info = M.response("COPYUID")
check(typ == "OK" and info and info[0] and info[0].endswith(b" 1 1"), "copy + COPYUID", info)

step("move")
typ, data = M.uid("MOVE", "1", "Junk")
check(typ == "OK", "move", data)
typ, data = M.uid("SEARCH", None, "ALL")
check(data == [b""], "INBOX empty after move", data)

step("expunge")
typ, data = M.select("Junk")
check(data == [b"1"], "Junk has the moved message", data)
M.store("1", "+FLAGS", r"(\Deleted)")
typ, data = M.expunge()
check(typ == "OK" and data == [b"1"], "expunge", data)

step("idle")
M.select("INBOX")
got = None
with M.idle(duration=10) as idler:
    print("IDLING", flush=True)
    for typ, datum in idler:
        if typ == "EXISTS":
            got = datum
            break
check(got == [b"1"], "EXISTS pushed during IDLE", got)
typ, data = M.uid("FETCH", "1:*", "(BODY.PEEK[HEADER.FIELDS (SUBJECT)])")
check(b"pushed while idle" in data[0][1], "pushed message readable", data)

step("logout")
typ, data = M.logout()
check(typ == "BYE", "logout", data)
print("ALL OK", flush=True)
