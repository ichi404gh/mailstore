"""Two-way sync with real Maildir clients: mbsync (isync) and OfflineIMAP.

    python3 sync_clients.py HOST PORT USER PASS [mbsync|offlineimap ...]

Each client goes through the same scenario against an emptied account:
  1. server: INBOX with 3 messages (one \\Seen), folder Projects with 1
  2. sync -> the Maildir has the same messages and flags
  3. local: mark one read, delete one (remove the file), new folder LocalOnly with a
     message; sync ->
     the server has the same
  4. server: a new message and a \\Flagged; sync -> both arrive locally
  5. sync again -> nothing changes, the client reports no error
Exits non-zero on the first failed check.
"""
import imaplib
import os
import shutil
import subprocess
import sys
import time

HOST, PORT, USER, PASS = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
CLIENTS = sys.argv[5:] or ["mbsync", "offlineimap"]
DEFAULT = ("INBOX", "Sent", "Drafts", "Trash", "Junk")


def check(cond, what):
    if not cond:
        print(f"  FAIL {what}")
        sys.exit(1)
    print(f"  ok   {what}")


def imap():
    m = imaplib.IMAP4(HOST, PORT)
    m.login(USER, PASS)
    return m


def msg(subject):
    return (f"From: alice@example.org\r\nTo: {USER}\r\nSubject: {subject}\r\n"
            f"Message-ID: <{subject.replace(' ', '-')}-{time.time_ns()}@compat>\r\n\r\n{subject} body\r\n").encode()


def reset():
    m = imap()
    _, listing = m.list()
    names = [line.decode().rsplit(' "/" ', 1)[1].strip('"') for line in listing]
    for name in names:
        m.select(f'"{name}"')
        m.store("1:*", "+FLAGS.SILENT", "(\\Deleted)")
        m.expunge()
    for name in sorted(names, key=len, reverse=True):
        if name not in DEFAULT:
            m.delete(f'"{name}"')
    m.append("INBOX", None, None, msg("first"))
    m.append("INBOX", "(\\Seen)", None, msg("second"))
    m.append("INBOX", None, None, msg("third"))
    m.create("Projects")
    m.append("Projects", None, None, msg("project plan"))
    m.logout()


def server_state():
    """{folder: {subject: sorted flags}} without \\Recent."""
    m = imap()
    _, listing = m.list()
    state = {}
    for line in listing:
        name = line.decode().rsplit(' "/" ', 1)[1].strip('"')
        m.select(f'"{name}"', readonly=True)
        _, data = m.fetch("1:*", "(FLAGS BODY.PEEK[HEADER.FIELDS (SUBJECT)])")
        msgs = {}
        for item in data:
            if isinstance(item, tuple):
                flags = item[0].decode().split("FLAGS (", 1)[1].split(")", 1)[0].split()
                subject = item[1].decode().split(":", 1)[1].strip()
                msgs[subject] = sorted(f for f in flags if f != "\\Recent")
        state[name] = msgs
    m.logout()
    return state


def maildir_state(root):
    """{folder: {subject: flags letters}} from a Maildir++ / Verbatim tree."""
    state = {}
    for dirpath, dirnames, _ in os.walk(root):
        if not ("cur" in dirnames and "new" in dirnames):
            continue
        folder = os.path.relpath(dirpath, root).replace(".", "/").strip("/") or "INBOX"
        msgs = {}
        for sub in ("cur", "new"):
            for f in os.listdir(os.path.join(dirpath, sub)):
                with open(os.path.join(dirpath, sub, f), "rb") as fh:
                    head = fh.read().split(b"\r\n\r\n")[0].split(b"\n\n")[0].decode()
                subject = next(l.split(":", 1)[1].strip() for l in head.splitlines() if l.lower().startswith("subject:"))
                letters = f.split(":2,", 1)[1] if ":2," in f else ""
                msgs[subject] = letters
        state[folder] = msgs
    return state


def maildir_find(root, folder, subject):
    for dirpath, dirnames, _ in os.walk(root):
        if "cur" in dirnames and (os.path.relpath(dirpath, root).replace(".", "/").strip("/") or "INBOX") == folder:
            for sub in ("cur", "new"):
                for f in os.listdir(os.path.join(dirpath, sub)):
                    path = os.path.join(dirpath, sub, f)
                    if f"Subject: {subject}".encode() in open(path, "rb").read():
                        return dirpath, path
    raise SystemExit(f"  FAIL {subject} not found locally in {folder}")


def set_letters(dirpath, path, letters):
    base = os.path.basename(path).split(":2,")[0]
    new = os.path.join(dirpath, "cur", f"{base}:2,{''.join(sorted(letters))}")
    os.rename(path, new)


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout[-3000:], p.stderr[-3000:])
    check(p.returncode == 0, "sync: " + " ".join(cmd[:2]))
    return p


def scenario(name, root, sync, local_folder):
    print(f"== {name}")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    reset()

    sync()
    local = maildir_state(root)
    check(set(local.get("INBOX", {})) == {"first", "second", "third"}, "INBOX downloaded")
    check("S" in local["INBOX"]["second"] and "S" not in local["INBOX"]["first"], "\\Seen became S")
    check(set(local.get("Projects", {})) == {"project plan"}, "Projects downloaded")

    d, p = maildir_find(root, "INBOX", "first")
    set_letters(d, p, "S")
    # Deleting = removing the file (OfflineIMAP treats the T flag as \\Deleted only).
    _, p = maildir_find(root, "INBOX", "third")
    os.remove(p)
    lo = local_folder(root, "LocalOnly")
    for sub in ("cur", "new", "tmp"):
        os.makedirs(os.path.join(lo, sub), exist_ok=True)
    with open(os.path.join(lo, "new", f"{int(time.time())}.local.compat"), "wb") as fh:
        fh.write(msg("written offline"))

    sync()
    server = server_state()
    check(server["INBOX"].get("first") == ["\\Seen"], "S became \\Seen on the server")
    check("third" not in server["INBOX"], "locally deleted message expunged on the server")
    check(set(server.get("LocalOnly", {})) == {"written offline"}, "local folder and message uploaded")

    m = imap()
    m.append("INBOX", None, None, msg("arrived later"))
    m.select("INBOX")
    _, data = m.search(None, "SUBJECT", '"second"')
    m.store(data[0].decode().split()[0], "+FLAGS", "(\\Flagged)")
    m.logout()

    sync()
    local = maildir_state(root)
    check("arrived later" in local["INBOX"], "new server message downloaded")
    check("F" in local["INBOX"]["second"], "\\Flagged became F")

    before = server_state()
    sync()
    check(maildir_state(root) == local and server_state() == before, "second sync is a no-op")


def mbsync():
    root = "/tmp/mbsync-mail"
    rc = "/tmp/mbsyncrc"
    with open(rc, "w") as f:
        f.write(f"""IMAPAccount ms
Host {HOST}
Port {PORT}
User {USER}
Pass {PASS}
TLSType None
AuthMechs LOGIN

IMAPStore ms-remote
Account ms

MaildirStore ms-local
Path {root}/
Inbox {root}/INBOX
SubFolders Verbatim

Channel ms
Far :ms-remote:
Near :ms-local:
Patterns *
Create Both
Remove Both
Expunge Both
SyncState *
""")
    scenario("mbsync", root, lambda: run(["mbsync", "-c", rc, "-a"]),
             lambda r, n: os.path.join(r, n))


def offlineimap():
    root = "/tmp/offlineimap-mail"
    rc = "/tmp/offlineimaprc"
    with open(rc, "w") as f:
        f.write(f"""[general]
accounts = ms
metadata = /tmp/offlineimap-meta
[Account ms]
localrepository = local
remoterepository = remote
[Repository local]
type = Maildir
localfolders = {root}
[Repository remote]
type = IMAP
remotehost = {HOST}
remoteport = {PORT}
remoteuser = {USER}
remotepass = {PASS}
ssl = no
starttls = no
""")
    shutil.rmtree("/tmp/offlineimap-meta", ignore_errors=True)
    scenario("offlineimap", root, lambda: run(["offlineimap", "-c", rc, "-o", "-u", "basic"]),
             lambda r, n: os.path.join(r, n))


for client in CLIENTS:
    {"mbsync": mbsync, "offlineimap": offlineimap}[client]()
print("ALL OK")
