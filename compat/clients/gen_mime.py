"""Write an mbox of random MIME structures (nested multipart, message/rfc822, base64
attachments, RFC 2231 filenames, koi8-r and utf-8 text) for the migration comparison.

    python3 gen_mime.py OUT.mbox [COUNT]
"""
import mailbox
import random
import sys
from email import policy
from email.mime.application import MIMEApplication
from email.mime.message import MIMEMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

random.seed(1)


def leaf(i):
    kind = random.choice(["plain", "html", "bin"])
    if kind == "bin":
        part = MIMEApplication(bytes(random.randrange(256) for _ in range(random.randrange(0, 500))), Name=f"f{i}.bin")
        part.add_header("Content-Disposition", "attachment", filename=f"файл {i}.bin")
        return part
    return MIMEText("строка\n" * random.randrange(0, 5) + "end", kind, random.choice(["utf-8", "koi8-r"]))


def tree(depth, counter=[0]):
    counter[0] += 1
    r = random.random()
    if depth > 3 or r < 0.4:
        return leaf(counter[0])
    if r < 0.55:
        inner = tree(depth + 1)
        inner["Subject"] = f"inner {counter[0]}"
        inner["From"] = "x@y.z"
        return MIMEMessage(inner)
    m = MIMEMultipart(random.choice(["mixed", "alternative", "related"]))
    for _ in range(random.randrange(1, 4)):
        m.attach(tree(depth + 1))
    return m


out = mailbox.mbox(sys.argv[1])
for k in range(int(sys.argv[2]) if len(sys.argv) > 2 else 200):
    m = tree(0)
    if not m.is_multipart() or m.get_content_type() == "message/rfc822":
        outer = MIMEMultipart()
        outer.attach(m)
        m = outer
    m["Subject"] = f"generated {k}"
    m["From"] = "Generator <gen@example.org>"
    out.add(m.as_bytes(policy=policy.SMTP))
out.close()
