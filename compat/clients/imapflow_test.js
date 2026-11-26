// What a polling service does with imapflow (a popular Node IMAP client), against mailstore.
//   node imapflow_test.js HOST PORT USER PASS
'use strict';
const { ImapFlow } = require('imapflow');

const [host, port, user, pass] = process.argv.slice(2);
const options = { host, port: Number(port), secure: false, auth: { user, pass }, logger: false };

function check(cond, what) {
  if (!cond) {
    console.log(`  FAIL ${what}`);
    process.exit(1);
  }
  console.log(`  ok   ${what}`);
}

const attachment = Buffer.from('%PDF-1.4 fake attachment bytes \x00\x01\x02 end');
const multipart = [
  'From: Alice <alice@example.org>',
  `To: ${user}`,
  'Subject: Report with attachment',
  'Message-ID: <report-1@compat>',
  'MIME-Version: 1.0',
  'Content-Type: multipart/mixed; boundary="b1"',
  '',
  '--b1',
  'Content-Type: text/plain; charset=utf-8',
  '',
  'See attached.',
  '--b1',
  'Content-Type: application/pdf; name="report.pdf"',
  'Content-Transfer-Encoding: base64',
  'Content-Disposition: attachment; filename="report.pdf"',
  '',
  attachment.toString('base64'),
  '--b1--',
  '',
].join('\r\n');
const plain = `From: bob@example.org\r\nTo: ${user}\r\nSubject: Quick question\r\nMessage-ID: <q-1@compat>\r\n\r\nHi!\r\n`;

(async () => {
  const client = new ImapFlow(options);
  await client.connect();
  check(client.capabilities.has('IDLE') && client.capabilities.has('UIDPLUS'), 'connect, LOGIN, CAPABILITY');

  // Clean slate.
  for (const box of await client.list()) {
    if (box.path.startsWith('Svc')) await client.mailboxDelete(box.path).catch(() => {});
  }
  let lock = await client.getMailboxLock('INBOX');
  try {
    if (client.mailbox.exists) await client.messageDelete('1:*');
  } finally {
    lock.release();
  }

  const list = await client.list();
  const special = Object.fromEntries(list.filter((b) => b.specialUse).map((b) => [b.specialUse, b.path]));
  check(list.some((b) => b.path === 'INBOX') && special['\\Sent'] && special['\\Trash'], 'LIST with special-use folders');

  await client.append('INBOX', multipart, [], new Date('2026-11-20T12:00:00Z'));
  await client.append('INBOX', plain, ['\\Seen']);

  lock = await client.getMailboxLock('INBOX');
  try {
    check(client.mailbox.exists === 2, 'SELECT sees both messages');

    const msgs = [];
    for await (const m of client.fetch('1:*', { uid: true, flags: true, envelope: true, bodyStructure: true, internalDate: true, size: true })) {
      msgs.push(m);
    }
    const report = msgs.find((m) => m.envelope.subject === 'Report with attachment');
    check(report && report.envelope.from[0].address === 'alice@example.org', 'FETCH ENVELOPE');
    check(report.internalDate.toISOString() === '2026-11-20T12:00:00.000Z', 'INTERNALDATE from APPEND');
    const parts = report.bodyStructure.childNodes || [];
    check(report.bodyStructure.type === 'multipart/mixed' && parts.length === 2 && parts[1].type === 'application/pdf'
      && parts[1].dispositionParameters.filename === 'report.pdf', 'FETCH BODYSTRUCTURE');

    const { content } = await client.download(report.uid, '2', { uid: true });
    const chunks = [];
    for await (const c of content) chunks.push(c);
    check(Buffer.concat(chunks).equals(attachment), 'download part 2 (BODY[2]), base64 decoded by the client');

    const unseen = await client.search({ seen: false }, { uid: true });
    check(unseen.length === 1 && unseen[0] === report.uid, 'SEARCH UNSEEN');
    await client.messageFlagsAdd(report.uid, ['\\Seen', '$Processed'], { uid: true });
    const processed = await client.search({ keyword: '$Processed' }, { uid: true });
    check(processed.length === 1, 'STORE flags and a keyword, SEARCH KEYWORD');

    // IDLE: another connection delivers, this one gets EXISTS.
    const arrived = new Promise((resolve) => client.once('exists', resolve));
    client.idle(); // ends by itself when the next command is sent
    await new Promise((r) => setTimeout(r, 300));
    const other = new ImapFlow(options);
    await other.connect();
    await other.append('INBOX', plain.replace('Quick question', 'Pushed while idle'));
    await other.logout();
    const event = await Promise.race([arrived, new Promise((r) => setTimeout(() => r(null), 5000))]);
    check(event && event.count === 3, 'IDLE receives EXISTS pushed by the server');

    await client.mailboxCreate(['Svc', 'Archive']);
    const moved = await client.messageMove(report.uid, 'Svc/Archive', { uid: true });
    check(moved && moved.uidMap && moved.uidMap.size === 1, 'MOVE with COPYUID');
  } finally {
    lock.release();
  }

  const renamed = await client.mailboxRename('Svc/Archive', 'Svc/Done');
  check(renamed.newPath === 'Svc/Done', 'RENAME');
  const st = await client.status('Svc/Done', { messages: true, unseen: true, uidNext: true });
  check(st.messages === 1 && st.unseen === 0, 'STATUS of the renamed folder');
  await client.mailboxDelete('Svc/Done');
  await client.mailboxDelete('Svc');
  check(!(await client.list()).some((b) => b.path.startsWith('Svc')), 'DELETE');

  await client.logout();
  console.log('ALL OK');
})().catch((e) => {
  console.log('  FAIL', e.message, e.responseText || '');
  process.exit(1);
});
