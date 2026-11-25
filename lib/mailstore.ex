defmodule Mailstore do
  @moduledoc """
  Mail storage for an IMAP cluster with compute separated from storage.

  Layout (Phoenix-style contexts: one public module per context, internals in the
  directory of the same name):

    * `Mailstore.Directory` - who exists and where: tenants, domains, accounts,
      routing rules. Internals: `Directory.Schemas.*`, `Directory.Queries`.
    * `Mailstore.Mailbox` - an account's folders and messages on its shard: the
      account actor, write commands, read queries, retention. Internals:
      `Mailbox.AccountServer`, `Mailbox.Commands`, `Mailbox.Queries`,
      `Mailbox.Schemas.*`, `Mailbox.Partitions`.
    * `Mailstore.Delivery` - routing an incoming message (replaces LMTP + sieve).
      Uses only the public APIs of `Directory` and `Mailbox`.
    * `Mailstore.Imap` - the IMAP4rev1 server: connection processes, framing, parser,
      session state machine. Uses only `Directory.authenticate` and `Mailbox`.
    * `Mailstore.Core.*` - pure functions with no database or processes: header
      parsing, flags, message classes, body codec, `NewMessage`.
    * `Mailstore.Telemetry` - :telemetry events, per-command stats, VM snapshot for
      load tests (`bench/`).
    * Infrastructure: `Mailstore.Repo`, `Mailstore.Shards` (tenant -> shard repo),
      `Mailstore.Schema`.

  Rule: a context never touches another context's schemas or queries directly.
  """
end
