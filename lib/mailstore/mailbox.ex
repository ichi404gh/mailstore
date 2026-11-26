defmodule Mailstore.Mailbox do
  @moduledoc """
  Context: the contents of an account (its folders and messages) on the tenant's shard.

  "Mailbox" here means everything one account owns. IMAP also calls a folder a
  "mailbox"; in this code a folder is always called a folder.

  The single entry point for IMAP sessions and delivery:

    * writes go through the account's actor (`Mailbox.AccountServer`), which
      serializes them and pushes deltas to subscribed sessions; except delivery
      (`deliver/1`), which writes a whole batch for many accounts in one transaction
      and then tells the running actors what arrived;
    * reads (FETCH, SEARCH, resync) go straight to the shard, so the actor never
      becomes a read bottleneck.

  Internal modules: `AccountServer` (process), `Commands` (write SQL), `Queries`
  (read queries), `Schemas.*`, `Partitions` (retention).
  """
  alias Mailstore.Core.{BodyCodec, Flags, NewMessage}
  alias Mailstore.Mailbox.{AccountServer, Cold, Commands, Partitions, Queries, Retention}
  alias Mailstore.Mailbox.Schemas.Folder
  alias Mailstore.Shards

  @default_folders [
    {"INBOX", nil},
    {"Sent", "\\Sent"},
    {"Drafts", "\\Drafts"},
    {"Trash", "\\Trash"},
    {"Junk", "\\Junk"}
  ]

  ## Provisioning

  @doc "Create the standard folder set for a new account. Idempotent."
  def create_default_folders(%{id: account_id, tenant_id: tenant_id}) do
    now = DateTime.utc_now()
    repo = Shards.repo(tenant_id)
    # From the sequence, like every folder: see Commands.create_folders/4.
    %{rows: [[uidvalidity]]} = repo.query!("SELECT nextval('folder_uidvalidity')")

    rows =
      for {name, special_use} <- @default_folders do
        %{
          tenant_id: tenant_id,
          account_id: account_id,
          name: name,
          special_use: special_use,
          uidvalidity: uidvalidity,
          inserted_at: now,
          updated_at: now
        }
      end

    repo.insert_all(Folder, rows, on_conflict: :nothing)
    :ok
  end

  ## Writes and live state (through the account actor)

  @doc """
  Select a folder: {:ok, actor_pid, view}; deltas arrive as `{:mailstore, folder_id, event}`.
  The view carries a snapshot from the actor's memory: `:uids` (`Mailstore.Core.UidMap`),
  `:first_unseen`, `:keywords`, `:recent`. `read_only: true` (EXAMINE) does not claim
  \\Recent messages.
  """
  defdelegate subscribe(account_id, folder, opts \\ []), to: AccountServer
  defdelegate unsubscribe(account_id), to: AccountServer

  @doc "SELECT/STATUS data from the actor's memory, no database round trip."
  defdelegate status(account_id, folder), to: AccountServer

  @doc "SEARCH UNSEEN from the actor's memory: {:ok, UidMap of unseen UIDs}."
  defdelegate unseen(account_id, folder), to: AccountServer

  @doc "Append `Mailstore.Core.NewMessage`s. Returns {:ok, uids}."
  defdelegate append(account_id, folder, msgs, opts \\ []), to: AccountServer

  @doc """
  Delivery with group commit: one transaction per shard for the whole batch, then the
  running account actors are told what arrived (they push EXISTS to their sessions).

  `items`: `%{account_id, tenant_id, msg}` plus optional `folder` (default "INBOX")
  and `key` (binary): an item whose key was already delivered to that account is
  reported as `:duplicate` and not stored again, so a batch can be replayed safely.

  Returns `{:ok, results}`, one per item in order: `{:stored, account_id, folder, uid}`,
  `{:duplicate, account_id}` or `{:failed, account_id, reason}`. Database errors raise:
  nothing of the shard's part of the batch was stored then.
  """
  def deliver(items) do
    items
    |> Enum.map(&Map.put_new(&1, :folder, "INBOX"))
    |> Enum.with_index()
    |> deliver_groups(30)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
    |> then(&{:ok, &1})
  end

  # One transaction per shard, the shards in parallel: their transactions are
  # independent, and each waits for its own commit (and standby). A group that hits a
  # tenant moved away (fenced folders) is rolled back; its items are grouped again once
  # the directory says where they live now.
  defp deliver_groups(indexed, attempts) do
    case Enum.group_by(indexed, fn {item, _i} -> Shards.repo(item.tenant_id) end) do
      groups when map_size(groups) == 1 ->
        Enum.flat_map(groups, &deliver_group(&1, attempts))

      groups ->
        groups
        |> Task.async_stream(
          fn group ->
            try do
              {:ok, deliver_group(group, attempts)}
            rescue
              e -> {:raise, e, __STACKTRACE__}
            end
          end,
          max_concurrency: map_size(groups),
          ordered: false,
          timeout: :infinity
        )
        # A shard's error is raised here, in the caller, as it would be without tasks
        # (the other shards' parts may have committed: replaying the batch is safe).
        |> Enum.flat_map(fn
          {:ok, {:ok, results}} -> results
          {:ok, {:raise, e, stacktrace}} -> reraise(e, stacktrace)
        end)
    end
  end

  defp deliver_group({repo, group}, attempts) do
    {items, indexes} = Enum.unzip(group)

    case deliver_tx(repo, items, 3) do
      {results, updates} ->
        for u <- updates, do: AccountServer.delivered(u.account_id, u)

        Enum.zip_with([indexes, items, results], fn [i, item, result] ->
          {i, result(item, result)}
        end)

      :tenant_moved when attempts > 1 ->
        Mailstore.Directory.Cache.refresh()
        Process.sleep(100)
        deliver_groups(group, attempts - 1)

      :tenant_moved ->
        raise Mailstore.Shards.TenantMoved, "tenant still fenced after retries"
    end
  end

  # A deadlock (with an actor's MOVE locking two folders) or a serialization failure
  # aborts the whole transaction; the batch is idempotent, so just run it again.
  defp deliver_tx(repo, items, attempts) do
    {:ok, result} = repo.transaction(fn -> Commands.deliver(repo, items) end)
    result
  rescue
    Mailstore.Shards.TenantMoved ->
      :tenant_moved

    e in Postgrex.Error ->
      if attempts > 1 and e.postgres[:code] in [:deadlock_detected, :serialization_failure],
        do: deliver_tx(repo, items, attempts - 1),
        else: reraise(e, __STACKTRACE__)
  end

  defp result(item, {:stored, _folder_id, uid}), do: {:stored, item.account_id, item.folder, uid}
  defp result(item, :duplicate), do: {:duplicate, item.account_id}
  defp result(item, {:failed, reason}), do: {:failed, item.account_id, reason}

  @doc "op: :add | :remove | :replace. Returns {:ok, [%{uid, flags, modseq}]}."
  defdelegate store_flags(account_id, folder, uids, op, flags), to: AccountServer

  defdelegate expunge(account_id, folder, uids \\ :all), to: AccountServer

  @doc "CREATE with missing parents: :ok | {:error, :invalid_name | :already_exists}."
  defdelegate create_folder(account_id, name), to: AccountServer

  @doc "DELETE: :ok | {:error, :no_such_folder | :inbox | :has_children}."
  defdelegate delete_folder(account_id, name), to: AccountServer

  @doc "SUBSCRIBE / UNSUBSCRIBE: :ok | {:error, :no_such_folder}."
  defdelegate set_subscribed(account_id, name, on?), to: AccountServer

  @doc "RENAME with children: :ok | {:error, reason} (see `AccountServer.rename_folder/3`)."
  defdelegate rename_folder(account_id, old, new), to: AccountServer

  @doc "Returns {:ok, %{uidvalidity, mapping: [{src_uid, dst_uid}]}} (for COPYUID)."
  defdelegate copy(account_id, from, uids, to), to: AccountServer
  defdelegate move(account_id, from, uids, to), to: AccountServer

  ## Reads (straight from the shard)

  def list_folders(tenant_id, account_id),
    do: run(tenant_id, :all, Queries.folders_of_account(account_id))

  @doc "All UIDs of a folder, ascending: the base of an IMAP session's seq -> UID map."
  def uids(tenant_id, folder_id), do: run(tenant_id, :all, Queries.uids(folder_id))

  def unseen_uids(tenant_id, folder_id), do: run(tenant_id, :all, Queries.unseen_uids(folder_id))

  def first_unseen_uid(tenant_id, folder_id),
    do: run(tenant_id, :one, Queries.first_unseen_uid(folder_id))

  @doc "UIDs of a folder as `{first, last}` runs: what a session's UID map is built from."
  def uid_ranges(tenant_id, folder_id), do: run(tenant_id, :all, Queries.uid_ranges(folder_id))

  @doc "Flags, size and internal date (no envelope, no body) for `uids` or `:all`."
  def fetch_flag_rows(_tenant_id, _folder_id, []), do: []

  def fetch_flag_rows(tenant_id, folder_id, uids) do
    tenant_id
    |> run(:all, Queries.flag_rows(folder_id, uids))
    |> Enum.map(fn m -> Map.put(m, :flags, Flags.to_list(m.flags, m.keywords)) end)
  end

  @doc """
  FETCH FLAGS/INTERNALDATE/RFC822.SIZE/ENVELOPE without touching bodies. With
  `structure?` the rows also carry `:bodystructure`, the MIME tree (`Mailstore.Core.Mime`).
  """
  def fetch_meta(tenant_id, folder_id, uids, structure? \\ false) do
    tenant_id
    |> run(:all, Queries.meta(folder_id, uids, structure?))
    |> Enum.map(fn m -> Map.put(m, :flags, Flags.to_list(m.flags, m.keywords)) end)
  end

  @doc "BODY[]: the original bytes, from Postgres or the cold tier."
  def fetch_raw(tenant_id, folder_id, uid) do
    case fetch_raw_many(tenant_id, folder_id, [uid]) do
      %{^uid => raw} -> {:ok, raw}
      _ -> :error
    end
  end

  @doc """
  BODY[] for several messages: %{uid => bytes}. One query for the bodies in Postgres;
  the rest are looked up in the cold tier (`Mailbox.Cold`).
  """
  def fetch_raw_many(_tenant_id, _folder_id, []), do: %{}

  def fetch_raw_many(tenant_id, folder_id, uids) do
    repo = Shards.repo(tenant_id)
    hot = Map.new(repo.all(Queries.bodies(folder_id, uids)), fn {uid, c, d} -> {uid, {c, d}} end)
    cold = Cold.read(repo, folder_id, Enum.reject(uids, &Map.has_key?(hot, &1)))

    hot
    |> Map.merge(cold)
    |> Map.new(fn {uid, {codec, data}} -> {uid, BodyCodec.decode(codec, data)} end)
  end

  @doc "CONDSTORE: flag changes and new messages since `modseq`."
  def changed_since(tenant_id, folder_id, modseq),
    do: run(tenant_id, :all, Queries.changed_since(folder_id, modseq))

  @doc "QRESYNC: UIDs expunged since `modseq`."
  def vanished_since(tenant_id, folder_id, modseq),
    do: run(tenant_id, :all, Queries.vanished_since(folder_id, modseq))

  @doc "Where a Message-ID lives in a tenant (threading, reply matching)."
  def find_by_message_id(tenant_id, message_id) do
    run(
      tenant_id,
      :all,
      Queries.by_message_id(tenant_id, NewMessage.hash64(message_id), message_id)
    )
  end

  ## Retention

  @doc "Make sure partitions exist on every shard."
  def ensure_partitions, do: Enum.each(Shards.all(), &Partitions.ensure/1)

  @doc """
  Retention (`Mailbox.Retention`): on every shard, drop each policy's weeks that ended
  more than its `keep_days` before `today`, and fix the folder counters; the actors of
  the affected accounts reload as their folders are done, and selected sessions see
  `EXPUNGE` for what went (`Mailbox.Partitions`). Returns
  `{:ok, %{weeks: [table], folder_updates: n}}`.
  """
  def apply_retention(today \\ Date.utc_today()) do
    results =
      for repo <- Shards.all(), policy <- Retention.policies() do
        Partitions.expire(repo, policy.id, Date.add(today, -policy.keep_days))
      end

    weeks = Enum.flat_map(results, & &1.weeks)
    folders = results |> Enum.map(& &1.folder_updates) |> Enum.sum()
    {:ok, %{weeks: weeks, folder_updates: folders}}
  end

  defp run(tenant_id, fun, query), do: apply(Shards.repo(tenant_id), fun, [query])
end
