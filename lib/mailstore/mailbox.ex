defmodule Mailstore.Mailbox do
  @moduledoc """
  Context: the contents of an account (its folders and messages) on the tenant's shard.

  "Mailbox" here means everything one account owns. IMAP also calls a folder a
  "mailbox"; in this code a folder is always called a folder.

  The single entry point for IMAP sessions and delivery:

    * writes go through the account's actor (`Mailbox.AccountServer`), which
      serializes them and pushes deltas to subscribed sessions;
    * reads (FETCH, SEARCH, resync) go straight to the shard, so the actor never
      becomes a read bottleneck.

  Internal modules: `AccountServer` (process), `Commands` (write SQL), `Queries`
  (read queries), `Schemas.*`, `Partitions` (retention).
  """
  alias Mailstore.Core.{BodyCodec, Flags, NewMessage}
  alias Mailstore.Mailbox.{AccountServer, Partitions, Queries}
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

  @doc "BODY[]: the original bytes."
  def fetch_raw(tenant_id, folder_id, uid) do
    case run(tenant_id, :one, Queries.body(folder_id, uid)) do
      {codec, data} -> {:ok, BodyCodec.decode(codec, data)}
      nil -> :error
    end
  end

  @doc "BODY[] for several messages in one query: %{uid => bytes}."
  def fetch_raw_many(_tenant_id, _folder_id, []), do: %{}

  def fetch_raw_many(tenant_id, folder_id, uids) do
    tenant_id
    |> run(:all, Queries.bodies(folder_id, uids))
    |> Map.new(fn {uid, codec, data} -> {uid, BodyCodec.decode(codec, data)} end)
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

  defdelegate ensure_partitions(), to: Partitions, as: :ensure
  defdelegate drop_partitions_before(cutoff), to: Partitions, as: :drop_before

  defp run(tenant_id, fun, query), do: apply(Shards.repo(tenant_id), fun, [query])
end
