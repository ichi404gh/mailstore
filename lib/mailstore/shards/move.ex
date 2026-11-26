defmodule Mailstore.Shards.Move do
  @moduledoc """
  Moving a tenant to another shard while it keeps receiving mail and serving IMAP.

  1. **Snapshot**: per folder, HIGHESTMODSEQ and UIDNEXT on the source.
  2. **Bulk copy**, no locks: messages and bodies (folder by folder, in UID order, in
     chunks), the expunge log, delivery keys. Writers keep writing to the source.
  3. **Freeze**, one source transaction: the tenant's move lock (stops folder
     creation) and a lock on each of its folder rows, which every write takes anyway
     (`Mailbox.Commands`), so the tenant's writes wait. Then the delta goes to the
     target in one target transaction:
     * folder rows, as they are now;
     * messages with UID >= the snapshot UIDNEXT (new since), with bodies;
     * flags of older messages whose MODSEQ moved past the snapshot;
     * messages expunged since (`vanished` past the snapshot MODSEQ) are deleted;
     * new delivery keys.
     The target commits, then the source folders are marked `fenced` and the source
     commits: the writers that waited now see the mark and raise `TenantMoved`.
  4. **Switch**: `tenants.shard` in the directory, then every node refreshes its cache
     and stops the tenant's actors. Writers retry on the new shard, sessions
     resubscribe and resync. Writes are paused for the freeze only (a delta of
     seconds of traffic), not for the bulk copy.
  5. **Cleanup** (`cleanup/2`), later: the tenant's rows leave the source.

  Folder ids are kept (shards allocate disjoint id ranges, `Mailstore.Shards`), so
  messages, UIDs, UIDVALIDITY and MODSEQs move unchanged: clients notice nothing but
  a resync.

  A failure before the source commit leaves the tenant on the source, with partial
  data on the target (the next attempt copies with ON CONFLICT and overwrites the
  folders). After the source commit and before the switch the tenant is fenced on the
  source and not yet visible on the target: writers retry for a few seconds, and
  `finish/2` completes a move interrupted there.
  """
  import Ecto.Query
  require Logger

  alias Mailstore.{Repo, Shards}
  alias Mailstore.Directory.Schemas.Account
  alias Mailstore.Mailbox.{AccountServer, Commands, Placement}
  alias Mailstore.Mailbox.Schemas.{Folder, Message, MessageBody, Vanished}

  @chunk 500

  @doc "Move `tenant_id` to shard `target_id`. Returns `{:ok, stats}`."
  def run(tenant_id, target_id) do
    source_id = current_shard(tenant_id)

    if source_id == target_id do
      {:error, :already_there}
    else
      source = Shards.repo_for(source_id)
      target = Shards.repo_for(target_id)
      accounts = accounts_of(tenant_id)

      # No expiry on either shard while the tenant's rows are copied.
      Mailstore.Mailbox.Partitions.with_maintenance_lock(
        [{source_id, source}, {target_id, target}],
        fn -> copy_and_switch(tenant_id, source_id, target_id, source, target, accounts) end
      )
    end
  end

  defp copy_and_switch(tenant_id, source_id, target_id, source, target, accounts) do
    started = System.monotonic_time(:millisecond)
    snapshot = snapshot(source, accounts)
    prepare_partitions(source, target, accounts)
    copied = bulk_copy(source, target, Map.keys(snapshot))
    copy_deliveries(source, target, accounts)

    frozen_at = System.monotonic_time(:millisecond)
    delta = freeze(source, target, tenant_id, accounts, snapshot)
    frozen_ms = System.monotonic_time(:millisecond) - frozen_at

    finish(tenant_id, target_id, accounts)

    stats = %{
      from: source_id,
      to: target_id,
      folders: map_size(snapshot),
      messages: copied,
      delta: delta,
      frozen_ms: frozen_ms,
      total_ms: System.monotonic_time(:millisecond) - started
    }

    Logger.info("tenant #{tenant_id} moved: #{inspect(stats)}")
    {:ok, stats}
  end

  @doc "The switch: point the tenant at `target_id` and tell every node."
  def finish(tenant_id, target_id, accounts \\ nil) do
    accounts = accounts || accounts_of(tenant_id)

    Repo.query!("UPDATE tenants SET shard = $2, updated_at = now() WHERE id = $1", [
      tenant_id,
      target_id
    ])

    for n <- Placement.members(), do: :erpc.call(n, __MODULE__, :moved_local, [accounts], 30_000)
    :ok
  end

  @doc false
  def moved_local(accounts) do
    :ok = Mailstore.Directory.Cache.refresh()
    AccountServer.stop_local(accounts)
  end

  @doc """
  Remove a moved tenant's rows from the shard it left. Refuses while the directory
  still places the tenant there.
  """
  def cleanup(tenant_id, shard_id) do
    if current_shard(tenant_id) == shard_id do
      {:error, :tenant_still_there}
    else
      repo = Shards.repo_for(shard_id)
      accounts = accounts_of(tenant_id)

      {:ok, n} =
        repo.transaction(fn ->
          ids =
            repo.all(
              from(f in Folder, where: f.account_id in ^accounts and f.fenced, select: f.id)
            )

          for table <- ~w(message_bodies messages vanished),
              do: repo.query!("DELETE FROM #{table} WHERE folder_id = ANY($1)", [ids])

          repo.query!("DELETE FROM deliveries WHERE account_id = ANY($1)", [accounts])
          repo.query!("DELETE FROM folders WHERE id = ANY($1)", [ids])
          length(ids)
        end)

      {:ok, %{folders: n}}
    end
  end

  defp accounts_of(tenant_id),
    do: Repo.all(from(a in Account, where: a.tenant_id == ^tenant_id, select: a.id))

  defp current_shard(tenant_id),
    do: Repo.one!(from(t in "tenants", where: t.id == ^tenant_id, select: t.shard))

  defp snapshot(source, accounts) do
    from(f in Folder,
      where: f.account_id in ^accounts,
      select: {f.id, {f.highest_modseq, f.uidnext}}
    )
    |> source.all()
    |> Map.new()
  end

  # The target needs the weeks and days the tenant's rows fall into.
  defp prepare_partitions(source, target, accounts) do
    %{rows: [[oldest]]} =
      source.query!(
        """
        SELECT min(m.stored_at) FROM messages m JOIN folders f ON f.id = m.folder_id
        WHERE f.account_id = ANY($1)
        """,
        [accounts]
      )

    if oldest do
      from = DateTime.to_date(oldest)
      weeks = div(Date.diff(Date.utc_today(), from), 7) + 13
      Mailstore.Mailbox.Partitions.ensure(target, from, weeks)
    end

    %{rows: [[first_day]]} =
      source.query!("SELECT min(created_at) FROM deliveries WHERE account_id = ANY($1)", [
        accounts
      ])

    if first_day do
      from = DateTime.to_date(first_day)

      target.query!("SELECT mailstore_ensure_delivery_partitions($1, $2)", [
        from,
        Date.diff(Date.utc_today(), from) + 8
      ])
    end
  end

  defp bulk_copy(source, target, folder_ids) do
    copied =
      Enum.reduce(folder_ids, 0, fn fid, n -> n + copy_messages(source, target, fid, 0) end)

    rows = source.all(from(v in Vanished, where: v.folder_id in ^folder_ids))
    insert(target, Vanished, Enum.map(rows, &fields(&1, Vanished)), on_conflict: :nothing)
    copied
  end

  # Messages of a folder with uid >= from_uid, with bodies, in chunks.
  defp copy_messages(source, target, fid, from_uid, total \\ 0) do
    rows =
      source.all(
        from(m in Message,
          where: m.folder_id == ^fid and m.uid >= ^from_uid,
          order_by: m.uid,
          limit: @chunk
        )
      )

    case rows do
      [] ->
        total

      rows ->
        uids = Enum.map(rows, & &1.uid)
        bodies = source.all(from(b in MessageBody, where: b.folder_id == ^fid and b.uid in ^uids))
        insert(target, Message, Enum.map(rows, &fields(&1, Message)), on_conflict: :nothing)

        insert(target, MessageBody, Enum.map(bodies, &fields(&1, MessageBody)),
          on_conflict: :nothing
        )

        copy_messages(source, target, fid, List.last(uids) + 1, total + length(rows))
    end
  end

  defp copy_deliveries(source, target, accounts) do
    %{rows: rows} =
      source.query!(
        "SELECT account_id, key, created_at FROM deliveries WHERE account_id = ANY($1)",
        [accounts]
      )

    for chunk <- Enum.chunk_every(rows, 5_000) do
      [a, k, c] = chunk |> Enum.zip() |> Enum.map(&Tuple.to_list/1)

      target.query!(
        """
        INSERT INTO deliveries (account_id, key, created_at)
        SELECT * FROM unnest($1::bigint[], $2::bytea[], $3::timestamptz[])
        ON CONFLICT DO NOTHING
        """,
        [a, k, c]
      )
    end

    :ok
  end

  defp freeze(source, target, tenant_id, accounts, snapshot) do
    {:ok, delta} =
      source.transaction(
        fn ->
          source.query!("SELECT pg_advisory_xact_lock(#{Commands.move_lock()}, $1::int)", [
            tenant_id
          ])

          folders =
            source.all(
              from(f in Folder,
                where: f.account_id in ^accounts,
                order_by: f.id,
                lock: "FOR UPDATE"
              )
            )

          {:ok, delta} =
            target.transaction(fn -> apply_delta(source, target, accounts, folders, snapshot) end)

          source.query!("UPDATE folders SET fenced = true WHERE account_id = ANY($1)", [accounts])
          delta
        end,
        timeout: :infinity
      )

    delta
  end

  defp apply_delta(source, target, accounts, folders, snapshot) do
    insert(target, Folder, Enum.map(folders, &fields(&1, Folder)),
      on_conflict: {:replace_all_except, [:id]},
      conflict_target: :id
    )

    stats =
      Enum.reduce(folders, %{new: 0, flags: 0, expunged: 0}, fn f, acc ->
        {modseq, uidnext} = Map.get(snapshot, f.id, {0, 1})
        new = copy_messages(source, target, f.id, uidnext)

        changed =
          source.all(
            from(m in Message,
              where: m.folder_id == ^f.id and m.uid < ^uidnext and m.modseq > ^modseq,
              select: {m.uid, m.flags, m.keywords, m.modseq}
            )
          )

        for {uid, flags, keywords, mseq} <- changed do
          target.query!(
            "UPDATE messages SET flags = $3, keywords = $4, modseq = $5 WHERE folder_id = $1 AND uid = $2",
            [f.id, uid, flags, keywords, mseq]
          )
        end

        gone = source.all(from(v in Vanished, where: v.folder_id == ^f.id and v.modseq > ^modseq))
        gone_uids = Enum.map(gone, & &1.uid)

        if gone_uids != [] do
          for table <- ~w(message_bodies messages) do
            target.query!("DELETE FROM #{table} WHERE folder_id = $1 AND uid = ANY($2)", [
              f.id,
              gone_uids
            ])
          end

          insert(target, Vanished, Enum.map(gone, &fields(&1, Vanished)),
            on_conflict: {:replace, [:modseq]},
            conflict_target: [:folder_id, :uid]
          )
        end

        %{
          acc
          | new: acc.new + new,
            flags: acc.flags + length(changed),
            expunged: acc.expunged + length(gone_uids)
        }
      end)

    copy_deliveries(source, target, accounts)

    # A folder created again on the target must not reuse a moved folder's UIDVALIDITY.
    target.query!(
      "SELECT setval('folder_uidvalidity', GREATEST((SELECT last_value FROM folder_uidvalidity), $1))",
      [Enum.max(Enum.map(folders, & &1.uidvalidity), fn -> 1 end)]
    )

    stats
  end

  defp insert(_repo, _schema, [], _opts), do: :ok

  defp insert(repo, schema, rows, opts) do
    for chunk <- Enum.chunk_every(rows, 500), do: repo.insert_all(schema, chunk, opts)
    :ok
  end

  defp fields(struct, schema), do: Map.take(struct, schema.__schema__(:fields))
end
