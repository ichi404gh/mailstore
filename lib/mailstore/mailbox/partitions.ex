defmodule Mailstore.Mailbox.Partitions do
  @moduledoc """
  Partitions of a shard. `messages` and `message_bodies`: by retention policy, then by
  week of `stored_at` (`messages_p<id>_<IYYY>w<IW>`, see `Mailstore.Mailbox.Retention`).
  `deliveries`: by day.

  Expiring a week never blocks traffic and survives a crash at any step:

  1. `DETACH PARTITION ... CONCURRENTLY` (a detach left pending by a crash is
     finalized). From here no query sees the week, and nothing can change it.
  2. One transaction replaces the detached table by its per-folder totals
     (`retention_pending_<name>`): messages and unseen per folder.
  3. Folder counters are reduced in chunks of folders, each chunk in its own short
     transaction that also deletes its rows from the totals, in folder id order like
     every writer (no deadlocks, no folder locked for long). HIGHESTMODSEQ goes up.
     Right after each chunk the actors of its accounts reload, wherever they run, and
     their selected sessions get EXPUNGE: a client sees the old state only between the
     detach and its folder's chunk.
  4. The bodies of the week are detached and dropped the same way, without totals,
     and so are its cold-tier locations (`cold_bodies`); then its packs are deleted
     from the object store (`Mailbox.Cold`).

  Expiry and tenant moves exclude each other per shard (`with_maintenance_lock/2`): a
  move copies folder rows and messages, and an expiry between those copies would
  leave the counters on the new shard short.

  A crashed run leaves detached tables or totals behind; the next run finds them by
  name and goes on. UIDs removed this way are not logged in `vanished`: a session
  learns them on resync (`EXPUNGE`), and QRESYNC may answer VANISHED with UIDs that
  never existed, so it can be computed from the UIDs that remain.
  """
  @chunk 1_000
  # Session advisory lock (key, policy id): one expiry per shard and policy at a time,
  # whichever node runs it.
  @expire_lock 7002
  # Shard maintenance: expiry holds it shared, a tenant move exclusive.
  @maintenance_lock 7003
  @lock_timeout "10s"
  @week ~r/^(messages|message_bodies|cold_bodies)_p(\d+)_(\d{4})w(\d{2})$/

  @doc """
  Make sure partitions exist: for every retention policy, weekly message partitions
  from the week of `from` for `weeks` weeks; daily `deliveries` partitions from
  yesterday a week ahead.
  """
  def ensure(repo, from \\ Date.add(Date.utc_today(), -7), weeks \\ 5) do
    repo.query!("SELECT mailstore_ensure_partitions($1::smallint[], $2, $3)", [
      Mailstore.Mailbox.Retention.ids(),
      from,
      weeks
    ])

    repo.query!("SELECT mailstore_ensure_delivery_partitions($1, $2)", [
      Date.add(Date.utc_today(), -1),
      8
    ])

    :ok
  end

  @doc """
  Forget deliveries recorded before `cutoff` (whole days): the dedup horizon. It must be
  longer than anything can be replayed from, i.e. the retention of the source topics.
  Returns the dropped partitions.
  """
  def drop_deliveries_before(repo, %Date{} = cutoff) do
    %{rows: rows} =
      repo.query!("""
      SELECT c.relname
      FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
      WHERE i.inhparent = 'deliveries'::regclass
      ORDER BY c.relname
      """)

    limit = "deliveries_" <> Calendar.strftime(cutoff, "%Y%m%d")

    for [name] <- rows, Regex.match?(~r/^deliveries_\d{8}$/, name), name < limit do
      repo.query!("DROP TABLE #{name}")
      name
    end
  end

  @doc """
  Weeks of policy `id` that ended on or before `cutoff`: `[{suffix, attached?}]` for
  `messages` (and the same for `message_bodies` with `table: "message_bodies"`),
  including tables a crashed run detached.
  """
  def expired(repo, id, %Date{} = cutoff, table \\ "messages") do
    %{rows: rows} =
      repo.query!(
        """
        SELECT c.relname, EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)
        FROM pg_class c
        WHERE c.relkind IN ('r', 'p') AND c.relnamespace = 'public'::regnamespace
          AND c.relname LIKE $1
        ORDER BY c.relname
        """,
        ["#{table}_p#{id}_%"]
      )

    for [name, attached] <- rows,
        [_, ^table, pid, year, week] <- [Regex.run(@week, name)],
        String.to_integer(pid) == id,
        week_start(String.to_integer(year), String.to_integer(week))
        |> Date.add(7)
        |> Date.compare(cutoff) != :gt,
        do: {name, attached}
  end

  @doc """
  Expire policy `id` on this shard up to `cutoff` (a date: weeks that ended on or before
  it go). Runs outside a transaction on one connection. Returns the expired week tables
  and how many folder counter updates were made (a folder counts once per week); `skipped: true` when another run
  holds the shard's lock for this policy.
  """
  def expire(repo, id, %Date{} = cutoff) do
    repo.checkout(fn ->
      %{rows: [[locked]]} =
        repo.query!(
          """
          SELECT pg_try_advisory_lock(#{@expire_lock}, $1)
             AND pg_try_advisory_lock_shared(#{@maintenance_lock}, 0)
          """,
          [id]
        )

      try do
        if locked,
          do: locked_expire(repo, id, cutoff),
          else: %{weeks: [], folder_updates: 0, skipped: true}
      after
        # Whatever was taken (the AND may have stopped after the first).
        repo.query!("SELECT pg_advisory_unlock_all()")
      end
    end)
  end

  @doc """
  Run `fun` holding the maintenance lock of every given shard (`[{shard_id, repo}]`)
  exclusively, taken in shard id order (two moves cannot deadlock). Waits for running
  expiries to finish; expiries started meanwhile skip these shards. Every query `fun`
  makes through these repos runs on the connection that holds the lock.
  """
  def with_maintenance_lock(shards, fun) do
    shards
    |> Enum.uniq_by(&elem(&1, 0))
    |> Enum.sort_by(&elem(&1, 0), :desc)
    |> Enum.reduce(fun, fn {_id, repo}, inner ->
      fn ->
        repo.checkout(fn ->
          repo.query!("SELECT pg_advisory_lock(#{@maintenance_lock}, 0)", [], timeout: :infinity)

          try do
            inner.()
          after
            repo.query!("SELECT pg_advisory_unlock(#{@maintenance_lock}, 0)")
          end
        end)
      end
    end)
    |> then(& &1.())
  end

  defp locked_expire(repo, id, cutoff) do
    repo.query!("SET lock_timeout = '#{@lock_timeout}'")

    try do
      finished = finish_pending(repo, "messages_p#{id}_")
      weeks = expired(repo, id, cutoff)

      folders =
        Enum.reduce(weeks, finished, fn {name, attached}, n ->
          if attached, do: detach(repo, "messages_p#{id}", name)
          pending = "retention_pending_" <> name

          {:ok, _} =
            repo.transaction(fn ->
              repo.query!("""
              CREATE TABLE #{pending} AS
              SELECT folder_id, count(*) AS n, count(*) FILTER (WHERE (flags & 1) = 0) AS unseen
              FROM #{name} GROUP BY folder_id
              """)

              repo.query!("DROP TABLE #{name}")
            end)

          n + apply_pending(repo, pending)
        end)

      for table <- ~w(message_bodies cold_bodies),
          {name, attached} <- expired(repo, id, cutoff, table) do
        if attached, do: detach(repo, "#{table}_p#{id}", name)
        repo.query!("DROP TABLE #{name}")
      end

      # The week's locations are gone: nothing refers to its packs any more.
      Mailstore.Mailbox.Cold.delete_expired_packs(repo, id, cutoff)

      %{weeks: Enum.map(weeks, &elem(&1, 0)), folder_updates: folders}
    after
      repo.query!("RESET lock_timeout")
    end
  end

  # Totals left by a crashed run.
  defp finish_pending(repo, prefix) do
    %{rows: rows} =
      repo.query!(
        "SELECT relname FROM pg_class WHERE relkind = 'r' AND relname LIKE $1 ORDER BY relname",
        ["retention_pending_#{prefix}%"]
      )

    for [pending] <- rows,
        String.starts_with?(pending, "retention_pending_" <> prefix),
        reduce: 0 do
      n -> n + apply_pending(repo, pending)
    end
  end

  defp apply_pending(repo, pending, total \\ 0) do
    {:ok, {n, accounts}} =
      repo.transaction(fn ->
        %{rows: rows} =
          repo.query!(
            """
            DELETE FROM #{pending}
            WHERE folder_id IN (SELECT folder_id FROM #{pending} ORDER BY folder_id LIMIT $1)
            RETURNING folder_id, n, unseen
            """,
            [@chunk]
          )

        [ids, ns, unseen] = columns(rows, 3)

        # Lock in id order, like every writer of folders (Mailbox.Commands).
        repo.query!("SELECT id FROM folders WHERE id = ANY($1) ORDER BY id FOR UPDATE", [ids])

        %{rows: accounts} =
          repo.query!(
            """
            UPDATE folders f
            SET messages_count = GREATEST(f.messages_count - c.n, 0),
                unseen_count = GREATEST(f.unseen_count - c.unseen, 0),
                highest_modseq = f.highest_modseq + 1, updated_at = now()
            FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) AS c(id, n, unseen)
            WHERE f.id = c.id
            RETURNING f.account_id
            """,
            [ids, ns, unseen]
          )

        {length(rows), accounts |> List.flatten() |> Enum.uniq()}
      end)

    # After the commit: the actors read the new counters.
    if accounts != [], do: Mailstore.Mailbox.AccountServer.reload(accounts)

    if n < @chunk do
      repo.query!("DROP TABLE #{pending}")
      total + n
    else
      apply_pending(repo, pending, total + n)
    end
  end

  defp columns([], k), do: List.duplicate([], k)
  defp columns(rows, _k), do: rows |> Enum.zip() |> Enum.map(&Tuple.to_list/1)

  # Detach without blocking traffic; finish a detach a crashed run left pending.
  @doc false
  def detach(repo, parent, name) do
    %{rows: [[pending]]} =
      repo.query!(
        "SELECT inhdetachpending FROM pg_inherits WHERE inhrelid = to_regclass($1)",
        [name]
      )

    if pending,
      do: repo.query!("ALTER TABLE #{parent} DETACH PARTITION #{name} FINALIZE"),
      else: repo.query!("ALTER TABLE #{parent} DETACH PARTITION #{name} CONCURRENTLY")
  end

  @doc "Key of the shard maintenance advisory lock (see `with_maintenance_lock/2`)."
  def maintenance_lock, do: @maintenance_lock

  @doc "The day after the last day of a week partition (`messages_p0_2026w48` etc.)."
  def week_end(name) do
    [_, _table, _pid, year, week] = Regex.run(@week, name)
    week_start(String.to_integer(year), String.to_integer(week)) |> Date.add(7)
  end

  # Monday of ISO week `week` of ISO year `year`.
  defp week_start(year, week) do
    jan4 = Date.new!(year, 1, 4)
    jan4 |> Date.add(1 - Date.day_of_week(jan4)) |> Date.add((week - 1) * 7)
  end
end
