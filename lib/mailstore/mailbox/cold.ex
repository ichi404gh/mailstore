defmodule Mailstore.Mailbox.Cold do
  @moduledoc """
  The cold tier: bodies of old weeks move from Postgres to the object store
  (`Mailstore.ColdStore`); metadata and flags stay in Postgres.

  **What goes.** Weeks of policies with `cold_after_days` (`Mailbox.Retention`), once
  the week ended more than `cold_after_days` ago. Old mail is assumed to be read
  rarely (about one message in a million, an assumption, not a measurement), so a
  cold read may take a round trip to the store.

  **Packs.** One object per tenant, policy and week, split at `pack_bytes`
  (64 MB by default): the bodies as stored (each compressed on its own), then an index
  `{folder_id, uid, offset, length, codec}` and a trailer, so a pack can be read back
  without the database. Key: `t<tenant>/p<policy>/<IYYY>w<IW>/<random>.pack`. A tenant's
  packs hold only its mail, so they move with the tenant to another shard.

  **Moving a week** (`run/1`, every 12 hours from `Mailbox.PartitionMaintainer`; one
  run per shard and policy at a time, not during a tenant move):

  1. Packs left `uploading` by a crashed run are deleted from the store and the table.
  2. Per tenant, bodies without a location yet are read in `(folder_id, uid)` order and
     cut into packs. For each pack: a `cold_packs` row (`uploading`), the upload (the
     store checks it with Content-MD5, then its size is checked), then one
     transaction writes the locations (`cold_bodies`) and marks the pack `stored`.
  3. When every body of the week has a location, the week's `message_bodies`
     partition is detached (`CONCURRENTLY`) and dropped.

  Until step 3 a body is in both places; readers take Postgres first. Expiry drops the
  week's `cold_bodies` partition and deletes its packs (`Mailbox.Partitions`).
  """
  require Logger
  alias Mailstore.ColdStore
  alias Mailstore.Mailbox.{Partitions, Retention}

  @page 2_000
  @cold_lock 7004
  @magic "MSTRPAK1"
  # Ranges of one pack closer than this are fetched in one request.
  @coalesce_gap 256 * 1024

  @doc """
  Move every due week of every cold policy on every shard. Returns
  `{:ok, %{weeks: [table], packs: n, bodies: n}}`.
  """
  def run(today \\ Date.utc_today()) do
    if ColdStore.enabled?() do
      results =
        for repo <- Mailstore.Shards.all(), policy <- Retention.cold_policies() do
          run_policy(repo, policy, today)
        end

      {:ok,
       %{
         weeks: Enum.flat_map(results, & &1.weeks),
         packs: results |> Enum.map(& &1.packs) |> Enum.sum(),
         bodies: results |> Enum.map(& &1.bodies) |> Enum.sum()
       }}
    else
      {:ok, %{weeks: [], packs: 0, bodies: 0}}
    end
  end

  defp run_policy(repo, policy, today) do
    cold_from = Date.add(today, -policy.cold_after_days)
    expires = Date.add(today, -policy.keep_days)

    repo.checkout(fn ->
      %{rows: [[locked]]} =
        repo.query!(
          """
          SELECT pg_try_advisory_lock(#{@cold_lock}, $1)
             AND pg_try_advisory_lock_shared(#{Partitions.maintenance_lock()}, 0)
          """,
          [policy.id]
        )

      try do
        if locked do
          clean_uploading(repo, policy.id)

          weeks =
            for {name, true} <- Partitions.expired(repo, policy.id, cold_from, "message_bodies"),
                # Expiry takes these anyway.
                Date.compare(Partitions.week_end(name), expires) == :gt,
                do: move_week(repo, policy.id, name)

          %{
            weeks: for(%{dropped: name} <- weeks, do: name),
            packs: weeks |> Enum.map(& &1.packs) |> Enum.sum(),
            bodies: weeks |> Enum.map(& &1.bodies) |> Enum.sum()
          }
        else
          %{weeks: [], packs: 0, bodies: 0}
        end
      after
        repo.query!("SELECT pg_advisory_unlock_all()")
      end
    end)
  end

  defp clean_uploading(repo, policy_id) do
    %{rows: rows} =
      repo.query!("SELECT id, key FROM cold_packs WHERE retention = $1 AND state = 'uploading'", [
        policy_id
      ])

    for [id, key] <- rows do
      :ok = ColdStore.delete(key)
      repo.query!("DELETE FROM cold_bodies WHERE pack_id = $1", [id])
      repo.query!("DELETE FROM cold_packs WHERE id = $1", [id])
    end
  end

  # Pack the week's bodies tenant by tenant, then drop the hot partition.
  defp move_week(repo, policy_id, hot) do
    suffix = String.replace_prefix(hot, "message_bodies_p#{policy_id}_", "")
    cold = "cold_bodies_p#{policy_id}_#{suffix}"
    week = Partitions.week_end(hot) |> Date.add(-7)
    Partitions.ensure(repo, week, 1)

    %{rows: tenants} =
      repo.query!(
        "SELECT DISTINCT f.tenant_id FROM #{hot} b JOIN folders f ON f.id = b.folder_id ORDER BY 1"
      )

    {packs, bodies} =
      Enum.reduce(tenants, {0, 0}, fn [tenant_id], {p, b} ->
        {p2, b2} = pack_tenant(repo, hot, cold, tenant_id, policy_id, week, suffix)
        {p + p2, b + b2}
      end)

    %{rows: [[left]]} =
      repo.query!("""
      SELECT count(*) FROM #{hot} b
      WHERE NOT EXISTS (SELECT 1 FROM #{cold} c WHERE c.folder_id = b.folder_id AND c.uid = b.uid)
      """)

    if left == 0 do
      repo.query!("SET lock_timeout = '10s'")

      try do
        Partitions.detach(repo, "message_bodies_p#{policy_id}", hot)
        repo.query!("DROP TABLE #{hot}")
      after
        repo.query!("RESET lock_timeout")
      end

      %{dropped: hot, packs: packs, bodies: bodies}
    else
      Logger.warning("cold: #{hot} still has #{left} bodies without a location, kept")
      %{packs: packs, bodies: bodies}
    end
  end

  defp pack_tenant(repo, hot, cold, tenant_id, policy_id, week, suffix) do
    limit = Map.get(cold_config(), :pack_bytes, 64 * 1024 * 1024)

    Stream.unfold({0, 0}, fn
      :done ->
        nil

      {f, u} ->
        %{rows: rows} =
          repo.query!(
            """
            SELECT b.folder_id, b.uid, b.stored_at, b.codec, b.data FROM #{hot} b
            JOIN folders f ON f.id = b.folder_id
            WHERE f.tenant_id = $1 AND (b.folder_id, b.uid) > ($2, $3)
              AND NOT EXISTS (SELECT 1 FROM #{cold} c WHERE c.folder_id = b.folder_id AND c.uid = b.uid)
            ORDER BY b.folder_id, b.uid
            LIMIT #{@page}
            """,
            [tenant_id, f, u]
          )

        case rows do
          [] -> nil
          rows when length(rows) < @page -> {rows, :done}
          rows -> {rows, rows |> List.last() |> then(fn [f, u | _] -> {f, u} end)}
        end
    end)
    |> Stream.flat_map(& &1)
    |> Stream.chunk_while(
      {[], 0},
      fn [_, _, _, _, data] = row, {acc, size} ->
        if size > 0 and size + byte_size(data) > limit,
          do: {:cont, Enum.reverse(acc), {[row], byte_size(data)}},
          else: {:cont, {[row | acc], size + byte_size(data)}}
      end,
      fn
        {[], _} -> {:cont, {[], 0}}
        {acc, _} -> {:cont, Enum.reverse(acc), {[], 0}}
      end
    )
    |> Enum.reduce({0, 0}, fn rows, {p, b} ->
      store_pack(repo, rows, tenant_id, policy_id, week, suffix)
      {p + 1, b + length(rows)}
    end)
  end

  defp store_pack(repo, rows, tenant_id, policy_id, week, suffix) do
    {entries, _} =
      Enum.map_reduce(rows, 0, fn [f, u, at, codec, data], offset ->
        {{f, u, at, codec, offset, byte_size(data), data}, offset + byte_size(data)}
      end)

    pack = encode(entries)

    key =
      "t#{tenant_id}/p#{policy_id}/#{suffix}/#{Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)}.pack"

    %{rows: [[pack_id]]} =
      repo.query!(
        """
        INSERT INTO cold_packs (tenant_id, retention, week, key, bytes) VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        [tenant_id, policy_id, week, key, byte_size(pack)]
      )

    :ok = ColdStore.put(key, pack)
    size = byte_size(pack)
    {:ok, ^size} = ColdStore.size(key)

    {:ok, _} =
      repo.transaction(fn ->
        [fs, us, ats, codecs, offsets, lengths] =
          entries
          |> Enum.map(fn {f, u, at, codec, offset, length, _} ->
            [f, u, at, codec, offset, length]
          end)
          |> Enum.zip()
          |> Enum.map(&Tuple.to_list/1)

        repo.query!(
          """
          INSERT INTO cold_bodies (folder_id, uid, stored_at, retention, pack_id, "offset", length, codec)
          SELECT f, u, at, $7, $8, o, l, c
          FROM unnest($1::bigint[], $2::bigint[], $3::timestamptz[], $4::smallint[], $5::bigint[], $6::int[])
            AS x(f, u, at, c, o, l)
          """,
          [fs, us, ats, codecs, offsets, lengths, policy_id, pack_id]
        )

        repo.query!("UPDATE cold_packs SET state = 'stored' WHERE id = $1", [pack_id])
      end)

    :ok
  end

  @doc """
  Pack layout: the bodies back to back, then the index (30 bytes per body:
  folder_id, uid, offset as 64-bit, length as 32-bit, codec as 16-bit, big-endian),
  then the index size (64-bit) and `#{@magic}`.
  """
  def encode(entries) do
    index =
      for {f, u, _at, codec, offset, length, _data} <- entries,
          into: <<>>,
          do: <<f::64, u::64, offset::64, length::32, codec::16>>

    IO.iodata_to_binary([
      Enum.map(entries, &elem(&1, 6)),
      index,
      <<byte_size(index)::64>>,
      @magic
    ])
  end

  @doc "The index of a pack: `[{folder_id, uid, offset, length, codec}]`."
  def decode_index(pack) do
    size = byte_size(pack)
    <<index_size::64, @magic>> = binary_part(pack, size - 16, 16)
    index = binary_part(pack, size - 16 - index_size, index_size)

    for <<f::64, u::64, offset::64, length::32, codec::16 <- index>>,
      do: {f, u, offset, length, codec}
  end

  @doc """
  Bodies of `uids` that are in the cold tier: `%{uid => {codec, data}}`. Ranges of one
  pack that lie close together are fetched in one request; packs in parallel.
  """
  def read(_repo, _folder_id, []), do: %{}

  def read(repo, folder_id, uids) do
    %{rows: rows} =
      repo.query!(
        """
        SELECT c.uid, c.codec, c."offset", c.length, p.key
        FROM cold_bodies c JOIN cold_packs p ON p.id = c.pack_id
        WHERE c.folder_id = $1 AND c.uid = ANY($2) AND p.state = 'stored'
        """,
        [folder_id, uids]
      )

    rows
    |> Enum.group_by(&List.last/1)
    |> Enum.flat_map(fn {key, entries} ->
      entries |> Enum.sort_by(&Enum.at(&1, 2)) |> coalesce() |> Enum.map(&{key, &1})
    end)
    |> Task.async_stream(
      fn {key, {from, to, entries}} ->
        {:ok, data} = ColdStore.get_range(key, from, to - from)

        for [uid, codec, offset, length, _key] <- entries,
            do: {uid, {codec, binary_part(data, offset - from, length)}}
      end,
      max_concurrency: 8,
      timeout: 60_000
    )
    |> Enum.flat_map(fn {:ok, bodies} -> bodies end)
    |> Map.new()
  end

  # [{from, to, entries}]: runs of entries whose gaps are below @coalesce_gap.
  defp coalesce(entries) do
    entries
    |> Enum.chunk_while(
      nil,
      fn [_, _, offset, length, _] = e, acc ->
        case acc do
          nil ->
            {:cont, {offset, offset + length, [e]}}

          {from, to, es} when offset - to <= @coalesce_gap ->
            {:cont, {from, max(to, offset + length), [e | es]}}

          {from, to, es} ->
            {:cont, {from, to, Enum.reverse(es)}, {offset, offset + length, [e]}}
        end
      end,
      fn
        nil -> {:cont, nil}
        {from, to, es} -> {:cont, {from, to, Enum.reverse(es)}, nil}
      end
    )
  end

  @doc "Delete the packs of weeks of policy `id` that ended on or before `cutoff`."
  def delete_expired_packs(repo, policy_id, %Date{} = cutoff) do
    %{rows: rows} =
      repo.query!(
        "SELECT id, key FROM cold_packs WHERE retention = $1 AND week + 7 <= $2 ORDER BY id",
        [policy_id, cutoff]
      )

    cond do
      rows == [] ->
        0

      not ColdStore.enabled?() ->
        Logger.warning("cold: #{length(rows)} expired packs, but no cold store is configured")
        0

      true ->
        for [id, key] <- rows do
          :ok = ColdStore.delete(key)
          repo.query!("DELETE FROM cold_packs WHERE id = $1", [id])
        end

        length(rows)
    end
  end

  defp cold_config, do: Map.new(Application.get_env(:mailstore, :cold_store, []))
end
