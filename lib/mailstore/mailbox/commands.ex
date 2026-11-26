defmodule Mailstore.Mailbox.Commands do
  @moduledoc """
  Write side of a shard: plain SQL, one function per IMAP write command. Every
  function must run inside `repo.transaction/1`. Only `Mailstore.Mailbox.AccountServer`
  and `Mailstore.Mailbox.deliver/1` call these; everything else goes through
  `Mailstore.Mailbox`.

  Correctness does not depend on the account actor: UIDs and MODSEQs come from an
  atomic `UPDATE folders ... RETURNING` (or a `FOR UPDATE` lock on the folder row), so
  two writers on the same folder (for example two actors during a netsplit) still get
  disjoint UID ranges. The actor only batches writes and fans out notifications.

  Each function returns the folder counters after the change (or `nil` if nothing
  changed) so the actor can refresh its in-memory view without another query.
  """
  import Bitwise
  alias Mailstore.Core.{BodyCodec, Flags}
  alias Mailstore.Mailbox.Schemas.{Message, MessageBody}

  @counters "uidnext, highest_modseq, messages_count, unseen_count"

  @doc "Append messages to a folder. Returns {uids, counters}."
  def append(repo, folder_id, tenant_id, msgs, opts \\ []) when msgs != [] do
    stored_at = Keyword.get(opts, :stored_at, DateTime.utc_now())
    n = length(msgs)
    unseen = Enum.count(msgs, &(not Flags.seen?(&1.flags_mask)))

    %{rows: [row]} =
      repo.query!(
        """
        UPDATE folders
        SET uidnext = uidnext + $2, highest_modseq = highest_modseq + $2,
            messages_count = messages_count + $2, unseen_count = unseen_count + $3,
            updated_at = now()
        WHERE id = $1
        RETURNING #{@counters}
        """,
        [folder_id, n, unseen]
      )

    counters = counters(row)
    first_uid = counters.uidnext - n
    first_modseq = counters.highest_modseq - n + 1

    {meta, bodies} =
      msgs
      |> Enum.with_index()
      |> Enum.map(fn {m, i} ->
        rows(m, tenant_id, folder_id, first_uid + i, first_modseq + i, stored_at)
      end)
      |> Enum.unzip()

    insert_rows(repo, meta, bodies)
    {Enum.map(meta, & &1.uid), counters}
  end

  @doc """
  Group commit for delivery: many messages to many folders of many accounts in one
  transaction, a fixed number of statements whatever the batch size.

  `items`: `%{account_id, tenant_id, folder, msg, key}`, where `folder` is a folder name
  as stored and `key` a binary that identifies the delivery (or nil). A key already in
  `deliveries` for that account (also earlier in the same batch) makes the item a
  duplicate: nothing is stored and no UID is spent, so replaying a batch is harmless.

  Folder rows are locked in id order before UIDs are allocated (concurrent batches
  touching the same folders cannot deadlock each other), and stay locked until commit:
  UIDs of a folder are therefore committed in ascending order, which the account actor
  relies on (`AccountServer.delivered/2`).

  Returns `{results, updates}`: one result per item, in order (`{:stored, folder_id,
  uid}`, `:duplicate` or `{:failed, :no_such_folder}`), and one update per folder that
  got messages: `%{account_id, folder_id, uids, unseen, keywords, counters}`.
  """
  def deliver(repo, items, opts \\ []) do
    stored_at = Keyword.get(opts, :stored_at, DateTime.utc_now())
    items = Enum.with_index(items)
    {keep, duplicates} = dedup(repo, items)
    folders = lock_folders(repo, keep)

    {placed, missing} =
      Enum.split_with(keep, fn {item, _i} ->
        Map.has_key?(folders, {item.account_id, item.folder})
      end)

    by_folder =
      Enum.group_by(placed, fn {item, _i} -> folders[{item.account_id, item.folder}] end)

    counters = bump_folders(repo, by_folder)

    # Each folder's new UIDs and MODSEQs end at its counters after the bump.
    placed =
      Enum.flat_map(by_folder, fn {folder_id, group} ->
        c = counters[folder_id]
        first = length(group) - 1

        group
        |> Enum.with_index()
        |> Enum.map(fn {{item, i}, k} ->
          %{
            index: i,
            item: item,
            folder_id: folder_id,
            uid: c.uidnext - first - 1 + k,
            modseq: c.highest_modseq - first + k
          }
        end)
      end)

    {meta, bodies} =
      placed
      |> Enum.map(fn p ->
        rows(p.item.msg, p.item.tenant_id, p.folder_id, p.uid, p.modseq, stored_at)
      end)
      |> Enum.unzip()

    insert_rows(repo, meta, bodies)

    updates =
      placed
      |> Enum.group_by(& &1.folder_id)
      |> Enum.map(fn {folder_id, ps} ->
        %{
          account_id: hd(ps).item.account_id,
          folder_id: folder_id,
          uids: Enum.map(ps, & &1.uid),
          unseen: for(p <- ps, not Flags.seen?(p.item.msg.flags_mask), do: p.uid),
          keywords: ps |> Enum.flat_map(& &1.item.msg.keywords) |> Enum.uniq(),
          counters: counters[folder_id]
        }
      end)

    results =
      (Enum.map(placed, &{&1.index, {:stored, &1.folder_id, &1.uid}}) ++
         Enum.map(duplicates, fn {_item, i} -> {i, :duplicate} end) ++
         Enum.map(missing, fn {_item, i} -> {i, {:failed, :no_such_folder}} end))
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(&elem(&1, 1))

    {results, updates}
  end

  # Record the keys; an item is kept if it has no key or its key was recorded by this
  # statement (once: the first occurrence in the batch). NOT EXISTS sees every day's
  # partition; the primary key catches a concurrent transaction recording the same key.
  defp dedup(repo, items) do
    keyed =
      for {%{key: key} = item, _i} <- items,
          is_binary(key),
          uniq: true,
          do: {item.account_id, key}

    recorded =
      if keyed == [] do
        MapSet.new()
      else
        {accounts, keys} = keyed |> Enum.sort() |> Enum.unzip()

        %{rows: rows} =
          repo.query!(
            """
            INSERT INTO deliveries (account_id, key)
            SELECT a, k FROM unnest($1::bigint[], $2::bytea[]) AS v(a, k)
            WHERE NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.account_id = v.a AND d.key = v.k)
            ON CONFLICT DO NOTHING
            RETURNING account_id, key
            """,
            [accounts, keys]
          )

        MapSet.new(rows, fn [a, k] -> {a, k} end)
      end

    {keep, dups, _taken} =
      Enum.reduce(items, {[], [], MapSet.new()}, fn {item, _i} = entry, {keep, dups, taken} ->
        case item[:key] do
          nil ->
            {[entry | keep], dups, taken}

          key ->
            id = {item.account_id, key}

            if MapSet.member?(recorded, id) and not MapSet.member?(taken, id),
              do: {[entry | keep], dups, MapSet.put(taken, id)},
              else: {keep, [entry | dups], taken}
        end
      end)

    {Enum.reverse(keep), Enum.reverse(dups)}
  end

  # %{{account_id, folder_name} => folder_id}, rows locked in id order.
  defp lock_folders(_repo, []), do: %{}

  defp lock_folders(repo, keep) do
    {accounts, names} =
      keep
      |> Enum.map(fn {item, _i} -> {item.account_id, item.folder} end)
      |> Enum.uniq()
      |> Enum.unzip()

    %{rows: rows} =
      repo.query!(
        """
        SELECT f.id, f.account_id, f.name
        FROM folders f JOIN unnest($1::bigint[], $2::text[]) AS v(a, n)
          ON f.account_id = v.a AND f.name = v.n
        ORDER BY f.id
        FOR UPDATE OF f
        """,
        [accounts, names]
      )

    Map.new(rows, fn [id, account_id, name] -> {{account_id, name}, id} end)
  end

  # One UPDATE for all folders: %{folder_id => counters after the batch}.
  defp bump_folders(_repo, by_folder) when map_size(by_folder) == 0, do: %{}

  defp bump_folders(repo, by_folder) do
    {ids, ns, unseen} =
      by_folder
      |> Enum.map(fn {id, group} ->
        {id, length(group),
         Enum.count(group, fn {item, _i} -> not Flags.seen?(item.msg.flags_mask) end)}
      end)
      |> unzip3()

    %{rows: rows} =
      repo.query!(
        """
        UPDATE folders f
        SET uidnext = f.uidnext + v.n, highest_modseq = f.highest_modseq + v.n,
            messages_count = f.messages_count + v.n, unseen_count = f.unseen_count + v.unseen,
            updated_at = now()
        FROM unnest($1::bigint[], $2::int[], $3::int[]) AS v(id, n, unseen)
        WHERE f.id = v.id
        RETURNING f.id, #{@counters}
        """,
        [ids, ns, unseen]
      )

    Map.new(rows, fn [id | counters] -> {id, counters(counters)} end)
  end

  defp unzip3(triples) do
    {a, b, c} =
      Enum.reduce(triples, {[], [], []}, fn {x, y, z}, {a, b, c} ->
        {[x | a], [y | b], [z | c]}
      end)

    {Enum.reverse(a), Enum.reverse(b), Enum.reverse(c)}
  end

  defp rows(m, tenant_id, folder_id, uid, modseq, stored_at) do
    {codec, data} = m.body || BodyCodec.encode(m.raw)

    {%{
       tenant_id: tenant_id,
       folder_id: folder_id,
       uid: uid,
       modseq: modseq,
       stored_at: stored_at,
       internal_date: m.internal_date,
       size: m.size,
       flags: m.flags_mask,
       keywords: m.keywords,
       class: m.class,
       message_id: m.message_id,
       message_id_h: m.message_id_h,
       in_reply_to_h: m.in_reply_to_h,
       thread_id: m.thread_id,
       from_addr: m.from_addr,
       subject: m.subject,
       envelope: m.envelope,
       bodystructure: m.bodystructure
     }, %{folder_id: folder_id, uid: uid, stored_at: stored_at, codec: codec, data: data}}
  end

  # Postgres takes at most 65 535 parameters per statement: 18 columns x 1 000 rows fits.
  defp insert_rows(repo, meta, bodies) do
    for chunk <- Enum.chunk_every(meta, 1_000), do: repo.insert_all(Message, chunk)
    for chunk <- Enum.chunk_every(bodies, 1_000), do: repo.insert_all(MessageBody, chunk)
    :ok
  end

  @doc """
  STORE: change flags on `uids`. One new MODSEQ per command, assigned only to
  messages whose flags actually change. Returns {[%{uid, mask, keywords}], modseq, counters}.
  """
  def store_flags(repo, folder_id, uids, op, mask, keywords)
      when op in [:add, :remove, :replace] do
    modseq = lock_next_modseq(repo, folder_id)

    {flags_expr, kw_expr} =
      case op do
        :add ->
          {"t.old_flags | $3::smallint",
           "ARRAY(SELECT DISTINCT x FROM unnest(t.old_kw || $4::text[]) AS x ORDER BY x)"}

        :remove ->
          {"t.old_flags & ~$3::smallint",
           "ARRAY(SELECT x FROM unnest(t.old_kw) AS x WHERE x <> ALL($4::text[]) ORDER BY x)"}

        :replace ->
          {"$3::smallint", "ARRAY(SELECT DISTINCT x FROM unnest($4::text[]) AS x ORDER BY x)"}
      end

    %{rows: rows} =
      repo.query!(
        """
        WITH t AS (
          SELECT uid, stored_at, flags AS old_flags, keywords AS old_kw
          FROM messages WHERE folder_id = $1 AND uid = ANY($2)
          FOR UPDATE
        ), c AS (
          SELECT uid, stored_at, old_flags, old_kw,
                 (#{flags_expr}) AS new_flags, (#{kw_expr}) AS new_kw
          FROM t
        )
        UPDATE messages m
        SET flags = c.new_flags, keywords = c.new_kw, modseq = $5
        FROM c
        WHERE m.folder_id = $1 AND m.uid = c.uid AND m.stored_at = c.stored_at
          AND (c.new_flags <> c.old_flags OR c.new_kw <> c.old_kw)
        RETURNING m.uid, m.flags, m.keywords, c.old_flags
        """,
        [folder_id, uids, mask, keywords, modseq]
      )

    if rows == [] do
      {[], nil, nil}
    else
      unseen_delta =
        Enum.reduce(rows, 0, fn [_uid, new, _kw, old], acc ->
          acc + unseen_bit(new) - unseen_bit(old)
        end)

      counters =
        update_counters(
          repo,
          folder_id,
          "highest_modseq = $2, unseen_count = unseen_count + $3",
          [
            modseq,
            unseen_delta
          ]
        )

      changes =
        rows
        |> Enum.map(fn [uid, flags, kw, _old] -> %{uid: uid, mask: flags, keywords: kw} end)
        |> Enum.sort_by(& &1.uid)

      {changes, modseq, counters}
    end
  end

  @doc """
  EXPUNGE / UID EXPUNGE: remove messages flagged \\Deleted (optionally only within
  `uids`), log them in `vanished`. Returns {expunged_uids, modseq, counters}.
  """
  def expunge(repo, folder_id, uids \\ :all) do
    modseq = lock_next_modseq(repo, folder_id)
    {uid_filter, params} = if uids == :all, do: {"", []}, else: {"AND uid = ANY($3)", [uids]}

    %{rows: rows} =
      repo.query!(
        """
        WITH del AS (
          DELETE FROM messages
          WHERE folder_id = $1 AND (flags & #{Flags.deleted()}) <> 0 #{uid_filter}
          RETURNING uid, stored_at, flags
        ), delb AS (
          DELETE FROM message_bodies b USING del
          WHERE b.folder_id = $1 AND b.uid = del.uid AND b.stored_at = del.stored_at
          RETURNING 1
        ), van AS (
          INSERT INTO vanished (folder_id, uid, modseq)
          SELECT $1, uid, $2 FROM del
          ON CONFLICT (folder_id, uid) DO UPDATE SET modseq = EXCLUDED.modseq
          RETURNING 1
        )
        SELECT uid, flags FROM del ORDER BY uid
        """,
        [folder_id, modseq | params]
      )

    if rows == [] do
      {[], nil, nil}
    else
      n = length(rows)
      unseen = Enum.count(rows, fn [_uid, flags] -> unseen_bit(flags) == 1 end)

      counters =
        update_counters(
          repo,
          folder_id,
          "highest_modseq = $2, messages_count = messages_count - $3, unseen_count = unseen_count - $4",
          [modseq, n, unseen]
        )

      {Enum.map(rows, &hd/1), modseq, counters}
    end
  end

  @doc """
  COPY within one tenant, server-side (bodies never leave the database).
  Returns {[{src_uid, dst_uid}], dst_counters}.
  """
  def copy(repo, src_folder_id, uids, dst_folder_id, opts \\ []) do
    stored_at = Keyword.get(opts, :stored_at, DateTime.utc_now())

    %{rows: src} =
      repo.query!(
        "SELECT uid, flags FROM messages WHERE folder_id = $1 AND uid = ANY($2) ORDER BY uid",
        [src_folder_id, uids]
      )

    if src == [] do
      {[], nil}
    else
      src_uids = Enum.map(src, &hd/1)
      k = length(src_uids)
      unseen = Enum.count(src, fn [_uid, flags] -> unseen_bit(flags) == 1 end)

      %{rows: [row]} =
        repo.query!(
          """
          UPDATE folders
          SET uidnext = uidnext + $2, highest_modseq = highest_modseq + $2,
              messages_count = messages_count + $2, unseen_count = unseen_count + $3,
              updated_at = now()
          WHERE id = $1
          RETURNING #{@counters}
          """,
          [dst_folder_id, k, unseen]
        )

      counters = counters(row)
      first_uid = counters.uidnext - k
      first_modseq = counters.highest_modseq - k + 1

      repo.query!(
        """
        INSERT INTO messages (tenant_id, folder_id, uid, modseq, stored_at, internal_date, size,
          flags, keywords, class, message_id, message_id_h, in_reply_to_h, thread_id,
          from_addr, subject, envelope, bodystructure)
        SELECT tenant_id, $2, $3 + row_number() OVER (ORDER BY uid) - 1,
          $4 + row_number() OVER (ORDER BY uid) - 1, $5, internal_date, size,
          flags, keywords, class, message_id, message_id_h, in_reply_to_h, thread_id,
          from_addr, subject, envelope, bodystructure
        FROM messages WHERE folder_id = $1 AND uid = ANY($6)
        """,
        [src_folder_id, dst_folder_id, first_uid, first_modseq, stored_at, src_uids]
      )

      repo.query!(
        """
        INSERT INTO message_bodies (folder_id, uid, stored_at, codec, data)
        SELECT $2, $3 + row_number() OVER (ORDER BY uid) - 1, $4, codec, data
        FROM message_bodies WHERE folder_id = $1 AND uid = ANY($5)
        """,
        [src_folder_id, dst_folder_id, first_uid, stored_at, src_uids]
      )

      {Enum.zip(src_uids, first_uid..(first_uid + k - 1)//1), counters}
    end
  end

  ## Folders

  @doc """
  CREATE: insert folders (parents first). UIDVALIDITY comes from the
  `folder_uidvalidity` sequence, so a name created again never reuses an old value.
  New folders are not subscribed: clients subscribe themselves (as with Dovecot).
  """
  def create_folders(repo, tenant_id, account_id, names) when names != [] do
    repo.query!(
      """
      INSERT INTO folders (tenant_id, account_id, name, uidvalidity, subscribed, inserted_at, updated_at)
      SELECT $1, $2, n, nextval('folder_uidvalidity'), false, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'
      FROM unnest($3::text[]) WITH ORDINALITY AS t(n, i)
      ORDER BY i
      """,
      [tenant_id, account_id, names]
    )

    :ok
  end

  @doc "SUBSCRIBE / UNSUBSCRIBE. Subscriptions belong to existing folders only."
  def set_subscribed(repo, folder_id, on?) do
    repo.query!("UPDATE folders SET subscribed = $2 WHERE id = $1", [folder_id, on?])
    :ok
  end

  @doc "DELETE: the folder with all its messages, bodies and expunge log."
  def delete_folder(repo, folder_id) do
    for table <- ["message_bodies", "messages", "vanished"],
        do: repo.query!("DELETE FROM #{table} WHERE folder_id = $1", [folder_id])

    repo.query!("DELETE FROM folders WHERE id = $1", [folder_id])
    :ok
  end

  @doc """
  RENAME: the folder and everything inside it move under the new name; ids, UIDs and
  UIDVALIDITY stay. Returns [{folder_id, new_name}].
  """
  def rename_folder(repo, account_id, old, new) do
    %{rows: rows} =
      repo.query!(
        """
        UPDATE folders
        SET name = $3 || substr(name, length($2) + 1), updated_at = now() AT TIME ZONE 'UTC'
        WHERE account_id = $1 AND (name = $2 OR starts_with(name, $2 || '/'))
        RETURNING id, name
        """,
        [account_id, old, new]
      )

    Enum.map(rows, fn [id, name] -> {id, name} end)
  end

  defp lock_next_modseq(repo, folder_id) do
    %{rows: [[next]]} =
      repo.query!("SELECT highest_modseq + 1 FROM folders WHERE id = $1 FOR UPDATE", [folder_id])

    next
  end

  defp update_counters(repo, folder_id, set_sql, params) do
    %{rows: [row]} =
      repo.query!(
        "UPDATE folders SET #{set_sql}, updated_at = now() WHERE id = $1 RETURNING #{@counters}",
        [folder_id | params]
      )

    counters(row)
  end

  defp counters([uidnext, highest_modseq, messages_count, unseen_count]) do
    %{
      uidnext: uidnext,
      highest_modseq: highest_modseq,
      messages_count: messages_count,
      unseen_count: unseen_count
    }
  end

  defp unseen_bit(flags), do: if((flags &&& 1) == 0, do: 1, else: 0)
end
