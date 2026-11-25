defmodule Mailstore.Mailbox.Commands do
  @moduledoc """
  Write side of a shard: plain SQL, one function per IMAP write command. Every
  function must run inside `repo.transaction/1`. Only `Mailstore.Mailbox.AccountServer`
  calls these; everything else goes through `Mailstore.Mailbox`.

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
        uid = first_uid + i
        {codec, data} = BodyCodec.encode(m.raw)

        {%{
           tenant_id: tenant_id,
           folder_id: folder_id,
           uid: uid,
           modseq: first_modseq + i,
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
      end)
      |> Enum.unzip()

    repo.insert_all(Message, meta)
    repo.insert_all(MessageBody, bodies)

    {Enum.map(meta, & &1.uid), counters}
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
