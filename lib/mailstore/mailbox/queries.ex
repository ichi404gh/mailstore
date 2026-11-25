defmodule Mailstore.Mailbox.Queries do
  @moduledoc """
  Composable read queries over mailbox schemas. They only build `Ecto.Query` values;
  `Mailstore.Mailbox` runs them against the tenant's shard. Writes are plain SQL in
  `Mailstore.Mailbox.Commands`.
  """
  import Ecto.Query
  alias Mailstore.Mailbox.Schemas.{Folder, Message, MessageBody, Vanished}

  def folders_of_account(account_id) do
    from f in Folder, where: f.account_id == ^account_id, order_by: f.id
  end

  def messages_in(folder_id), do: from(m in Message, where: m.folder_id == ^folder_id)

  def uids(folder_id) do
    from m in messages_in(folder_id), order_by: m.uid, select: m.uid
  end

  def unseen_uids(folder_id) do
    from m in uids(folder_id), where: fragment("(? & 1) = 0", m.flags)
  end

  def first_unseen_uid(folder_id), do: from(m in unseen_uids(folder_id), limit: 1)

  @doc """
  Runs of consecutive UIDs as `{first, last}` (gaps-and-islands): `uid - row_number()`
  is constant within a run. A session's UID map is built from this, so SELECT moves one
  row per run instead of one row per message.
  """
  def uid_ranges(folder_id), do: ranges_of(messages_in(folder_id))

  @doc "Runs of unseen UIDs, from the partial index `messages_folder_unseen`."
  def unseen_uid_ranges(folder_id) do
    ranges_of(from(m in messages_in(folder_id), where: fragment("(? & 1) = 0", m.flags)))
  end

  defp ranges_of(messages) do
    islands =
      from m in messages,
        select: %{uid: m.uid, run: fragment("? - row_number() OVER (ORDER BY ?)", m.uid, m.uid)}

    from i in subquery(islands),
      group_by: i.run,
      order_by: min(i.uid),
      select: {min(i.uid), max(i.uid)}
  end

  @doc "What flag/size/date searches need: no envelope, no body. `:all` or a UID list."
  def flag_rows(folder_id, uids) do
    query =
      from m in messages_in(folder_id),
        order_by: m.uid,
        select: %{
          uid: m.uid,
          flags: m.flags,
          keywords: m.keywords,
          size: m.size,
          internal_date: m.internal_date
        }

    if uids == :all, do: query, else: from(m in query, where: m.uid in ^uids)
  end

  @doc "FETCH metadata; the MIME tree only when asked, it is the widest column."
  def meta(folder_id, uids, structure? \\ false)

  def meta(folder_id, uids, true),
    do: from(m in meta(folder_id, uids, false), select_merge: %{bodystructure: m.bodystructure})

  def meta(folder_id, uids, false) do
    from m in messages_in(folder_id),
      where: m.uid in ^uids,
      order_by: m.uid,
      select: %{
        uid: m.uid,
        modseq: m.modseq,
        flags: m.flags,
        keywords: m.keywords,
        size: m.size,
        internal_date: m.internal_date,
        envelope: m.envelope
      }
  end

  def bodies(folder_id, uids) do
    from b in MessageBody,
      where: b.folder_id == ^folder_id and b.uid in ^uids,
      select: {b.uid, b.codec, b.data}
  end

  def body(folder_id, uid) do
    from b in MessageBody,
      where: b.folder_id == ^folder_id and b.uid == ^uid,
      select: {b.codec, b.data}
  end

  def changed_since(folder_id, modseq) do
    from m in messages_in(folder_id),
      where: m.modseq > ^modseq,
      order_by: m.uid,
      select: %{uid: m.uid, modseq: m.modseq, flags: m.flags, keywords: m.keywords}
  end

  def vanished_since(folder_id, modseq) do
    from v in Vanished,
      where: v.folder_id == ^folder_id and v.modseq > ^modseq,
      order_by: v.uid,
      select: v.uid
  end

  def by_message_id(tenant_id, message_id_h, message_id) do
    from m in Message,
      where:
        m.tenant_id == ^tenant_id and m.message_id_h == ^message_id_h and
          m.message_id == ^message_id,
      select: {m.folder_id, m.uid}
  end
end
