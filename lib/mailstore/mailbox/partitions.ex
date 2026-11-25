defmodule Mailstore.Mailbox.Partitions do
  @moduledoc """
  Weekly partitions of `messages` / `message_bodies`. Retention = drop whole weeks.

  Prototype: one retention class. Production: partition by retention class too
  (warmup 14 days, standard 365), so warmup weeks can be dropped early.
  """
  alias Mailstore.Repo
  alias Mailstore.Mailbox.AccountServer

  @doc "Make sure partitions exist from last week up to `weeks` ahead."
  def ensure(repo \\ Repo, from \\ Date.add(Date.utc_today(), -7), weeks \\ 12) do
    repo.query!("SELECT mailstore_ensure_partitions($1, $2)", [from, weeks])
    :ok
  end

  @doc "messages partitions whose upper bound is <= cutoff."
  def list_before(repo \\ Repo, %DateTime{} = cutoff) do
    %{rows: rows} =
      repo.query!(
        """
        SELECT c.relname
        FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'messages'::regclass
          AND (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \\(''([^'']+)''\\)'))[1]::timestamptz <= $1
        ORDER BY c.relname
        """,
        [cutoff]
      )

    List.flatten(rows)
  end

  @doc """
  Drop every week that ends before `cutoff`, then fix counters of the affected
  folders: `retained_from_uid` moves up (clients learn it as VANISHED 1:n) and
  HIGHESTMODSEQ is bumped so CONDSTORE clients notice.

  Production would DETACH PARTITION CONCURRENTLY first to avoid an exclusive lock on
  the parent table.
  """
  def drop_before(repo \\ Repo, %DateTime{} = cutoff) do
    names = list_before(repo, cutoff)

    {:ok, affected} =
      repo.transaction(fn ->
        affected =
          Enum.flat_map(names, fn name ->
            true = Regex.match?(~r/^messages_\d{4}w\d{2}$/, name)
            suffix = String.replace_prefix(name, "messages_", "")
            %{rows: rows} = repo.query!("SELECT DISTINCT folder_id FROM #{name}")
            repo.query!("DROP TABLE #{name}")
            repo.query!("DROP TABLE IF EXISTS message_bodies_#{suffix}")
            List.flatten(rows)
          end)
          |> Enum.uniq()

        if affected != [] do
          repo.query!(
            """
            UPDATE folders f
            SET messages_count = s.cnt, unseen_count = s.unseen,
                retained_from_uid = COALESCE(s.min_uid, f.uidnext),
                highest_modseq = f.highest_modseq + 1, updated_at = now()
            FROM (
              SELECT fo.id, count(m.uid) AS cnt,
                     count(m.uid) FILTER (WHERE (m.flags & 1) = 0) AS unseen,
                     min(m.uid) AS min_uid
              FROM folders fo LEFT JOIN messages m ON m.folder_id = fo.id
              WHERE fo.id = ANY($1)
              GROUP BY fo.id
            ) s
            WHERE f.id = s.id
            """,
            [affected]
          )
        end

        affected
      end)

    AccountServer.reload_all()
    {:ok, %{dropped: names, affected_folders: affected}}
  end
end
