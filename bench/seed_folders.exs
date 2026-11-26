# Big INBOXes for memory tests, inserted in bulk with SQL (delivery would take ~10 min
# for 2M messages). Runs inside the mailstore app:
#
#   mix run bench/seed_folders.exs -- --users 1000 --domain big.test --fill 2000
#   mix run bench/seed_folders.exs -- --users 1000 --domain big.test --delete-fraction 0.01
#   mix run bench/seed_folders.exs -- --users 1000 --domain big.test --delete-every-other
#
# Accounts are bigN@DOMAIN with password "pw". --fill inserts N messages into each empty
# INBOX (all \Seen except the newest 10). --delete-fraction removes that share of the
# older messages at random (holes in the UID sequence), --delete-every-other removes
# every even UID (the worst case for range storage). Counters in `folders` are fixed up.
# Only metadata rows are inserted (no bodies): enough for SELECT/IDLE memory tests.
alias Mailstore.{Directory, Repo, Shards}

{opts, _, _} =
  OptionParser.parse(Enum.reject(System.argv(), &(&1 == "--")),
    strict: [
      users: :integer,
      domain: :string,
      fill: :integer,
      delete_fraction: :float,
      delete_every_other: :boolean
    ]
  )

users = opts[:users] || 1_000
domain_name = opts[:domain] || "big.test"
hash = Directory.hash_password("pw", 1_000)

domain =
  Directory.get_domain_by_name(domain_name) ||
    elem(Directory.create_domain(elem(Directory.create_tenant(%{name: "bigfolders"}), 1), domain_name), 1)

1..users
|> Task.async_stream(fn i -> Directory.create_account(domain, "big#{i}", %{password_hash: hash}) end,
  max_concurrency: 16
)
|> Stream.run()

# Accounts are in the directory; folders and messages on the tenant's shard.
%{rows: [[account_ids]]} =
  Repo.query!("SELECT array_agg(id) FROM accounts WHERE domain_id = $1", [domain.id])

shard = Shards.repo(domain.tenant_id)

inbox = "SELECT id FROM folders WHERE account_id = ANY($1) AND name = 'INBOX'"

recount = fn ->
  shard.query!(
    """
    UPDATE folders f
    SET messages_count = s.cnt, unseen_count = s.unseen,
        uidnext = GREATEST(f.uidnext, s.max_uid + 1), highest_modseq = f.highest_modseq + 1
    FROM (
      SELECT fo.id, count(m.uid) AS cnt, count(m.uid) FILTER (WHERE (m.flags & 1) = 0) AS unseen,
             COALESCE(max(m.uid), 0) AS max_uid
      FROM folders fo LEFT JOIN messages m ON m.folder_id = fo.id
      WHERE fo.id IN (#{inbox})
      GROUP BY fo.id
    ) s
    WHERE f.id = s.id
    """,
    [account_ids],
    timeout: :infinity
  )
end

if n = opts[:fill] do
  {us, %{num_rows: rows}} =
    :timer.tc(fn ->
      shard.query!(
        """
        INSERT INTO messages (tenant_id, folder_id, uid, modseq, stored_at, internal_date, size,
                              flags, keywords, class, message_id, from_addr, subject)
        SELECT f.tenant_id, f.id, g, g + 1, now(), now() - (($2 - g) * interval '20 minutes'), 2000,
               CASE WHEN g <= $2 - 10 THEN 1 ELSE 0 END, '{}', 2,
               '<bulk-' || f.id || '-' || g || '@load>', 'sender@example.org', 'bulk ' || g
        FROM folders f
        CROSS JOIN generate_series(1, $2) AS g
        WHERE f.id IN (#{inbox})
          AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.folder_id = f.id)
        """,
        [account_ids, n],
        timeout: :infinity
      )
    end)

  recount.()
  IO.puts("inserted #{rows} messages in #{div(us, 1000)} ms")
end

if p = opts[:delete_fraction] do
  %{num_rows: rows} =
    shard.query!(
      "DELETE FROM messages WHERE folder_id IN (#{inbox}) AND (flags & 1) <> 0 AND random() < $2",
      [account_ids, p],
      timeout: :infinity
    )

  recount.()
  IO.puts("deleted #{rows} messages (~#{p * 100}% of the older ones)")
end

if opts[:delete_every_other] do
  %{num_rows: rows} =
    shard.query!(
      "DELETE FROM messages WHERE folder_id IN (#{inbox}) AND uid % 2 = 0",
      [account_ids],
      timeout: :infinity
    )

  recount.()
  IO.puts("deleted #{rows} messages (every even UID)")
end

%{rows: [[folders, msgs, avg_runs]]} =
  shard.query!(
    """
    SELECT count(*), COALESCE(sum(n), 0), round(avg(runs), 1)
    FROM (
      SELECT folder_id, count(*) AS n, count(DISTINCT uid - rn) AS runs
      FROM (SELECT folder_id, uid, row_number() OVER (PARTITION BY folder_id ORDER BY uid) AS rn
            FROM messages WHERE folder_id IN (#{inbox})) t
      GROUP BY folder_id
    ) x
    """,
    [account_ids],
    timeout: :infinity
  )

IO.puts("#{domain_name}: #{folders} INBOXes, #{msgs} messages, ~#{avg_runs} UID runs per folder")
