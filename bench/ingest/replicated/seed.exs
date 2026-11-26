# Many mailboxes over two shards, in bulk SQL (create_account one by one would take
# an hour at this size). Runs inside the app, with shard 1 already bootstrapped:
#
#   MAILSTORE_SHARD1_URL=ecto://postgres:postgres@localhost:55501/mailstore_dev_shard1 \
#     mix run bench/ingest/replicated/seed.exs -- --users 860000 --tenants 200 \
#     [--shard2 ecto://postgres:postgres@localhost:55502/mailstore_dev_shard2]
#
# Tenant t (t0..t199) owns domain dT.load.test and lives on shard 1 + rem(t, 2).
# Mailbox user<i> belongs to tenant rem(i, tenants): user<i>@d<rem(i, tenants)>.load.test,
# the layout `produce.exs --tenants` sends to. Every mailbox gets the five default
# folders on its shard. Password: pw.
alias Mailstore.{Directory, Repo, Shards}

{opts, _, _} =
  OptionParser.parse(Enum.reject(System.argv(), &(&1 == "--")),
    strict: [users: :integer, tenants: :integer, shard2: :string]
  )

users = opts[:users] || 860_000
tenants = opts[:tenants] || 200

shard2_url =
  opts[:shard2] || "ecto://postgres:postgres@localhost:55502/mailstore_dev_shard2"

{:ok, shard2} = Shards.register("shard2", shard2_url, create: true, migrate: true, pool_size: 10)
hash = Directory.hash_password("pw", 1_000)
t0 = System.monotonic_time(:millisecond)
elapsed = fn -> "#{div(System.monotonic_time(:millisecond) - t0, 1000)} s" end

Repo.query!(
  """
  INSERT INTO tenants (name, shard, inserted_at, updated_at)
  SELECT 't' || t, CASE WHEN t % 2 = 0 THEN 1 ELSE $2 END, now(), now()
  FROM generate_series(0, $1 - 1) t
  """,
  [tenants, shard2]
)

Repo.query!("""
INSERT INTO domains (tenant_id, name, status, inserted_at, updated_at)
SELECT id, 'd' || substr(name, 2) || '.load.test', 'active', now(), now() FROM tenants
WHERE name ~ '^t[0-9]+$'
""")

Repo.query!(
  """
  INSERT INTO accounts (tenant_id, domain_id, local_part, address, password_hash, inserted_at, updated_at)
  SELECT d.tenant_id, d.id, 'user' || i, 'user' || i || '@' || d.name, $3, now(), now()
  FROM generate_series(1, $1) i
  JOIN domains d ON d.name = 'd' || (i % $2) || '.load.test'
  """,
  [users, tenants, hash],
  timeout: :infinity
)

IO.puts("directory: #{tenants} tenants, #{users} accounts (#{elapsed.()})")

# Folders, per shard, in chunks: the accounts of that shard's tenants.
for shard <- [1, shard2] do
  repo = Shards.repo_for(shard)

  %{rows: rows} =
    Repo.query!(
      """
      SELECT a.id, a.tenant_id FROM accounts a JOIN tenants t ON t.id = a.tenant_id
      WHERE t.shard = $1 AND t.name ~ '^t[0-9]+$' ORDER BY a.id
      """,
      [shard],
      timeout: :infinity
    )

  rows
  |> Enum.chunk_every(20_000)
  |> Enum.each(fn chunk ->
    [ids, tids] = chunk |> Enum.zip() |> Enum.map(&Tuple.to_list/1)

    repo.query!(
      """
      INSERT INTO folders (tenant_id, account_id, name, special_use, uidvalidity, inserted_at, updated_at)
      SELECT a.tid, a.id, f.name, f.su, nextval('folder_uidvalidity'), now(), now()
      FROM unnest($1::bigint[], $2::bigint[]) AS a(id, tid)
      CROSS JOIN (VALUES ('INBOX', NULL), ('Sent', '\\Sent'), ('Drafts', '\\Drafts'),
                         ('Trash', '\\Trash'), ('Junk', '\\Junk')) AS f(name, su)
      """,
      [ids, tids],
      timeout: :infinity
    )
  end)

  repo.query!("ANALYZE folders")
  IO.puts("shard #{shard}: #{length(rows)} accounts, #{length(rows) * 5} folders (#{elapsed.()})")
end

Repo.query!("ANALYZE")
