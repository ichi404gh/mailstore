# Seed accounts for load tests. Runs inside the mailstore app:
#
#   mix run bench/seed.exs -- --users 5000 --domain load.test --password pw --messages 3
#
# Accounts are named user1..userN@DOMAIN. The password is hashed once and reused.
# --iterations sets the PBKDF2 cost of that hash: the default 1000 keeps LOGIN cheap so
# the test measures sessions, not hashing; use 100000 to include production-like auth cost.
# Re-running is safe: existing accounts are skipped.
alias Mailstore.{Delivery, Directory}

{opts, _, _} =
  OptionParser.parse(Enum.reject(System.argv(), &(&1 == "--")),
    strict: [users: :integer, domain: :string, password: :string, messages: :integer, iterations: :integer]
  )

users = opts[:users] || 5_000
domain_name = opts[:domain] || "load.test"
password = opts[:password] || "pw"
messages = opts[:messages] || 3
hash = Directory.hash_password(password, opts[:iterations] || 1_000)

domain =
  Directory.get_domain_by_name(domain_name) ||
    elem(Directory.create_domain(elem(Directory.create_tenant(%{name: "loadtest"}), 1), domain_name), 1)

{micros, created} =
  :timer.tc(fn ->
    1..users
    |> Task.async_stream(
      fn i ->
        case Directory.create_account(domain, "user#{i}", %{password_hash: hash}) do
          {:ok, account} ->
            for m <- 1..messages//1 do
              raw =
                "From: Sender #{m} <sender#{m}@example.org>\r\nTo: #{account.address}\r\n" <>
                  "Subject: seeded #{m}\r\nMessage-ID: <seed-#{i}-#{m}@load>\r\n\r\nseeded body #{m}\r\n"

              {:ok, _} = Delivery.deliver(raw, account.address, class: :reply)
            end

            1

          {:error, _exists} ->
            0
        end
      end,
      max_concurrency: 16,
      timeout: :infinity
    )
    |> Enum.reduce(0, fn {:ok, n}, acc -> acc + n end)
  end)

IO.puts("#{created} new accounts (#{users - created} existed) in #{div(micros, 1000)} ms: user1..user#{users}@#{domain_name}")
