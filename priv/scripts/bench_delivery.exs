# Delivery throughput: one transaction per message, as in the current prototype.
#
#   MIX_ENV=dev mix run priv/scripts/bench_delivery.exs -- --accounts 1000 --messages 20000 --concurrency 64
#
# Expect it to be bound by commit latency x concurrency. The next step (group commit:
# the actor batches appends arriving within a few ms into one transaction) is what this
# number is meant to compare against.
alias Mailstore.{Delivery, Directory, Repo}

{opts, _, _} =
  OptionParser.parse(Enum.reject(System.argv(), &(&1 == "--")),
    strict: [accounts: :integer, messages: :integer, concurrency: :integer]
  )

accounts = opts[:accounts] || 1_000
messages = opts[:messages] || 20_000
concurrency = opts[:concurrency] || 64
tag = System.unique_integer([:positive])

{:ok, tenant} = Directory.create_tenant(%{name: "bench-#{tag}"})
{:ok, domain} = Directory.create_domain(tenant, "bench#{tag}.test")

{t_setup, addresses} =
  :timer.tc(fn ->
    1..accounts
    |> Task.async_stream(
      fn i ->
        {:ok, a} = Directory.create_account(domain, "user#{i}")
        a.address
      end,
      max_concurrency: 16
    )
    |> Enum.map(fn {:ok, addr} -> addr end)
    |> List.to_tuple()
  end)

IO.puts("created #{accounts} accounts in #{div(t_setup, 1000)} ms")

body = String.duplicate("Hey, checking in on the proposal I sent last week. Any thoughts? ", 25)

raw = fn i ->
  "From: Warm Up <warm#{rem(i, 97)}@sender.example>\r\nTo: x\r\nSubject: Re: proposal #{i}\r\n" <>
    "Message-ID: <bench-#{tag}-#{i}@sender.example>\r\nDate: Fri, 20 Nov 2026 12:00:00 +0000\r\n\r\n" <>
    body <> "\r\n"
end

{t, _} =
  :timer.tc(fn ->
    1..messages
    |> Task.async_stream(
      fn i ->
        {:ok, [{:stored, _, _, _}]} =
          Delivery.deliver(raw.(i), elem(addresses, rem(i, accounts)), class: :warmup)
      end,
      max_concurrency: concurrency,
      ordered: false
    )
    |> Stream.run()
  end)

secs = t / 1_000_000

IO.puts(
  "delivered #{messages} messages in #{Float.round(secs, 2)} s = #{round(messages / secs)} msg/s"
)

%{rows: [[raw_bytes, stored_bytes]]} =
  Repo.query!(
    """
    SELECT sum(m.size), sum(octet_length(b.data))
    FROM messages m JOIN message_bodies b USING (folder_id, uid, stored_at)
    WHERE m.tenant_id = $1
    """,
    [tenant.id]
  )

IO.puts(
  "body bytes: raw #{raw_bytes}, stored #{stored_bytes} (#{Float.round(raw_bytes / stored_bytes, 1)}x)"
)

IO.puts("live account actors: #{Registry.count(Mailstore.Mailbox.Registry)}")
