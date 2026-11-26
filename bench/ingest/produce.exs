# Synthetic records in the format of the inbound topic (see Mailstore.Ingest.Record):
#
#   mix run --no-start bench/ingest/produce.exs -- --count 100000 [--rate 0] [--users 5000]
#     [--domain load.test] [--topic mailstore.inbound] [--unknown 0.01]
#     [--seed 1] [--hosts localhost:9092]
#
# --rate 0 = as fast as possible (a backlog); otherwise records per second. Recipients
# are user1..userN@DOMAIN (bench/seed.exs), a share --unknown of them do not exist;
# --to sends everything to one address.
# Each message is unique (Message-ID with --seed and the index); 70% are warmup. The raw
# message is ~4 KB like a real one (authentication results, DKIM, two Received hops,
# multipart/alternative); the record also carries the decoded body twice.
{opts, _, _} =
  OptionParser.parse(Enum.reject(System.argv(), &(&1 == "--")),
    strict: [
      count: :integer,
      rate: :integer,
      users: :integer,
      domain: :string,
      topic: :string,
      unknown: :float,
      seed: :integer,
      hosts: :string,
      to: :string
    ]
  )

count = opts[:count] || 100_000
rate = opts[:rate] || 0
users = opts[:users] || 5_000
domain = opts[:domain] || "load.test"
topic = opts[:topic] || "mailstore.inbound"
unknown = opts[:unknown] || 0.01
seed = opts[:seed] || 1

hosts =
  for hp <- String.split(opts[:hosts] || "localhost:9092", ",") do
    [h, p] = String.split(hp, ":")
    {String.to_charlist(h), String.to_integer(p)}
  end

{:ok, _} = Application.ensure_all_started(:brod)
:ok = :brod.start_client(hosts, :producer, [])
:ok = :brod.start_producer(:producer, topic, max_linger_ms: 5, max_batch_size: 1_000_000)
{:ok, partitions} = :brod.get_partitions_count(:producer, topic)

known = :counters.new(1, [])
dkim = Base.encode64(:crypto.strong_rand_bytes(256))

body =
  String.duplicate("Thanks for reaching out last week! Please send me a sample of the work. ", 6)

record = fn i ->
  rcpt =
    cond do
      opts[:to] -> opts[:to]
      :rand.uniform() < unknown -> "nobody#{i}@#{domain}"
      true -> "user#{:rand.uniform(users)}@#{domain}"
    end

  now = DateTime.utc_now() |> DateTime.truncate(:second)
  date = Calendar.strftime(now, "%a, %d %b %Y %H:%M:%S +0000")
  msg_id = "<s#{seed}-#{i}-#{System.unique_integer([:positive])}@sender.example>"
  boundary = "--_NmP-#{Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)}-Part_1"

  raw =
    Enum.join(
      [
        "Authentication-Results: mx1.relay.example; dmarc=pass (p=quarantine dis=none) header.from=sender.example",
        "Authentication-Results: mx1.relay.example; spf=pass smtp.mailfrom=sender.example",
        "Authentication-Results: mx1.relay.example;\r\n\tdkim=pass (2048-bit key; unprotected) header.d=sender.example header.s=google",
        "Received: from mail-qt1-f178.example.com (mail-qt1-f178.example.com [192.0.2.178])\r\n\tby mx1.relay.example (Postfix) with ESMTPS id 0B56CC013D\r\n\tfor <#{rcpt}>; #{date} (UTC)",
        "Received: by mail-qt1-f178.example.com with SMTP id d75a77b69052e-#{i}\r\n        for <#{rcpt}>; #{date}",
        "DKIM-Signature: v=1; a=rsa-sha256; c=relaxed/relaxed; d=sender.example; s=google;\r\n        h=content-type:mime-version:date:subject:to:from:message-id;\r\n        b=" <>
          Enum.join(for(<<chunk::binary-64 <- dkim>>, do: chunk), "\r\n         "),
        "Message-ID: #{msg_id}",
        "From: Ella McDonald <ella@sender.example>",
        "To: #{rcpt}",
        "Subject: Looking forward to working together. | #{i}",
        "Date: #{date}",
        "MIME-Version: 1.0",
        "Content-Type: multipart/alternative;\r\n boundary=\"#{boundary}\"",
        "",
        "--#{boundary}",
        "Content-Type: text/plain; charset=utf-8",
        "Content-Transfer-Encoding: quoted-printable",
        "",
        body,
        "--#{boundary}",
        "Content-Type: text/html; charset=utf-8",
        "Content-Transfer-Encoding: quoted-printable",
        "",
        "<p>" <> body <> "</p>",
        "--#{boundary}--",
        ""
      ],
      "\r\n"
    )

  warmup = :rand.uniform() < 0.7

  # The fields Mailstore.Ingest.Record reads, plus decoded copies of the body: producers
  # often carry such extras, and they make the record as heavy as a real one.
  json =
    Jason.encode!(%{
      message_id: msg_id,
      message_received_ts: DateTime.to_iso8601(now),
      sender_email: "ella@sender.example",
      rcp_email: rcpt,
      subject: "Looking forward to working together. | #{i}",
      text: body,
      html: "<p>" <> body <> "</p>",
      is_warmup: :rand.uniform() < 0.7,
      is_spamtest: false,
      envelope_content: raw
    })

  if String.starts_with?(rcpt, "user"), do: :counters.add(known, 1, 1)
  {"mx#{rem(i, 30)}", json}
end

# Send in chunks; with --rate, pace each chunk.
chunk = if rate > 0, do: max(div(rate, 20), 1), else: 500
started = System.monotonic_time(:millisecond)

1..count
|> Stream.chunk_every(chunk)
|> Stream.with_index()
|> Enum.each(fn {indexes, n} ->
  indexes
  |> Enum.map(record)
  |> Enum.group_by(fn {key, _} -> :erlang.phash2(key, partitions) end)
  |> Enum.each(fn {partition, kvs} ->
    :ok =
      :brod.produce_sync(:producer, topic, partition, "", Enum.map(kvs, fn {k, v} -> {k, v} end))
  end)

  if rate > 0 do
    due = started + div((n + 1) * chunk * 1000, rate)
    wait = due - System.monotonic_time(:millisecond)
    if wait > 0, do: Process.sleep(wait)
  end
end)

secs = (System.monotonic_time(:millisecond) - started) / 1000

IO.puts(
  "produced #{count} records to #{topic} in #{Float.round(secs, 1)} s (#{round(count / secs)}/s), " <>
    "#{:counters.get(known, 1)} to existing accounts, Message-IDs <s#{seed}-...>"
)
