# Runs a mailstore node with ingest on and measures it until --expect records are done:
#
#   MAILSTORE_KAFKA_HOSTS=localhost:9092 MAILSTORE_KAFKA_GROUP=bench-1 \
#   MAILSTORE_KAFKA_OFFSET_RESET=earliest MAILSTORE_IMAP_PORT=0 \
#     mix run bench/ingest/measure.exs -- --expect 100000 [--pg-container mailstore-pg]
#
# Prints a line per second and a summary: records/s, CPU of this node (`ps`, like the
# other benchmarks) and of Postgres (container cgroup) per stored message, batch sizes
# and durations, and lag = now - Kafka timestamp of the oldest record of each batch.
# MAILSTORE_INGEST_BATCH_SIZE=1 gives one transaction per message, for comparison.
{opts, _, _} =
  OptionParser.parse(Enum.reject(System.argv(), &(&1 == "--")),
    strict: [expect: :integer, idle: :integer, pg_container: :string]
  )

# --idle N: stop once no batch arrived for N seconds (when the count is not known).
expect = opts[:expect] || if(opts[:idle], do: :infinity, else: raise("--expect N or --idle S"))
idle_after = opts[:idle]
pg = opts[:pg_container]
table = :ets.new(:ingest_bench, [:public, :set])

for key <- [:messages, :stored, :duplicate, :unknown, :failed, :batches],
    do: :ets.insert(table, {key, 0})

:ets.insert(table, {:durations, []})
:ets.insert(table, {:lags, []})

:telemetry.attach(
  "ingest-bench",
  [:mailstore, :ingest, :batch],
  fn _, m, _, _ ->
    for key <- [:messages, :stored, :duplicate, :unknown, :failed],
        do: :ets.update_counter(table, key, Map.fetch!(m, key))

    :ets.update_counter(table, :batches, 1)
    [{_, ds}] = :ets.lookup(table, :durations)

    :ets.insert(
      table,
      {:durations, [System.convert_time_unit(m.duration, :native, :microsecond) | ds]}
    )

    if lag = m[:lag_ms] do
      [{_, ls}] = :ets.lookup(table, :lags)
      :ets.insert(table, {:lags, [lag | ls]})
    end
  end,
  nil
)

cpu_ms = fn ->
  {out, 0} = System.cmd("ps", ["-o", "cputime=", "-p", System.pid()])
  # [[dd-]hh:]mm:ss.cc
  parts = out |> String.trim() |> String.replace("-", ":") |> String.split(":")
  {secs, _} = Float.parse(List.last(parts))

  mins =
    parts
    |> Enum.drop(-1)
    |> Enum.reverse()
    |> Enum.zip([1, 60, 1440])
    |> Enum.map(fn {v, k} -> String.to_integer(v) * k end)
    |> Enum.sum()

  round((mins * 60 + secs) * 1000)
end

# --pg-container a,b,c sums the containers (primaries and standbys of all shards).
pg_ms = fn ->
  if pg do
    pg
    |> String.split(",", trim: true)
    |> Enum.map(fn name ->
      {out, 0} = System.cmd("docker", ["exec", name, "cat", "/sys/fs/cgroup/cpu.stat"])
      [_, us] = Regex.run(~r/usage_usec (\d+)/, out)
      div(String.to_integer(us), 1000)
    end)
    |> Enum.sum()
  end
end

done = fn -> :ets.lookup_element(table, :messages, 2) end
# Start the clock at the first batch, not at node start (Kafka join takes seconds).
Stream.repeatedly(fn -> Process.sleep(20) end) |> Enum.find(fn _ -> done.() > 0 end)
t0 = System.monotonic_time(:millisecond)
{c0, p0, n0} = {cpu_ms.(), pg_ms.(), done.()}

# Until --expect records, or --idle seconds without progress (the clock stops at the
# last progress then).
last_t =
  Stream.repeatedly(fn ->
    Process.sleep(1000)
    n = done.()

    IO.puts(
      "#{Float.round((System.monotonic_time(:millisecond) - t0) / 1000, 1)} s: #{n}/#{expect}"
    )

    {n, System.monotonic_time(:millisecond)}
  end)
  |> Enum.reduce_while({0, t0}, fn {n, t}, {prev, since} ->
    since = if n > prev, do: t, else: since

    cond do
      expect != :infinity and n >= expect -> {:halt, t}
      idle_after && t - since >= idle_after * 1000 -> {:halt, since}
      true -> {:cont, {n, since}}
    end
  end)

secs = (last_t - t0) / 1000
{c1, p1} = {cpu_ms.(), pg_ms.()}
get = fn key -> :ets.lookup_element(table, key, 2) end
stored = get.(:stored)
n = get.(:messages) - n0

pct = fn list, p ->
  list |> Enum.sort() |> Enum.at(min(length(list) - 1, trunc(length(list) * p)))
end

durations = get.(:durations)
lags = get.(:lags)

IO.puts("""

records: #{get.(:messages)} (stored #{stored}, duplicate #{get.(:duplicate)}, unknown recipient #{get.(:unknown)}, failed #{get.(:failed)})
rate: #{round(n / secs)} records/s over #{Float.round(secs, 1)} s
batches: #{get.(:batches)}, #{Float.round(get.(:messages) / max(get.(:batches), 1), 1)} records per batch, duration p50 #{pct.(durations, 0.5)} us, p99 #{pct.(durations, 0.99)} us
CPU per stored message: node #{Float.round((c1 - c0) / max(stored, 1) * 1000)} us#{if p0, do: ", Postgres #{Float.round((p1 - p0) / max(stored, 1) * 1000)} us", else: ""}
CPU: node #{Float.round((c1 - c0) / 1000 / secs, 2)} cores#{if p0, do: ", Postgres #{Float.round((p1 - p0) / 1000 / secs, 2)} cores", else: ""}
#{if lags != [], do: "lag of the oldest record in a batch (Kafka timestamp -> committed): p50 #{pct.(lags, 0.5)} ms, p99 #{pct.(lags, 0.99)} ms", else: ""}
""")
