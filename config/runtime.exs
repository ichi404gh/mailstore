import Config

# Periodic one-line report (connections, actors, memory, commands/s), e.g. during load tests.
if ms = System.get_env("MAILSTORE_REPORT_MS") do
  config :mailstore, :telemetry_report_interval, String.to_integer(ms)
end

# Extra socket options for IMAP connections, for memory experiments, e.g.
#   MAILSTORE_IMAP_SOCKET_OPTS="buffer=4096,recbuf=16384,sndbuf=16384"
if socket_opts = System.get_env("MAILSTORE_IMAP_SOCKET_OPTS") do
  parsed =
    for pair <- String.split(socket_opts, ",", trim: true) do
      [key, value] = String.split(pair, "=")
      {String.to_atom(key), String.to_integer(value)}
    end

  imap = Application.get_env(:mailstore, :imap, [])
  config :mailstore, :imap, Keyword.put(imap, :socket_opts, parsed)
end

# Port 0 for one-off scripts that start the app next to a running server.
if port = System.get_env("MAILSTORE_IMAP_PORT") do
  config :mailstore,
         :imap,
         Application.get_env(:mailstore, :imap, [])
         |> Keyword.put(:port, String.to_integer(port))
         |> Keyword.put(:proxy_port, if(port == "0", do: 0, else: String.to_integer(port) + 1))
end

# Cluster peers, e.g. MAILSTORE_CLUSTER_NODES="ms1@10.0.0.1,ms2@10.0.0.2" (start each node
# with --name and the same --cookie).
if nodes = System.get_env("MAILSTORE_CLUSTER_NODES") do
  config :mailstore, :cluster,
    nodes: nodes |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)
end

# Inbound queue, e.g. MAILSTORE_KAFKA_HOSTS="kfk1:9092,kfk2:9092".
if hosts = System.get_env("MAILSTORE_KAFKA_HOSTS") do
  hosts =
    for hp <- String.split(hosts, ",", trim: true) do
      [host, port] = String.split(hp, ":")
      {String.to_atom(host), String.to_integer(port)}
    end

  env = fn name, default -> System.get_env(name, default) end

  ingest =
    Application.get_env(:mailstore, :ingest, [])
    |> Keyword.merge(enabled: true, hosts: hosts)
    |> Keyword.merge(
      for {key, var} <- [
            group_id: "MAILSTORE_KAFKA_GROUP",
            dead_letter_topic: "MAILSTORE_KAFKA_DEAD_LETTER"
          ],
          value = env.(var, nil),
          do: {key, value}
    )

  ingest =
    case env.("MAILSTORE_KAFKA_TOPICS", nil) do
      nil -> ingest
      topics -> Keyword.put(ingest, :topics, String.split(topics, ",", trim: true))
    end

  ingest =
    case env.("MAILSTORE_KAFKA_OFFSET_RESET", nil) do
      nil -> ingest
      "earliest" -> Keyword.put(ingest, :offset_reset_policy, :earliest)
      "latest" -> Keyword.put(ingest, :offset_reset_policy, :latest)
    end

  ingest =
    Keyword.merge(
      ingest,
      for(
        {key, var} <- [
          batch_size: "MAILSTORE_INGEST_BATCH_SIZE",
          batch_timeout: "MAILSTORE_INGEST_BATCH_TIMEOUT",
          batchers: "MAILSTORE_INGEST_BATCHERS"
        ],
        value = env.(var, nil),
        do: {key, String.to_integer(value)}
      )
    )

  config :mailstore, :ingest, ingest
end

# Retention policies as JSON, e.g.
#   MAILSTORE_RETENTION='{"policies": {"standard": {"id": 0, "keep_days": 365, "cold_after_days": 30},
#                                      "short": {"id": 1, "keep_days": 30}},
#                         "classes": {"warmup": "short"}, "default": "standard"}'
if json = System.get_env("MAILSTORE_RETENTION") do
  r = Jason.decode!(json)
  atom = &String.to_atom/1

  policies =
    for {name, p} <- Map.fetch!(r, "policies"),
        do: {atom.(name), Enum.map(p, fn {k, v} -> {atom.(k), v} end)}

  config :mailstore, :retention,
    policies: policies,
    classes:
      for({class, policy} <- Map.get(r, "classes", %{}), do: {atom.(class), atom.(policy)}),
    default: atom.(Map.get(r, "default", "standard"))
end

# A single-shard production install: the shard's database (created and owned by the
# platform, not by mailstore); further shards are added with Mailstore.Shards.register/3.
if url = System.get_env("MAILSTORE_SHARD1_URL") do
  config :mailstore, :bootstrap_shards, [%{name: "shard1", url: url}]
end

if config_env() == :prod do
  config :mailstore, Mailstore.Repo,
    url: System.fetch_env!("DATABASE_URL"),
    pool_size: String.to_integer(System.get_env("POOL_SIZE", "20"))
end
