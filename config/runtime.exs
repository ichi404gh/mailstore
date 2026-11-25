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

if config_env() == :prod do
  config :mailstore, Mailstore.Repo,
    url: System.fetch_env!("DATABASE_URL"),
    pool_size: String.to_integer(System.get_env("POOL_SIZE", "20"))
end
