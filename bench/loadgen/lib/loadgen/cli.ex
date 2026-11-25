defmodule Loadgen.CLI do
  @moduledoc "Options shared by all scenarios."

  @common [
    host: :string,
    port: :integer,
    sessions: :integer,
    users: :integer,
    user_format: :string,
    password: :string,
    folders: :string,
    rate: :integer,
    report: :integer,
    source_ips: :string,
    server_node: :string,
    tls: :boolean
  ]

  @common_help """
    --host 127.0.0.1        --port 1143
    --sessions N            total sessions to open
    --users N               accounts user1..userN (see bench/seed.exs)
    --user-format FMT       default "user%d@load.test"
    --password PW           default "pw"
    --rate N                new sessions per second during the ramp
    --report SECONDS        progress line interval
    --source-ips LIST       local IPs to spread connections over (each IP gives ~16k
                            ephemeral ports on macOS, ~28k on Linux)
    --tls                   connect with TLS (e.g. HAProxy on 10993; certificate not verified)
    --server-node NODE      e.g. mailstore@myhost: read server memory/CPU over :erpc
                            (start this VM with --sname and the server's --cookie)
  """

  def common_help, do: @common_help

  def parse(args, extra_switches, extra_defaults) do
    {opts, _, invalid} = OptionParser.parse(args, strict: @common ++ extra_switches)
    if invalid != [], do: Mix.raise("unknown options: #{inspect(invalid)}")

    common = %{
      host: opts |> Keyword.get(:host, "127.0.0.1") |> String.to_charlist(),
      port: Keyword.get(opts, :port, 1143),
      sessions: Keyword.get(opts, :sessions, 1000),
      users: Keyword.get(opts, :users, 500),
      user_format: Keyword.get(opts, :user_format, "user%d@load.test"),
      password: Keyword.get(opts, :password, "pw"),
      folders: opts |> Keyword.get(:folders, "INBOX,Junk") |> String.split(","),
      rate: Keyword.get(opts, :rate, 500),
      report: Keyword.get(opts, :report, 5),
      source_ips: opts |> Keyword.get(:source_ips, "") |> parse_ips(),
      server_node: server_node(opts[:server_node]),
      tls: Keyword.get(opts, :tls, false)
    }

    if common.tls, do: {:ok, _} = Application.ensure_all_started(:ssl)

    extra =
      Map.new(extra_defaults, fn {key, default} -> {key, Keyword.get(opts, key, default)} end)

    Map.merge(common, extra)
  end

  defp parse_ips(""), do: []

  defp parse_ips(list) do
    for ip <- String.split(list, ",") do
      {:ok, addr} = ip |> String.to_charlist() |> :inet.parse_address()
      addr
    end
  end

  defp server_node(nil), do: nil

  defp server_node(name) do
    unless Node.alive?(),
      do:
        Mix.raise(
          "--server-node needs a distributed VM: elixir --sname loadgen --cookie COOKIE -S mix loadgen.<scenario> ..."
        )

    node = String.to_atom(name)

    unless Node.connect(node),
      do: Mix.raise("cannot connect to #{node} (cookie? is it running with --sname?)")

    node
  end
end
