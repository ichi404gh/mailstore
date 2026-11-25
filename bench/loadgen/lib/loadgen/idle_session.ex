defmodule Loadgen.IdleSession do
  @moduledoc """
  One simulated client: connect, LOGIN, SELECT a folder, IDLE, then sit there.
  Reports `{:ready, pid, setup_us}` or `{:failed, pid, reason}` to the coordinator and
  `{:push, pid}` for every EXISTS the server pushes. Hibernates between messages to keep
  the load generator's own memory low.
  """
  alias Loadgen.ImapClient

  def start(coordinator, cfg, user, folder, source_ip) do
    spawn(fn -> run(coordinator, cfg, user, folder, source_ip) end)
  end

  defp run(coord, cfg, user, folder, source_ip) do
    t0 = System.monotonic_time(:microsecond)

    until = Map.get(cfg, :until, "idle")

    with {:ok, sock} <- ImapClient.connect(cfg.host, cfg.port, source_ip, cfg.tls),
         :ok <- setup(sock, user, folder, cfg.password, until) do
      send(coord, {:ready, self(), System.monotonic_time(:microsecond) - t0})
      :ok = ImapClient.setopts(sock, active: :once)
      :erlang.hibernate(__MODULE__, :idle_loop, [coord, sock])
    else
      {:error, reason} -> send(coord, {:failed, self(), reason})
      other -> send(coord, {:failed, self(), other})
    end
  end

  # Run the setup steps up to and including --until (connect < login < select < idle);
  # stopping early is for bisecting server memory per step.
  defp setup(sock, user, folder, password, until) do
    steps = [
      {"login", fn -> login(sock, user, password) end},
      {"select",
       fn -> ok(ImapClient.command(sock, "a2", "SELECT #{quote_string(folder)}"), :select) end},
      {"idle", fn -> with(:ok <- ImapClient.idle(sock, "a3"), do: {:ok, :idle}) end}
    ]

    count = Enum.find_index(["connect", "login", "select", "idle"], &(&1 == until)) || 3

    steps
    |> Enum.take(count)
    |> Enum.reduce_while(:ok, fn {_name, run}, :ok ->
      case run.() do
        {:ok, _} -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp login(sock, user, password),
    do: ok(ImapClient.command(sock, "a1", "LOGIN #{user} #{quote_string(password)}"), :login)

  @doc false
  def idle_loop(coord, {_mod, raw} = sock) do
    {data, closed} = ImapClient.tags(sock)

    receive do
      {^data, ^raw, "* " <> rest} ->
        if String.contains?(rest, "EXISTS"), do: send(coord, {:push, self()})
        :ok = ImapClient.setopts(sock, active: :once)

      {^data, ^raw, _line} ->
        :ok = ImapClient.setopts(sock, active: :once)

      {^closed, ^raw} ->
        send(coord, {:closed, self()})
        exit(:normal)

      :stop ->
        ImapClient.send(sock, "DONE\r\nz LOGOUT\r\n")
        ImapClient.close(sock)
        exit(:normal)
    end

    :erlang.hibernate(__MODULE__, :idle_loop, [coord, sock])
  end

  defp ok({:ok, line}, _step), do: {:ok, line}
  defp ok({status, line}, step) when status in [:no, :bad], do: {:error, {step, String.trim(line)}}
  defp ok({:error, reason}, step), do: {:error, {step, reason}}

  defp quote_string(s), do: ~s(") <> String.replace(s, ~s("), ~s(\\")) <> ~s(")
end
