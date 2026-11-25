defmodule Loadgen.PollerSession do
  @moduledoc """
  One simulated sequencer connection: LOGIN, SELECT INBOX, then every `interval_ms`
  run the configured cycle of commands over the same connection. Latency of every
  command goes to `Loadgen.Stats`. Optionally reconnects (LOGOUT, new LOGIN) after a
  random lifetime around `reconnect_mean_ms`, like real clients do every ~30 minutes.

  Cycle steps:
    noop          NOOP
    status        STATUS INBOX (MESSAGES UIDNEXT UNSEEN)
    select        SELECT INBOX again
    search_unseen UID SEARCH UNSEEN
    search_new    UID SEARCH UID <last+1>:*   (remembers new UIDs)
    fetch_new     UID FETCH <new> (UID FLAGS BODY.PEEK[])   (skipped when nothing new)
    store_seen    UID STORE <new> +FLAGS.SILENT (\\Seen)      (skipped when nothing new)
    fetch_flags   UID FETCH 1:* (UID FLAGS)   (full flag resync)
  """
  alias Loadgen.{ImapClient, Stats}

  def start(coord, cfg, user, source_ip) do
    spawn(fn -> init(coord, cfg, user, source_ip) end)
  end

  defp init(coord, cfg, user, source_ip) do
    t0 = now_us()

    st = %{
      coord: coord,
      cfg: cfg,
      user: user,
      ip: source_ip,
      sock: nil,
      tag: 0,
      last_uid: 0,
      new: []
    }

    case open(st) do
      {:ok, st} ->
        send(coord, {:ready, self(), now_us() - t0})
        # Spread the first cycle over one interval so sessions do not poll in lockstep.
        Process.send_after(self(), :tick, :rand.uniform(max(cfg.interval_ms, 1)))
        :erlang.hibernate(__MODULE__, :loop, [arm_reconnect(st)])

      {:error, reason} ->
        send(coord, {:failed, self(), reason})
    end
  end

  @doc false
  def loop(st) do
    receive do
      :tick ->
        st = st |> maybe_reconnect() |> cycle()
        Process.send_after(self(), :tick, st.cfg.interval_ms)
        :erlang.hibernate(__MODULE__, :loop, [st])

      :stop ->
        if st.sock, do: ImapClient.send(st.sock, "z LOGOUT\r\n")
        if st.sock, do: ImapClient.close(st.sock)
        exit(:normal)
    end
  end

  defp open(st) do
    with {:ok, sock} <- ImapClient.connect(st.cfg.host, st.cfg.port, st.ip, st.cfg.tls),
         st = %{st | sock: sock},
         {:ok, st, _} <- run(st, nil, "LOGIN #{st.user} #{quote_string(st.cfg.password)}"),
         {:ok, st, lines} <- run(st, nil, "SELECT INBOX") do
      {:ok, %{st | last_uid: max(st.last_uid, uidnext(lines) - 1)}}
    else
      {:error, reason} -> {:error, reason}
      {status, _st, lines} -> {:error, {status, Enum.take(lines, 1)}}
    end
  end

  defp arm_reconnect(%{cfg: %{reconnect_mean_ms: 0}} = st), do: Map.put(st, :reconnect_at, nil)

  defp arm_reconnect(st) do
    lifetime = round(st.cfg.reconnect_mean_ms * (0.5 + :rand.uniform()))
    Map.put(st, :reconnect_at, System.monotonic_time(:millisecond) + lifetime)
  end

  defp maybe_reconnect(%{sock: nil} = st), do: reconnect(st)

  defp maybe_reconnect(%{reconnect_at: at} = st) when is_integer(at) do
    if System.monotonic_time(:millisecond) >= at do
      ImapClient.send(st.sock, "z LOGOUT\r\n")
      ImapClient.close(st.sock)
      reconnect(%{st | sock: nil})
    else
      st
    end
  end

  defp maybe_reconnect(st), do: st

  defp reconnect(st) do
    t0 = now_us()

    case open(st) do
      {:ok, st} ->
        Stats.record("reconnect", now_us() - t0)
        arm_reconnect(st)

      {:error, reason} ->
        Stats.error("reconnect", reason)
        %{st | sock: nil}
    end
  end

  defp cycle(%{sock: nil} = st), do: st

  defp cycle(st) do
    st =
      Enum.reduce_while(st.cfg.cycle, st, fn step, st ->
        case step(step, st) do
          %{sock: nil} = st -> {:halt, st}
          st -> {:cont, st}
        end
      end)

    %{st | new: []}
  end

  defp step("noop", st), do: timed(st, "noop", "NOOP") |> elem(0)

  defp step("status", st),
    do: timed(st, "status", "STATUS INBOX (MESSAGES UIDNEXT UNSEEN)") |> elem(0)

  defp step("select", st), do: timed(st, "select", "SELECT INBOX") |> elem(0)
  defp step("search_unseen", st), do: timed(st, "search_unseen", "UID SEARCH UNSEEN") |> elem(0)
  defp step("fetch_flags", st), do: timed(st, "fetch_flags", "UID FETCH 1:* (UID FLAGS)") |> elem(0)

  defp step("search_new", st) do
    {st, lines} = timed(st, "search_new", "UID SEARCH UID #{st.last_uid + 1}:*")
    %{st | new: lines |> search_result() |> Enum.filter(&(&1 > st.last_uid))}
  end

  defp step("fetch_new", %{new: []} = st), do: st

  defp step("fetch_new", st) do
    {st, _} = timed(st, "fetch_new", "UID FETCH #{Enum.join(st.new, ",")} (UID FLAGS BODY.PEEK[])")
    %{st | last_uid: Enum.max([st.last_uid | st.new])}
  end

  defp step("store_seen", %{new: []} = st), do: st

  defp step("store_seen", st) do
    timed(st, "store_seen", "UID STORE #{Enum.join(st.new, ",")} +FLAGS.SILENT (\\Seen)") |> elem(0)
  end

  defp step(other, _st), do: raise("unknown cycle step #{inspect(other)}")

  # Run a command, record its latency; on a broken connection drop the socket so the
  # next tick reconnects.
  defp timed(st, name, line) do
    t0 = now_us()

    case run(st, name, line) do
      {:ok, st, lines} ->
        Stats.record(name, now_us() - t0)
        {st, lines}

      {status, st, lines} when status in [:no, :bad] ->
        Stats.error(name, {status, Enum.take(lines, 1)})
        {st, lines}

      {:error, reason} ->
        Stats.error(name, reason)
        ImapClient.close(st.sock)
        {%{st | sock: nil}, []}
    end
  end

  defp run(st, _name, line) do
    tag = "p#{st.tag}"
    st = %{st | tag: st.tag + 1}

    case ImapClient.exec(st.sock, tag, line) do
      {status, lines} when status in [:ok, :no, :bad] -> {status, st, lines}
      {:error, reason} -> {:error, reason}
    end
  end

  defp search_result(lines) do
    Enum.flat_map(lines, fn
      "* SEARCH" <> rest -> rest |> String.split() |> Enum.map(&String.to_integer/1)
      _ -> []
    end)
  end

  defp uidnext(lines) do
    Enum.find_value(lines, 1, fn line ->
      case Regex.run(~r/\[UIDNEXT (\d+)\]/, line) do
        [_, n] -> String.to_integer(n)
        nil -> nil
      end
    end)
  end

  defp now_us, do: System.monotonic_time(:microsecond)
  defp quote_string(s), do: ~s(") <> String.replace(s, ~s("), ~s(\\")) <> ~s(")
end
