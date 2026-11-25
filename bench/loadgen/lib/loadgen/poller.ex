defmodule Loadgen.Poller do
  @moduledoc """
  Scenario `poller`: N persistent sessions, each running a cycle of commands every
  `interval` seconds (a sequencer polling its mailboxes), while the server receives
  new mail at `deliver_rate` messages/s so that searches find something to fetch.

  After the ramp and a warm-up, it measures a window of `duration` seconds:
    * client-side latency per command (p50/p95/p99) and commands/s;
    * server CPU (cores) from `:erlang.statistics(:runtime)` and CPU time per command;
    * with `--server-pid`, the same from `ps` for any server (this is what compares servers);
    * Postgres CPU from the container cgroup (`--pg-container`), loadgen's own CPU.
  """
  alias Loadgen.{Runner, Stats}

  @raw_body String.duplicate(
              "Thanks for reaching out. I looked at the proposal and have a few questions about pricing and the rollout timeline. ",
              14
            )

  def run(cfg) do
    Stats.init()
    cmds_per_cycle = Enum.join(cfg.cycle, ",")

    IO.puts(
      "poller scenario: #{cfg.sessions} sessions over #{cfg.users} users, cycle [#{cmds_per_cycle}] " <>
        "every #{div(cfg.interval_ms, 1000)}s, deliveries #{cfg.deliver_rate}/s, measure #{cfg.duration}s"
    )

    st =
      Runner.new(cfg)
      |> Runner.ramp(fn i ->
        Loadgen.PollerSession.start(self(), cfg, Runner.user(cfg, i, 1), Runner.source_ip(cfg, i))
      end)

    Runner.setup_summary(st, System.monotonic_time(:millisecond) - st.t0, "connect+LOGIN+SELECT")

    delivered = :counters.new(1, [:write_concurrency])
    deliverers = start_deliverers(cfg, delivered)
    warmup = min(cfg.interval_ms, 15_000)
    IO.puts("\nwarm-up #{div(warmup, 1000)}s (one polling interval, so cycles are spread)...")
    st = Runner.hold(st, warmup, &progress/1)

    Stats.reset()
    s0 = Runner.server_snapshot(cfg, [])
    os0 = Runner.os_snapshot(cfg)
    pg0 = pg_cpu_us(cfg)
    {l0, _} = :erlang.statistics(:runtime)
    d0 = :counters.get(delivered, 1)
    t0 = System.monotonic_time(:millisecond)

    st = Runner.hold(st, cfg.duration * 1000, &progress/1)

    t1 = System.monotonic_time(:millisecond)
    d1 = :counters.get(delivered, 1)
    {l1, _} = :erlang.statistics(:runtime)
    pg1 = pg_cpu_us(cfg)
    os1 = Runner.os_snapshot(cfg)
    s1 = Runner.server_snapshot(cfg, [])
    Enum.each(deliverers, &send(&1, :stop))

    report(st, (t1 - t0) / 1000, s0, s1, pg0, pg1, (l1 - l0) / (t1 - t0))
    os_report((t1 - t0) / 1000, os0, os1, pg0, pg1, d1 - d0)
    Runner.stop_all(st)
  end

  defp progress(st) do
    IO.puts(
      "[#{Runner.elapsed_s(st)}s] ready=#{st.ready} failed=#{st.failed} commands=#{Stats.total_count()}"
    )
  end

  # New mail for random users, through the server's delivery path (as the Kafka consumer
  # will), or over LMTP for a server that has no :erpc entry point. `delivered` counts
  # messages handed over (LMTP: stored, 250 received), so the report shows the real rate.
  # Stalwart allows 5 concurrent inbound SMTP/LMTP connections per IP by default.
  @lmtp_conns 4

  defp start_deliverers(%{deliver_rate: 0}, _delivered), do: []

  defp start_deliverers(%{deliver_lmtp: lmtp} = cfg, delivered) when lmtp != "" do
    [host, port] = String.split(lmtp, ":")
    # Each LMTP transaction waits for its 250, so several connections share the rate.
    per_tick = max(div(cfg.deliver_rate, 10 * @lmtp_conns), 1)

    for _ <- 1..@lmtp_conns do
      spawn_link(fn ->
        {:ok, sock} =
          :gen_tcp.connect(String.to_charlist(host), String.to_integer(port), [
            :binary,
            active: false,
            packet: :line
          ])

        "220" <> _ = lmtp_reply(sock)
        :ok = :gen_tcp.send(sock, "LHLO loadgen\r\n")
        "250" <> _ = lmtp_reply(sock)
        lmtp_loop(cfg, sock, per_tick, delivered)
      end)
    end
  end

  defp start_deliverers(%{server_node: nil}, _delivered), do: []

  defp start_deliverers(cfg, delivered) do
    [spawn_link(fn -> deliver_loop(cfg, max(div(cfg.deliver_rate, 10), 1), delivered) end)]
  end

  defp deliver_loop(cfg, per_tick, delivered) do
    receive do
      :stop -> :ok
    after
      100 ->
        for _ <- 1..per_tick do
          {user, raw} = new_mail(cfg)
          :erpc.cast(cfg.server_node, Mailstore.Delivery, :deliver, [raw, user, [class: :reply]])
        end

        :counters.add(delivered, 1, per_tick)
        deliver_loop(cfg, per_tick, delivered)
    end
  end

  defp lmtp_loop(cfg, sock, per_tick, delivered) do
    receive do
      :stop -> :gen_tcp.send(sock, "QUIT\r\n")
    after
      100 ->
        for _ <- 1..per_tick do
          {user, raw} = new_mail(cfg)

          :ok =
            :gen_tcp.send(
              sock,
              "MAIL FROM:<prospect@client.example>\r\nRCPT TO:<#{user}>\r\nDATA\r\n"
            )

          "250" <> _ = lmtp_reply(sock)
          "250" <> _ = lmtp_reply(sock)
          "354" <> _ = lmtp_reply(sock)
          :ok = :gen_tcp.send(sock, [raw, ".\r\n"])
          "250" <> _ = lmtp_reply(sock)
          :counters.add(delivered, 1, 1)
        end

        lmtp_loop(cfg, sock, per_tick, delivered)
    end
  end

  defp lmtp_reply(sock) do
    {:ok, line} = :gen_tcp.recv(sock, 0, 30_000)

    case line do
      <<_code::binary-size(3), "-", _::binary>> -> lmtp_reply(sock)
      _ -> line
    end
  end

  defp new_mail(cfg) do
    n = System.unique_integer([:positive])
    user = Runner.user(cfg, :rand.uniform(cfg.sessions) - 1, 1)

    raw =
      "From: Prospect <prospect#{rem(n, 997)}@client.example>\r\nTo: #{user}\r\n" <>
        "Subject: Re: quick question #{n}\r\nMessage-ID: <poll-#{n}-#{System.unique_integer([:positive])}@client>\r\n" <>
        "Date: Fri, 20 Nov 2026 12:00:00 +0000\r\n\r\n" <> @raw_body <> "\r\n"

    {user, raw}
  end

  # Server-agnostic: CPU of the server process from ps, per client command.
  defp os_report(_secs, nil, _os1, _pg0, _pg1, _delivered), do: :ok

  defp os_report(secs, os0, os1, pg0, pg1, delivered) do
    {rows, _} = Stats.summary()
    total = rows |> Enum.map(fn {_, r} -> r.count end) |> Enum.sum()
    cpu_ms = os1.cpu_ms - os0.cpu_ms
    cores = cpu_ms / max(os1.wall_ms - os0.wall_ms, 1)
    per_cmd = if total > 0, do: cpu_ms * 1000 / total, else: 0.0

    pg =
      if pg0 && pg1,
        do: ", postgres #{Float.round((pg1 - pg0) / (secs * 1_000_000), 2)} cores",
        else: ""

    IO.puts(
      "   server process (ps): #{Float.round(cores, 2)} cores, #{Float.round(per_cmd, 1)} us CPU " <>
        "per client command (deliveries included: #{delivered}, #{round(delivered / secs)}/s), " <>
        "RSS #{Float.round(os1.rss_kb / 1024, 1)} MB#{pg}"
    )
  end

  defp report(st, secs, s0, s1, pg0, pg1, loadgen_cores) do
    {rows, errors} = Stats.summary()
    total = rows |> Enum.map(fn {_, r} -> r.count end) |> Enum.sum()
    rate = total / secs
    ms = fn us -> :erlang.float_to_binary(us / 1000, decimals: 1) end

    IO.puts(
      "\n== measured #{Float.round(secs, 1)}s with #{st.ready} sessions: #{total} commands, #{round(rate)} commands/s"
    )

    IO.puts("   client-side latency (ms):")
    IO.puts("     command         count     /s    avg    p50    p95    p99")

    for {name, r} <- rows do
      IO.puts(
        "     " <>
          String.pad_trailing(name, 14) <>
          String.pad_leading(Integer.to_string(r.count), 7) <>
          String.pad_leading(Integer.to_string(round(r.count / secs)), 7) <>
          String.pad_leading(ms.(r.avg_us), 7) <>
          String.pad_leading(ms.(r.p50_us), 7) <>
          String.pad_leading(ms.(r.p95_us), 7) <> String.pad_leading(ms.(r.p99_us), 7)
      )
    end

    for {name, reason, n} <- errors, do: IO.puts("   errors x#{n} #{name}: #{reason}")

    if s0 && s1 do
      cores = (s1.cpu_ms - s0.cpu_ms) / (s1.wall_ms - s0.wall_ms)
      server_cmds = command_delta(s0, s1)
      cpu_per_cmd = if server_cmds > 0, do: (s1.cpu_ms - s0.cpu_ms) * 1000 / server_cmds, else: 0.0

      IO.puts("""
         server: #{Float.round(cores, 2)} cores (of #{s1.schedulers} schedulers), \
      #{server_cmds} commands seen by the server, \
      #{Float.round(cpu_per_cmd, 1)} us CPU per command (incl. deliveries and DB client work)
         server-side command time (avg us): #{server_side_avg(s0, s1)}\
      """)

      if pg0 && pg1 do
        IO.puts("   postgres: #{Float.round((pg1 - pg0) / (secs * 1_000_000), 2)} cores")
      end

      IO.puts("   loadgen itself: #{Float.round(loadgen_cores, 2)} cores (same machine)")

      per_conn = rate / max(st.ready, 1)

      IO.puts(
        "   per connection: #{Float.round(per_conn, 3)} commands/s; same profile on 97k connections " <>
          "(one Dovecot host today) = #{round(per_conn * 97_000)} commands/s ~ " <>
          "#{Float.round(cpu_per_cmd * per_conn * 97_000 / 1_000_000, 2)} BEAM cores"
      )
    end
  end

  defp command_delta(s0, s1) do
    total = fn s -> s.commands |> Map.values() |> Enum.map(& &1.count) |> Enum.sum() end
    total.(s1) - total.(s0)
  end

  defp server_side_avg(s0, s1) do
    by_cmd = fn s ->
      Enum.reduce(s.commands, %{}, fn {{cmd, _status}, r}, acc ->
        Map.update(acc, cmd, {r.count, r.count * r.avg_us}, fn {c, t} ->
          {c + r.count, t + r.count * r.avg_us}
        end)
      end)
    end

    before = by_cmd.(s0)

    by_cmd.(s1)
    |> Enum.map(fn {cmd, {c, t}} ->
      {c0, t0} = Map.get(before, cmd, {0, 0})
      {cmd, c - c0, t - t0}
    end)
    |> Enum.filter(fn {_, c, _} -> c > 0 end)
    |> Enum.sort()
    |> Enum.map_join(", ", fn {cmd, c, t} -> "#{cmd} #{div(t, c)}" end)
  end

  defp pg_cpu_us(%{pg_container: ""}), do: nil

  defp pg_cpu_us(%{pg_container: container}) do
    case System.cmd("docker", ["exec", container, "cat", "/sys/fs/cgroup/cpu.stat"],
           stderr_to_stdout: true
         ) do
      {out, 0} ->
        [_, us] = Regex.run(~r/usage_usec (\d+)/, out)
        String.to_integer(us)

      _ ->
        nil
    end
  end
end
