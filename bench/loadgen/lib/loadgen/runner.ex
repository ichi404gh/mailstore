defmodule Loadgen.Runner do
  @moduledoc """
  Shared machinery for scenarios: ramping sessions up at a fixed rate, collecting their
  `{:ready | :failed | :push | :closed, ...}` messages, progress lines, setup latency
  summary and server snapshots: over `:erpc` (mailstore) or from `ps` (any server).
  """

  def new(cfg) do
    %{
      cfg: cfg,
      started: 0,
      ready: 0,
      failed: 0,
      closed: 0,
      pushes: 0,
      failures: %{},
      setup_us: [],
      pids: [],
      t0: System.monotonic_time(:millisecond)
    }
  end

  @doc "Account for session `i`: consecutive sessions share a user, one per folder."
  def user(cfg, i, per_user \\ nil) do
    per_user = per_user || length(cfg.folders)
    String.replace(cfg.user_format, "%d", Integer.to_string(rem(div(i, per_user), cfg.users) + 1))
  end

  def folder(cfg, i), do: Enum.at(cfg.folders, rem(i, length(cfg.folders)))

  def source_ip(%{source_ips: []}, _i), do: nil
  def source_ip(%{source_ips: ips}, i), do: Enum.at(ips, rem(i, length(ips)))

  @doc "Start sessions in 100 ms batches until all are started and have reported."
  def ramp(st, spawn_fn, progress \\ &progress/1) do
    batch = max(div(st.cfg.rate, 10), 1)
    next_report = System.monotonic_time(:millisecond) + st.cfg.report * 1000
    ramp_loop(st, spawn_fn, batch, next_report, progress)
  end

  defp ramp_loop(%{cfg: cfg} = st, spawn_fn, batch, next_report, progress) do
    st =
      if st.started < cfg.sessions do
        n = min(batch, cfg.sessions - st.started)
        pids = for i <- st.started..(st.started + n - 1)//1, do: spawn_fn.(i)
        %{st | started: st.started + n, pids: pids ++ st.pids}
      else
        st
      end

    st = drain(st, 100)
    now = System.monotonic_time(:millisecond)

    next_report =
      if now >= next_report do
        progress.(st)
        now + cfg.report * 1000
      else
        next_report
      end

    if st.ready + st.failed >= cfg.sessions,
      do: st,
      else: ramp_loop(st, spawn_fn, batch, next_report, progress)
  end

  @doc "Keep collecting messages for `ms`, printing progress every report interval."
  def hold(st, ms, progress \\ &progress/1)
  def hold(st, ms, _progress) when ms <= 0, do: st

  def hold(st, ms, progress) do
    step = min(ms, st.cfg.report * 1000)
    st = drain(st, step)
    progress.(st)
    hold(st, ms - step, progress)
  end

  def stop_all(st) do
    Enum.each(st.pids, &send(&1, :stop))
    IO.puts("sent stop to #{length(st.pids)} sessions")
    Process.sleep(1_000)
  end

  defp drain(st, ms), do: drain_until(st, System.monotonic_time(:millisecond) + ms)

  defp drain_until(st, deadline) do
    wait = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:ready, _pid, us} ->
        drain_until(%{st | ready: st.ready + 1, setup_us: [us | st.setup_us]}, deadline)

      {:failed, _pid, reason} ->
        drain_until(fail(st, reason), deadline)

      {:push, _pid} ->
        drain_until(%{st | pushes: st.pushes + 1}, deadline)

      {:closed, _pid} ->
        drain_until(%{st | closed: st.closed + 1}, deadline)
    after
      wait -> st
    end
  end

  defp fail(st, reason) do
    key = reason |> inspect() |> String.slice(0, 80)
    %{st | failed: st.failed + 1, failures: Map.update(st.failures, key, 1, &(&1 + 1))}
  end

  def elapsed_s(st), do: Float.round((System.monotonic_time(:millisecond) - st.t0) / 1000, 1)

  def progress(st) do
    mem = Float.round(:erlang.memory(:total) / 1_048_576, 1)

    IO.puts(
      "[#{elapsed_s(st)}s] started=#{st.started} ready=#{st.ready} failed=#{st.failed} " <>
        "pushes=#{st.pushes} closed=#{st.closed} loadgen_mem=#{mem}MB"
    )
  end

  def setup_summary(st, ramp_ms, label) do
    IO.puts("\n== ramp finished in #{ramp_ms} ms: ready=#{st.ready} failed=#{st.failed}")

    for {reason, n} <- st.failures |> Enum.sort_by(&(-elem(&1, 1))) |> Enum.take(5),
        do: IO.puts("   failure x#{n}: #{reason}")

    if st.setup_us != [] do
      sorted = Enum.sort(st.setup_us)
      n = length(sorted)
      pct = fn p -> div(Enum.at(sorted, min(round(p * n), n - 1)), 1000) end

      IO.puts(
        "   setup (#{label}) ms: p50=#{pct.(0.5)} p95=#{pct.(0.95)} p99=#{pct.(0.99)} max=#{pct.(1.0)}"
      )
    end
  end

  def server_snapshot(cfg, opts \\ [gc: true, connections: true])
  def server_snapshot(%{server_node: nil}, _opts), do: nil

  def server_snapshot(%{server_node: node}, opts) do
    :erpc.call(node, Mailstore.Telemetry, :snapshot, [opts], 120_000)
  end

  @doc """
  What the OS sees of the server process: `%{rss_kb, cpu_ms, wall_ms}`, or nil without
  `--server-pid`. The same measurement for every server, unlike `server_snapshot/2`.
  """
  def os_snapshot(%{server_pid: nil}), do: nil

  def os_snapshot(%{server_pid: pid}) do
    {out, 0} = System.cmd("ps", ["-o", "rss=,cputime=", "-p", Integer.to_string(pid)])
    [rss, cpu] = String.split(out)

    %{
      rss_kb: String.to_integer(rss),
      cpu_ms: cputime_ms(cpu),
      wall_ms: System.monotonic_time(:millisecond)
    }
  end

  # ps cputime: [[dd-]hh:]mm:ss.cc
  defp cputime_ms(s) do
    {days, rest} =
      case String.split(s, "-") do
        [d, rest] -> {String.to_integer(d), rest}
        [rest] -> {0, rest}
      end

    secs =
      rest
      |> String.split(":")
      |> Enum.reduce(0.0, fn part, acc -> acc * 60 + parse_num(part) end)

    round((days * 86_400 + secs) * 1000)
  end

  defp parse_num(part) do
    case Float.parse(part) do
      {f, ""} -> f
      _ -> 0.0
    end
  end

  def os_summary(_label, nil, _later, _sessions), do: :ok

  def os_summary(label, base, later, sessions) do
    per = if sessions > 0, do: Float.round((later.rss_kb - base.rss_kb) / sessions, 1), else: 0.0
    cores = (later.cpu_ms - base.cpu_ms) / max(later.wall_ms - base.wall_ms, 1)

    IO.puts(
      "   server process (ps) #{label}: RSS #{Float.round(base.rss_kb / 1024, 1)} -> " <>
        "#{Float.round(later.rss_kb / 1024, 1)} MB, #{per} KB per session; " <>
        "CPU #{later.cpu_ms - base.cpu_ms} ms (#{Float.round(cores, 2)} cores)"
    )
  end
end
