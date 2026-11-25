defmodule Loadgen.Idle do
  @moduledoc """
  Scenario `idle`: open N sessions at a fixed rate, each LOGIN + SELECT + IDLE, hold
  them, then disconnect. Sessions are spread over users like production: consecutive
  sessions take the next folder of the same user (INBOX, Junk), then the next user.

  With `server_node`, reads `Mailstore.Telemetry.snapshot/1` before the ramp, after
  it and after the hold, and prints server memory per session.
  """
  alias Loadgen.Runner

  def run(cfg) do
    baseline = Runner.server_snapshot(cfg)

    IO.puts(
      "idle scenario: #{cfg.sessions} sessions over #{cfg.users} users (#{Enum.join(cfg.folders, ",")}) " <>
        "at #{cfg.rate}/s to #{cfg.host}:#{cfg.port}, hold #{cfg.hold}s" <>
        if(baseline, do: ", server node #{cfg.server_node}", else: "")
    )

    st =
      Runner.new(cfg)
      |> Runner.ramp(fn i ->
        Loadgen.IdleSession.start(
          self(),
          cfg,
          Runner.user(cfg, i),
          Runner.folder(cfg, i),
          Runner.source_ip(cfg, i)
        )
      end)

    Runner.setup_summary(
      st,
      System.monotonic_time(:millisecond) - st.t0,
      "connect+LOGIN+SELECT+IDLE"
    )

    memory_summary(st, baseline, Runner.server_snapshot(cfg))

    st = Runner.hold(st, cfg.hold * 1000)
    IO.puts("\nhold done: pushes received=#{st.pushes} closed by server=#{st.closed}")
    after_hold(st, baseline, Runner.server_snapshot(cfg))
    Runner.stop_all(st)
  end

  defp memory_summary(st, baseline, after_ramp)
       when is_map(baseline) and is_map(after_ramp) and st.ready > 0 do
    d = fn key -> after_ramp.memory[key] - baseline.memory[key] end
    per = fn bytes -> Float.round(bytes / st.ready / 1024, 1) end

    IO.puts("""
       server: connections #{baseline.imap_connections} -> #{after_ramp.imap_connections}, \
    actors #{baseline.account_actors} -> #{after_ramp.account_actors}, \
    processes #{baseline.process_count} -> #{after_ramp.process_count}
       server memory delta per ready session (after GC):
         total     #{per.(d.(:total))} KB
         processes #{per.(d.(:processes))} KB   (connection processes alone: \
    #{per.(after_ramp.imap_connection_process_bytes)} KB)
         binary    #{per.(d.(:binary))} KB
         ets       #{per.(d.(:ets))} KB
         system    #{per.(d.(:system))} KB
       server total memory: #{Float.round(after_ramp.memory.total / 1_048_576, 1)} MB\
    """)
  end

  defp memory_summary(_st, _baseline, _after), do: :ok

  # After the hold, idle connections have hibernated: the steady-state figure.
  defp after_hold(st, baseline, final) when is_map(baseline) and is_map(final) and st.ready > 0 do
    per = fn key ->
      Float.round((final.memory[key] - baseline.memory[key]) / st.ready / 1024, 1)
    end

    IO.puts(
      "   steady state per session (after hold, hibernated): total #{per.(:total)} KB, " <>
        "processes #{per.(:processes)} KB, binary #{per.(:binary)} KB; " <>
        "connection processes alone #{Float.round(final.imap_connection_process_bytes / st.ready / 1024, 1)} KB; " <>
        "server total #{Float.round(final.memory.total / 1_048_576, 1)} MB"
    )
  end

  defp after_hold(_st, _baseline, _final), do: :ok
end
