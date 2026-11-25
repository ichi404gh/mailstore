defmodule Mailstore.Telemetry do
  @moduledoc """
  Observability: `:telemetry` events, per-command stats and a VM snapshot.

  Events:

    * `[:mailstore, :imap, :connection, :open]` - measurements `%{count: 1}`
    * `[:mailstore, :imap, :connection, :close]` - measurements `%{duration: native}`
    * `[:mailstore, :imap, :command, :start | :stop | :exception]` - a `:telemetry.span/3`
      around every command; metadata `%{command: "UID FETCH"}`, plus `status`
      (`:ok | :no | :bad | :idle`) on `:stop`

  `snapshot/1` is what a load test reads (locally or over `:erpc` from the load
  generator node). `Telemetry.Reporter` logs a compact line periodically when
  `MAILSTORE_REPORT_MS` is set.
  """
  use Supervisor

  alias Mailstore.Telemetry.{CommandStats, Reporter}

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @impl true
  def init(_arg) do
    reporter =
      case Application.get_env(:mailstore, :telemetry_report_interval) do
        nil -> []
        ms -> [{Reporter, ms}]
      end

    Supervisor.init([CommandStats | reporter], strategy: :one_for_one)
  end

  @doc """
  VM memory, CPU time, process and port counts, IMAP connections, account actors,
  command stats.

  Options:
    * `gc: true` - garbage-collect every process first, for a cleaner memory figure
    * `connections: true` - also sum the memory of IMAP connection processes
  """
  def snapshot(opts \\ []) do
    if opts[:gc], do: Enum.each(Process.list(), &:erlang.garbage_collect/1)
    mem = Map.new(:erlang.memory())

    {cpu_ms, _} = :erlang.statistics(:runtime)

    base = %{
      at: DateTime.utc_now(),
      # CPU time of all VM threads (can exceed wall time) and a wall clock to divide by
      cpu_ms: cpu_ms,
      wall_ms: System.monotonic_time(:millisecond),
      schedulers: :erlang.system_info(:schedulers_online),
      memory: Map.take(mem, [:total, :processes, :binary, :ets, :atom, :code, :system]),
      process_count: :erlang.system_info(:process_count),
      port_count: :erlang.system_info(:port_count),
      imap_connections: Mailstore.Imap.connection_count(),
      account_actors: Registry.count(Mailstore.Mailbox.Registry),
      commands: CommandStats.summary()
    }

    if opts[:connections] do
      pids = Mailstore.Imap.connection_pids()

      bytes =
        Enum.reduce(pids, 0, fn pid, acc ->
          case Process.info(pid, :memory) do
            {:memory, m} -> acc + m
            nil -> acc
          end
        end)

      Map.put(base, :imap_connection_process_bytes, bytes)
    else
      base
    end
  end
end
