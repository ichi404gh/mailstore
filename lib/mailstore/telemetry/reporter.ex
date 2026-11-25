defmodule Mailstore.Telemetry.Reporter do
  @moduledoc "Logs one line with connections, actors, memory and command rate every interval."
  use GenServer
  require Logger

  def start_link(interval), do: GenServer.start_link(__MODULE__, interval, name: __MODULE__)

  @impl true
  def init(interval) do
    Process.send_after(self(), :report, interval)
    {:ok, %{interval: interval, last_commands: 0}}
  end

  @impl true
  def handle_info(:report, st) do
    s = Mailstore.Telemetry.snapshot()
    commands = s.commands |> Map.values() |> Enum.map(& &1.count) |> Enum.sum()
    rate = (commands - st.last_commands) * 1000 / st.interval
    mb = fn bytes -> Float.round(bytes / 1_048_576, 1) end

    Logger.info(
      "conns=#{s.imap_connections} actors=#{s.account_actors} procs=#{s.process_count} " <>
        "mem_total=#{mb.(s.memory.total)}MB processes=#{mb.(s.memory.processes)}MB " <>
        "binary=#{mb.(s.memory.binary)}MB cmds/s=#{Float.round(rate, 1)}"
    )

    Process.send_after(self(), :report, st.interval)
    {:noreply, %{st | last_commands: commands}}
  end
end
