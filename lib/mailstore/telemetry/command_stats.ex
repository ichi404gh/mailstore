defmodule Mailstore.Telemetry.CommandStats do
  @moduledoc """
  Aggregates IMAP command spans into an ETS table: count, total time and a coarse
  latency histogram per {command, status}. Lock-free counters, cheap enough to leave on.
  """
  use GenServer

  @table __MODULE__
  @event [:mailstore, :imap, :command, :stop]
  # count, total_us, <1ms, <10ms, <100ms, >=100ms
  @buckets_ms [1, 10, 100]

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(nil) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      write_concurrency: true,
      read_concurrency: true
    ])

    :telemetry.attach({__MODULE__, :stop}, @event, &__MODULE__.handle_event/4, nil)
    {:ok, nil}
  end

  @impl true
  def terminate(_reason, _state), do: :telemetry.detach({__MODULE__, :stop})

  @doc false
  def handle_event(@event, %{duration: duration}, meta, _config) do
    us = System.convert_time_unit(duration, :native, :microsecond)
    bucket = 4 + Enum.count(@buckets_ms, &(us >= &1 * 1000))
    key = {meta.command, Map.get(meta, :status, :ok)}
    :ets.update_counter(@table, key, [{2, 1}, {3, us}, {bucket, 1}], {key, 0, 0, 0, 0, 0, 0})
  end

  @doc "%{{command, status} => %{count, avg_us, lt_1ms, lt_10ms, lt_100ms, ge_100ms}}"
  def summary do
    if :ets.whereis(@table) == :undefined do
      %{}
    else
      Map.new(:ets.tab2list(@table), fn {key, count, total, b1, b10, b100, slow} ->
        {key,
         %{
           count: count,
           avg_us: div(total, max(count, 1)),
           lt_1ms: b1,
           lt_10ms: b10,
           lt_100ms: b100,
           ge_100ms: slow
         }}
      end)
    end
  end

  def reset, do: :ets.delete_all_objects(@table)
end
