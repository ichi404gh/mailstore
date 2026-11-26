defmodule Mailstore.Mailbox.PartitionMaintainer do
  @moduledoc """
  Keeps future partitions created, drops `deliveries` days older than the dedup
  horizon (`:delivery_dedup_days`) and applies retention (`Mailbox.apply_retention/1`)
  unless `config :mailstore, :retention, schedule: false`. Every node runs it; per
  shard and policy only one expiry runs at a time (an advisory lock).
  """
  use GenServer
  require Logger

  @every :timer.hours(12)

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(nil) do
    send(self(), :ensure)
    {:ok, nil}
  end

  @impl true
  def handle_info(:ensure, state) do
    for repo <- Mailstore.Shards.all() do
      try do
        Mailstore.Mailbox.Partitions.ensure(repo)
        days = Application.get_env(:mailstore, :delivery_dedup_days, 8)
        cutoff = Date.add(Date.utc_today(), -days)
        Mailstore.Mailbox.Partitions.drop_deliveries_before(repo, cutoff)
      rescue
        e -> Logger.warning("partition maintenance skipped: #{Exception.message(e)}")
      end
    end

    if Keyword.get(Application.get_env(:mailstore, :retention, []), :schedule, true) do
      try do
        {:ok, %{weeks: weeks, folder_updates: n}} = Mailstore.Mailbox.apply_retention()

        if weeks != [],
          do: Logger.info("retention: dropped #{inspect(weeks)}, #{n} folder updates")
      rescue
        e -> Logger.warning("retention skipped: #{Exception.message(e)}")
      end
    end

    Process.send_after(self(), :ensure, @every)
    {:noreply, state}
  end
end
