defmodule Mailstore.Mailbox.PartitionMaintainer do
  @moduledoc """
  Keeps future partitions created and drops `deliveries` days older than the dedup
  horizon (`:delivery_dedup_days`). Message retention drops are a separate, explicit job.
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

    Process.send_after(self(), :ensure, @every)
    {:noreply, state}
  end
end
