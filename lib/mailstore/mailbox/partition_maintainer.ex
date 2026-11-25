defmodule Mailstore.Mailbox.PartitionMaintainer do
  @moduledoc "Keeps future partitions created. Retention drops are a separate, explicit job."
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
      rescue
        e -> Logger.warning("partition maintenance skipped: #{Exception.message(e)}")
      end
    end

    Process.send_after(self(), :ensure, @every)
    {:noreply, state}
  end
end
