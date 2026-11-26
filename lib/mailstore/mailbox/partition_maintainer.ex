defmodule Mailstore.Mailbox.PartitionMaintainer do
  @moduledoc """
  Keeps future partitions created, drops `deliveries` days older than the dedup
  horizon (`:delivery_dedup_days`), applies retention (`Mailbox.apply_retention/1`)
  and moves due weeks to the cold tier (`Mailbox.Cold.run/1`, when a cold store is
  configured), unless `config :mailstore, :retention, schedule: false`. Every node
  runs it; per shard and policy only one expiry and one cold run at a time (advisory
  locks). A cold run of a big week takes long; this process waits for it.

  At start the partitions are made synchronously, before IMAP and ingest start (they
  come later in the supervision tree): after a long downtime the current week may have
  none, and a delivery into a missing partition fails. A shard whose partitions could
  not be made (the lock timeout of `Partitions.ensure/3`, or the database is down) is
  tried again in a minute, at start and later alike.
  """
  use GenServer
  require Logger

  @every :timer.hours(12)
  @retry :timer.minutes(1)

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(nil) do
    ensure_partitions()
    Process.send_after(self(), :ensure, @every)
    send(self(), :maintain)
    {:ok, nil}
  end

  @impl true
  def handle_info(:partitions, state) do
    ensure_partitions()
    {:noreply, state}
  end

  def handle_info(:ensure, state) do
    ensure_partitions()
    Process.send_after(self(), :ensure, @every)
    handle_info(:maintain, state)
  end

  def handle_info(:maintain, state) do
    for repo <- Mailstore.Shards.all() do
      try do
        days = Application.get_env(:mailstore, :delivery_dedup_days, 8)
        cutoff = Date.add(Date.utc_today(), -days)
        Mailstore.Mailbox.Partitions.drop_deliveries_before(repo, cutoff)
      rescue
        e -> Logger.warning("dropping old deliveries skipped: #{Exception.message(e)}")
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

      try do
        {:ok, cold} = Mailstore.Mailbox.Cold.run()
        if cold.packs > 0, do: Logger.info("cold tier: #{inspect(cold)}")
      rescue
        e -> Logger.warning("cold tier run failed: #{Exception.message(e)}")
      end
    end

    {:noreply, state}
  end

  # Every shard; one that fails is tried again in a minute, the others are not held up.
  defp ensure_partitions do
    failed =
      Enum.reject(Mailstore.Shards.all(), fn repo ->
        try do
          Mailstore.Mailbox.Partitions.ensure(repo)
        rescue
          e ->
            Logger.warning(
              "partitions of #{inspect(repo)} not made, retrying in a minute: #{Exception.message(e)}"
            )

            false
        end
      end)

    if failed != [], do: Process.send_after(self(), :partitions, @retry)
    :ok
  end
end
