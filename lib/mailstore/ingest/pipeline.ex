defmodule Mailstore.Ingest.Pipeline do
  @moduledoc """
  Consumer of the inbound topics: Kafka -> `Ingest.Record` -> `Delivery.deliver_many/1`.

  * Processors parse records and do the CPU work in parallel (`Delivery.plan/1`):
    recipient lookup, delivery rules, MIME structure, envelope, zstd. A record that
    cannot be parsed fails and goes to the dead-letter topic (`handle_failed/2`).
  * A batch (`batch_size` records or `batch_timeout` ms) is one group commit per shard
    (`Mailstore.Mailbox.deliver/1`).
  * Offsets are committed after the batch, periodically (BroadwayKafka). Writing is
    idempotent (delivery key, see `Ingest.Record`), so anything replayed after a crash
    or a rebalance is recognized and not stored twice; offsets can be committed late.
  * The database being down is not a reason to skip mail: the batch is retried with
    backoff until it goes through, and the pipeline stops pulling meanwhile.
    Any other error is isolated to its message: the batch is retried one message at a
    time and only the messages that still fail go to the dead-letter topic.

  Emits `[:mailstore, :ingest, :batch]` with counts of what happened to the batch, its
  duration and `lag_ms` (now minus the Kafka timestamp of its oldest record).
  """
  use Broadway
  require Logger

  alias Broadway.Message
  alias Mailstore.Delivery
  alias Mailstore.Ingest.{DeadLetter, Record}

  def start_link(config) do
    Broadway.start_link(__MODULE__,
      name: Keyword.get(config, :name, __MODULE__),
      producer: [
        module:
          Keyword.get_lazy(config, :producer, fn -> {BroadwayKafka.Producer, kafka(config)} end),
        # Each producer is a consumer group member with its share of the partitions.
        concurrency: Keyword.get(config, :producer_concurrency, 4)
      ],
      processors: [
        default: [concurrency: Keyword.get(config, :processors, System.schedulers_online())]
      ],
      batchers: [
        default: [
          concurrency: Keyword.get(config, :batchers, 2),
          batch_size: Keyword.get(config, :batch_size, 200),
          batch_timeout: Keyword.get(config, :batch_timeout, 200)
        ]
      ],
      context: %{dead_letter: Keyword.get(config, :dead_letter_topic)}
    )
  end

  defp kafka(config) do
    [
      hosts: Keyword.fetch!(config, :hosts),
      group_id: Keyword.fetch!(config, :group_id),
      topics: Keyword.fetch!(config, :topics),
      offset_reset_policy: Keyword.get(config, :offset_reset_policy, :latest),
      # Late commits: offsets go to Kafka every few seconds, not after every batch.
      # After a crash up to that much is replayed, and recognized as duplicates.
      offset_commit_on_ack: Keyword.get(config, :offset_commit_on_ack, false),
      group_config: [
        offset_commit_interval_seconds: Keyword.get(config, :offset_commit_interval_seconds, 5)
      ],
      # A producer fetches its partitions one after another, and on an empty one the
      # broker holds the request up to max_wait_time; then the producer sleeps
      # receive_interval before the next round. With the defaults (1 s, 2 s) and 32
      # partitions a round takes over half a minute; these keep it under 0.5 s.
      receive_interval: Keyword.get(config, :receive_interval, 50),
      fetch_config: [
        max_wait_time: Keyword.get(config, :fetch_max_wait_ms, 10),
        # A record carries the whole message and the body twice more, decoded. Kafka
        # returns a record bigger than this anyway, alone.
        max_bytes: Keyword.get(config, :fetch_max_bytes, 2 * 1024 * 1024)
      ]
    ]
  end

  @impl true
  def handle_message(_processor, message, _context) do
    case Record.parse(message.data) do
      # The original stays referenced for the dead-letter topic (no copy: same binary).
      {:ok, r} ->
        plan =
          Delivery.plan(%{
            rcpt: r.rcpt,
            raw: r.raw,
            class: r.class,
            internal_date: r.received_at,
            key: r.key
          })

        # BroadwayKafka batches per topic partition, to keep its order. Records are
        # independent (UIDs come from the database), so batch across partitions: at
        # 1 000 records/s over 16 partitions that is ~15x bigger batches. Acks stay
        # correct: offsets acked out of order are committed once contiguous.
        %{message | metadata: Map.put(message.metadata, :raw_value, message.data)}
        |> Message.put_data(plan)
        |> Message.put_batch_key(:ingest)

      {:error, reason} ->
        Message.failed(message, reason)
    end
  end

  @impl true
  def handle_batch(_batcher, messages, _info, _context) do
    started = System.monotonic_time()

    results =
      try do
        deliver(messages)
      rescue
        e -> one_by_one(messages, e)
      end

    messages =
      Enum.zip_with(messages, results, fn
        message, {:error, {:exception, reason}} -> Message.failed(message, reason)
        message, _result -> message
      end)

    # Lag: from the Kafka timestamp of the oldest record to its commit here (the worst
    # wait in the batch).
    oldest = messages |> Enum.map(&(&1.metadata[:ts] || 0)) |> Enum.min()

    measurements =
      results
      |> count()
      |> Map.put(:duration, System.monotonic_time() - started)
      |> Map.put(:lag_ms, if(oldest > 0, do: System.os_time(:millisecond) - oldest))

    :telemetry.execute([:mailstore, :ingest, :batch], measurements, %{})

    messages
  end

  @impl true
  def handle_failed(messages, %{dead_letter: topic}) do
    for message <- messages do
      reason = failure(message.status)
      meta = message.metadata

      Logger.warning(
        "ingest: dead letter #{meta[:topic]}/#{meta[:partition]}@#{meta[:offset]}: #{reason}"
      )

      if topic, do: DeadLetter.publish(topic, message, reason)
    end

    messages
  end

  # Group commit; waits out database outages (nothing is acked meanwhile).
  defp deliver(messages, backoff \\ 100) do
    messages |> Enum.map(& &1.data) |> Delivery.store()
  rescue
    e ->
      if transient?(e) do
        Logger.warning(
          "ingest: database unavailable, retrying in #{backoff} ms: #{Exception.message(e)}"
        )

        Process.sleep(backoff)
        deliver(messages, min(backoff * 2, 5_000))
      else
        reraise e, __STACKTRACE__
      end
  end

  # Something in the batch breaks the group commit: find it. Each message alone is
  # still a transaction of its own, and still idempotent.
  defp one_by_one(messages, error) do
    Logger.error("ingest: batch failed, retrying one by one: #{Exception.message(error)}")

    Enum.map(messages, fn message ->
      try do
        [result] = deliver([message])
        result
      rescue
        e -> {:error, {:exception, Exception.message(e)}}
      end
    end)
  end

  defp transient?(%DBConnection.ConnectionError{}), do: true

  defp transient?(%Postgrex.Error{postgres: %{code: code}}),
    do: code in Mailstore.Mailbox.Unavailable.transient_codes()

  defp transient?(%Postgrex.Error{}), do: true
  defp transient?(_), do: false

  defp count(results) do
    zero = %{messages: 0, stored: 0, duplicate: 0, discarded: 0, unknown: 0, failed: 0}

    Enum.reduce(results, zero, fn
      result, acc ->
        acc = Map.update!(acc, :messages, &(&1 + 1))

        case result do
          {:ok, targets} ->
            Enum.reduce(targets, acc, fn
              {:stored, _, _, _}, a -> Map.update!(a, :stored, &(&1 + 1))
              {:duplicate, _}, a -> Map.update!(a, :duplicate, &(&1 + 1))
              {:discarded, _}, a -> Map.update!(a, :discarded, &(&1 + 1))
              {:failed, _, _}, a -> Map.update!(a, :failed, &(&1 + 1))
              {:external, _}, a -> a
            end)

          {:error, :unknown_recipient} ->
            Map.update!(acc, :unknown, &(&1 + 1))

          {:error, _} ->
            Map.update!(acc, :failed, &(&1 + 1))
        end
    end)
  end

  defp failure({:failed, reason}) when is_binary(reason), do: reason
  defp failure(status), do: inspect(status)
end
