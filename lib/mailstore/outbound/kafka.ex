defmodule Mailstore.Outbound.Kafka do
  @moduledoc """
  Outbound adapter: forwards as records of the relay sender's Kafka topic
  (`Kafka.Record`); the relay sender sends them through an SMTP relay. Options (in
  `config :mailstore, :outbound`): `hosts`, `topic`, `relay` (the relay sender's name
  for the SMTP relay, its `server_name`), `max_record_bytes` (the topic's
  max.message.bytes or less), `ack_timeout`.

  Every record of a call is sent at once with its own brod client, and the call waits
  for all acknowledgements (`acks=all`, the brod default): a batch costs about one round
  trip, not one per record. The relay sender's own dedup (message id, recipient,
  sender) can catch the rare duplicate `Outbound` lets through, since `message_id` is
  the message's Message-ID.

  Records are keyed by relay, as the topic's other producers key them, and partitions
  are chosen like the Java client and aiokafka choose them (murmur2 of the key), so one
  relay's records share a partition whoever wrote them; brod's own `:hash` partitioner
  would not.
  """
  @behaviour Mailstore.Outbound.Adapter

  import Bitwise

  @client :mailstore_outbound
  @m 0x5BD1E995
  @mask 0xFFFFFFFF

  alias Mailstore.Outbound.{Error, Kafka.Record}

  @impl true
  def children(config) do
    hosts = Keyword.fetch!(config, :hosts)
    [%{id: __MODULE__, start: {:brod, :start_link_client, [hosts, @client, client_config()]}}]
  end

  defp client_config, do: [auto_start_producers: true, default_producer_config: []]

  @impl true
  def deliver(config, forwards) do
    relay = Keyword.get(config, :relay) || raise ArgumentError, "outbound :relay is not set"
    max = Keyword.get(config, :max_record_bytes, 16 * 1024 * 1024)
    now = DateTime.utc_now()
    records = Enum.map(forwards, &{relay, &1 |> Record.build(relay, now) |> Jason.encode!()})

    case Enum.find(records, fn {_, value} -> byte_size(value) > max end) do
      nil ->
        case publish(config, records) do
          {:error, reason} when reason in [:message_too_large, :record_list_too_large] ->
            {:error,
             %Error{message: "rejected by the topic: #{reason}", reason: reason, transient: false}}

          result ->
            result
        end

      {_, value} ->
        {:error,
         %Error{
           message: "a forward is #{byte_size(value)} bytes as a record, over #{max}",
           reason: :message_too_large,
           transient: false
         }}
    end
  end

  @doc "Write `[{key, value}]` to the topic and wait for every acknowledgement."
  @spec publish(keyword, [{binary, binary}]) :: :ok | {:error, term}
  def publish(config, records) do
    topic = Keyword.fetch!(config, :topic)
    timeout = Keyword.get(config, :ack_timeout, 30_000)

    with {:ok, count} <- :brod.get_partitions_count(@client, topic),
         {:ok, refs} <- produce(topic, count, records) do
      Enum.reduce_while(refs, :ok, fn ref, :ok ->
        case :brod.sync_produce_request(ref, timeout) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  catch
    # The client is not running (yet) or a producer went down meanwhile.
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp produce(topic, count, records) do
    Enum.reduce_while(records, {:ok, []}, fn {key, value}, {:ok, refs} ->
      case :brod.produce(@client, topic, partition(key, count), key, value) do
        {:ok, ref} -> {:cont, {:ok, [ref | refs]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc "The partition the Kafka Java client picks for `key`: murmur2, made positive."
  def partition(key, count), do: band(murmur2(key), 0x7FFFFFFF) |> rem(count)

  @doc "Kafka's murmur2 (`org.apache.kafka.common.utils.Utils.murmur2`), unsigned."
  def murmur2(data) do
    h = bxor(0x9747B28C, byte_size(data))
    {h, tail} = blocks(data, h)

    h =
      case tail do
        <<a, b, c>> -> h |> bxor(c <<< 16) |> bxor(b <<< 8) |> bxor(a) |> mul()
        <<a, b>> -> h |> bxor(b <<< 8) |> bxor(a) |> mul()
        <<a>> -> h |> bxor(a) |> mul()
        <<>> -> h
      end

    h = h |> bxor(h >>> 13) |> mul()
    bxor(h, h >>> 15)
  end

  defp blocks(<<k::little-32, rest::binary>>, h) do
    k = mul(k)
    k = k |> bxor(k >>> 24) |> mul()
    blocks(rest, h |> mul() |> bxor(k))
  end

  defp blocks(tail, h), do: {h, tail}

  defp mul(x), do: band(x * @m, @mask)
end
