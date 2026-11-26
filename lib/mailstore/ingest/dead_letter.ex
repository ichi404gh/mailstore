defmodule Mailstore.Ingest.DeadLetter do
  @moduledoc """
  Records the pipeline could not store, sent to a dead-letter topic as they came (same
  key and value) with the reason and origin in headers. Replaying them into the inbound
  topic later is safe: delivery is idempotent.
  """
  require Logger

  @client :mailstore_ingest_dead_letter

  def child_spec(hosts) do
    %{
      id: __MODULE__,
      start: {:brod, :start_link_client, [hosts, @client, [auto_start_producers: true]]}
    }
  end

  def publish(topic, message, reason) do
    meta = message.metadata

    value = %{
      value: original(message),
      headers: [
        {"x-error", reason},
        {"x-origin", "#{meta[:topic]}/#{meta[:partition]}@#{meta[:offset]}"}
      ]
    }

    case :brod.produce_sync(@client, topic, :hash, meta[:key] || "", value) do
      :ok -> :ok
      {:error, error} -> Logger.error("ingest: dead letter not written: #{inspect(error)}")
    end
  end

  # A failed processor leaves the original JSON in data; a failed batch has the record.
  defp original(%{data: data}) when is_binary(data), do: data
  defp original(%{metadata: %{raw_value: raw}}), do: raw
  defp original(%{data: data}), do: inspect(data)
end
