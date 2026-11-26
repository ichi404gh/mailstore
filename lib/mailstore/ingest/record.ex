defmodule Mailstore.Ingest.Record do
  @moduledoc """
  One message of the inbound topics (`mailhub_relay_sender_in.basic` / `.priority`), as
  the SMTP layer writes it: JSON, one message per recipient.

  Used fields:

    * `envelope_content` - the message as received (RFC 5322, with `Received:` headers);
    * `rcp_email` - the recipient (`all_recipients` is informational: fan-out to
      recipients already happened upstream);
    * `message_received_ts` - when SMTP accepted it, becomes INTERNALDATE;
    * `is_spamtest`, `is_warmup` - the class.

  Everything else (decoded duplicates of the body in `email_body` / `email_html`,
  sender rotation, relay bookkeeping) is ignored.

  `key` identifies the delivery for idempotent ingest: SHA-256 of the recipient and the
  raw bytes, cut to 128 bits. The same record replayed (restart before the offset
  commit, DLQ replay, producer retry, the same message in both topics) has the same key.
  A message the sending MTA itself sent twice differs in its `Received:` headers and is
  stored twice, as any mail server would.
  """
  @enforce_keys [:rcpt, :raw, :class, :key]
  defstruct [:rcpt, :raw, :class, :key, :received_at]

  @type t :: %__MODULE__{
          rcpt: String.t(),
          raw: binary,
          class: atom,
          key: <<_::128>>,
          received_at: DateTime.t() | nil
        }

  @spec parse(binary) :: {:ok, t} | {:error, String.t()}
  def parse(json) do
    with {:ok, %{} = doc} <- decode(json),
         {:ok, raw} <- fetch_string(doc, "envelope_content"),
         {:ok, rcpt} <- fetch_string(doc, "rcp_email"),
         true <- String.contains?(rcpt, "@") || {:error, "rcp_email is not an address"} do
      rcpt = rcpt |> String.trim() |> String.downcase()

      {:ok,
       %__MODULE__{
         rcpt: rcpt,
         raw: raw,
         class: class(doc),
         key: key(rcpt, raw),
         received_at: timestamp(doc["message_received_ts"])
       }}
    end
  end

  def key(rcpt, raw) do
    <<key::binary-16, _::binary>> = :crypto.hash(:sha256, [rcpt, 0, raw])
    key
  end

  defp decode(json) do
    case Jason.decode(json) do
      {:ok, %{} = doc} -> {:ok, doc}
      {:ok, _} -> {:error, "not a JSON object"}
      {:error, e} -> {:error, "invalid JSON: " <> Exception.message(e)}
    end
  end

  defp fetch_string(doc, field) do
    case doc[field] do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "#{field} missing"}
    end
  end

  # Reply detection (`reply`, for the master inbox) needs an agreed signal upstream.
  defp class(%{"is_spamtest" => true}), do: :spam_test
  defp class(%{"is_warmup" => true}), do: :warmup
  defp class(_), do: :other

  defp timestamp(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp timestamp(_), do: nil
end
