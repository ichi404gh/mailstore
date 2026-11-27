defmodule Mailstore.Ingest.Record do
  @moduledoc """
  One message of the inbound topic, as the SMTP layer writes it: a JSON object, one
  per recipient (fan-out to recipients happens upstream).

  Fields:

    * `envelope_content` - the message as received (RFC 5322, with `Received:` headers),
      its bytes decoded as Latin-1 (one byte, one character: the SMTP layer writes
      `content.decode("latin-1")`), so 8-bit messages come back byte for byte. A string
      with characters above U+00FF cannot be that; it is taken as UTF-8 text;
    * `rcp_email` - the recipient;
    * `sender_email` - the envelope sender (MAIL FROM; empty for a bounce), the sender
      of forwards to external targets;
    * `message_received_ts` - when SMTP accepted it (ISO 8601), becomes INTERNALDATE;
    * `is_spamtest`, `is_warmup` - the class, as classified upstream.

  Any other field is ignored, so producers can carry their own bookkeeping.

  `key` identifies the delivery for idempotent ingest: SHA-256 of the recipient and the
  raw bytes, cut to 128 bits. The same record replayed (restart before the offset
  commit, DLQ replay, producer retry, the same message in two topics) has the same key.
  A message the sending MTA itself sent twice differs in its `Received:` headers and is
  stored twice, as any mail server would.
  """
  @enforce_keys [:rcpt, :raw, :class, :key]
  defstruct [:rcpt, :raw, :class, :key, :received_at, :sender]

  @type t :: %__MODULE__{
          rcpt: String.t(),
          raw: binary,
          class: atom,
          key: <<_::128>>,
          received_at: DateTime.t() | nil,
          sender: String.t() | nil
        }

  @spec parse(binary) :: {:ok, t} | {:error, String.t()}
  def parse(json) do
    with {:ok, %{} = doc} <- decode(json),
         {:ok, content} <- fetch_string(doc, "envelope_content"),
         {:ok, rcpt} <- fetch_string(doc, "rcp_email"),
         true <- String.contains?(rcpt, "@") || {:error, "rcp_email is not an address"} do
      rcpt = rcpt |> String.trim() |> String.downcase()
      raw = bytes(content)

      {:ok,
       %__MODULE__{
         rcpt: rcpt,
         raw: raw,
         class: class(doc),
         key: key(rcpt, raw),
         received_at: timestamp(doc["message_received_ts"]),
         sender: sender(doc["sender_email"])
       }}
    end
  end

  def key(rcpt, raw) do
    <<key::binary-16, _::binary>> = :crypto.hash(:sha256, [rcpt, 0, raw])
    key
  end

  defp bytes(content) do
    case :unicode.characters_to_binary(content, :utf8, :latin1) do
      raw when is_binary(raw) -> raw
      _ -> content
    end
  end

  defp sender(value) when is_binary(value), do: String.trim(value)
  defp sender(_), do: nil

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
