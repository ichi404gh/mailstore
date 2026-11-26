defmodule Mailstore.Core.NewMessage do
  @moduledoc """
  A message ready to be stored: the raw bytes plus everything derived from them
  once, at delivery time, so reads never re-parse the body.
  """
  alias Mailstore.Core.{BodyCodec, Flags, Headers, MessageClass, Mime}

  @enforce_keys [:raw]
  defstruct [
    :raw,
    :size,
    :internal_date,
    :flags_mask,
    :keywords,
    :class,
    :message_id,
    :message_id_h,
    :in_reply_to_h,
    :thread_id,
    :from_addr,
    :subject,
    :envelope,
    :bodystructure,
    # {codec, data} once compressed (`compress/1`); otherwise the writer compresses.
    :body
  ]

  @doc """
  Options: `:flags` (IMAP flag names), `:class` (see `Mailstore.Core.MessageClass`),
  `:internal_date` (defaults to now).
  """
  def from_raw(raw, opts \\ []) when is_binary(raw) do
    headers = Headers.parse(raw)
    {mask, keywords} = Flags.parse(Keyword.get(opts, :flags, []))
    message_id = Headers.first_msg_id(Headers.get(headers, "message-id"))
    in_reply_to = Headers.first_msg_id(Headers.get(headers, "in-reply-to"))
    references = Headers.msg_ids(Headers.get(headers, "references"))

    %__MODULE__{
      raw: raw,
      size: byte_size(raw),
      internal_date: usec(opts[:internal_date] || DateTime.utc_now()),
      flags_mask: mask,
      keywords: keywords,
      class: MessageClass.to_int(Keyword.get(opts, :class, :other)),
      message_id: message_id,
      message_id_h: hash64(message_id),
      in_reply_to_h: hash64(in_reply_to),
      # Root of the thread: first References entry, else the parent, else itself.
      thread_id: hash64(List.first(references) || in_reply_to || message_id),
      from_addr: Headers.address(Headers.get(headers, "from")),
      subject: Headers.get(headers, "subject"),
      envelope: Headers.envelope(headers),
      bodystructure: Mime.parse(raw)
    }
  end

  @doc """
  Compress the body now, so the writer does not do it inside its transaction (ingest
  calls this in parallel processors, before the group commit).
  """
  def compress(%__MODULE__{body: nil, raw: raw} = msg), do: %{msg | body: BodyCodec.encode(raw)}
  def compress(msg), do: msg

  @doc "64-bit hash for compact indexes on header values."
  def hash64(nil), do: nil

  def hash64(value) do
    <<h::signed-64, _::binary>> = :crypto.hash(:sha256, value)
    h
  end

  defp usec(%DateTime{microsecond: {us, _}} = dt), do: %{dt | microsecond: {us, 6}}
end
