defmodule Mailstore.Outbound.Kafka.Record do
  @moduledoc """
  A forward as a record of the relay sender's topic: a JSON object it reads into its
  message model. It sends `envelope_content` from `sender_email` to `rcp_email`
  through the relay named `server_name`; the other required fields only feed its
  result topic (logs, stats), so they are filled plainly:

    * `message_id` - the Message-ID of the message (part of the relay sender's dedup
      key), else one made from the forward key;
    * `original_sender_email` - the `From` address; `subject` - decoded;
    * `message_received_header` - the topmost `Received:`;
    * `message_headers`, `message_new_headers` - `"{}"`, `email_body` - `""`: the
      body is not copied a second time;
    * `is_warmup`, `is_spamtest` - from the class; flags of outbound sending
      (`is_followup`, `gcdt_applied`, `is_email_body_tweaks_applied`) are false.

  Bytes: a JSON string holds text, so `envelope_content` carries the raw bytes decoded
  as Latin-1 (one byte, one character), the same convention as the records the SMTP
  layer writes. `envelope_encoding: "latin-1"` says so: the relay sender must encode
  the string back with it, or 8-bit messages will not go out byte for byte.

  `forward_key` (hex) and `forwarded_for` (the original recipient) are ours, for
  tracing; the relay sender ignores fields it does not know.
  """
  alias Mailstore.Core.{Headers, MimeText}

  @spec build(Mailstore.Outbound.forward(), String.t(), DateTime.t()) :: map
  def build(f, relay, now) do
    headers = Headers.parse(f.raw)
    header = &Headers.get(headers, &1)
    key = f.key && Base.encode16(f.key, case: :lower)

    %{
      message_id:
        Headers.first_msg_id(header.("message-id")) ||
          "<#{key || Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)}@forward.mailstore>",
      mailpit_id: 0,
      message_received_ts: DateTime.to_iso8601(f[:received_at] || now),
      sender_email: f[:sender] || "",
      original_sender_email: Headers.address(header.("from")) || "",
      rcp_email: f.target,
      all_recipients: [f.target],
      server_name: relay,
      is_sent: false,
      ts: DateTime.to_iso8601(now),
      subject: MimeText.decode_header(header.("subject") || ""),
      message_received_header: header.("received") || "",
      email_html: nil,
      message_headers: "{}",
      message_new_headers: "{}",
      email_body: "",
      is_warmup: f.class == :warmup,
      is_followup: false,
      is_spamtest: f.class == :spam_test,
      gcdt_applied: false,
      is_email_body_tweaks_applied: false,
      envelope_content: :unicode.characters_to_binary(f.raw, :latin1, :utf8),
      envelope_encoding: "latin-1",
      user_id: nil,
      forward_key: key,
      forwarded_for: f.rcpt
    }
  end
end
