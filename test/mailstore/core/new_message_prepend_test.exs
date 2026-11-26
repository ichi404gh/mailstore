defmodule Mailstore.Core.NewMessagePrependTest do
  use ExUnit.Case, async: true
  alias Mailstore.Core.{Mime, NewMessage}

  # A rule adds a header on top; the message is derived without a second parse, so the
  # shifted tree must equal a fresh parse, or BODY[n] would slice the wrong bytes.
  test "prepend shifts every offset exactly like a fresh parse" do
    raw =
      "From: a@x\r\nSubject: s\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"o\"\r\n\r\n" <>
        "--o\r\nContent-Type: multipart/alternative; boundary=\"i\"\r\n\r\n" <>
        "--i\r\nContent-Type: text/plain\r\n\r\nplain\r\n--i\r\nContent-Type: text/html\r\n\r\n<p>x</p>\r\n--i--\r\n" <>
        "--o\r\nContent-Type: message/rfc822\r\n\r\nSubject: inner\r\n\r\ninner body\r\n" <>
        "--o--\r\n"

    msg = NewMessage.from_raw(raw)
    prefix = "X-Warmup-Vendor: VendorA\r\nX-Two: 2\r\n"
    derived = NewMessage.prepend(msg, prefix, :warmup)

    assert derived.raw == prefix <> raw
    assert derived.size == byte_size(prefix <> raw)
    assert derived.bodystructure == Mime.parse(prefix <> raw)
    assert derived.class == 1
    assert Mime.slice(derived.raw, Mime.part(derived.bodystructure, [1, 2])["b"]) == "<p>x</p>"
  end
end
