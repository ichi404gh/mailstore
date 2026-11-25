defmodule Mailstore.Core.HeadersTest do
  use ExUnit.Case, async: true
  alias Mailstore.Core.{Headers, NewMessage}

  test "parses folded headers, ids and addresses" do
    raw =
      "From: \"Bob B\" <Bob@Example.ORG>\r\nSubject: long\r\n  folded subject\r\n" <>
        "Message-ID: <a@b>\r\nReferences: <root@x> <mid@x>\r\nIn-Reply-To: <mid@x>\r\n\r\nbody"

    headers = Headers.parse(raw)
    assert Headers.get(headers, "subject") == "long folded subject"
    assert Headers.address(Headers.get(headers, "from")) == "bob@example.org"

    msg = NewMessage.from_raw(raw)
    assert msg.message_id == "<a@b>"
    assert msg.thread_id == NewMessage.hash64("<root@x>")
    assert msg.in_reply_to_h == NewMessage.hash64("<mid@x>")
  end

  test "8-bit headers become valid UTF-8" do
    raw = "Subject: caf" <> <<0xE9>> <> "\r\n\r\nx"
    assert NewMessage.from_raw(raw).subject == "café"
  end
end
