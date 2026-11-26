defmodule Mailstore.Ingest.RecordTest do
  use ExUnit.Case, async: true
  alias Mailstore.Ingest.Record

  @raw "From: a@example.org\r\nTo: Bob@Acme.test\r\nSubject: hi\r\n\r\nbody\r\n"

  # Same shape as mailhub_relay_sender_in.* (synthetic values).
  def record(overrides \\ %{}) do
    %{
      "message_id" => "<m1@example.org>",
      "message_received_ts" => "2026-11-26T10:02:51+00:00",
      "sender_email" => "a@example.org",
      "rcp_email" => "Bob@Acme.test",
      "all_recipients" => ["Bob@Acme.test"],
      "server_name" => "mailhub://1025",
      "subject" => "hi",
      "email_body" => "body",
      "email_html" => "",
      "is_warmup" => false,
      "is_followup" => false,
      "is_spamtest" => false,
      "envelope_content" => @raw,
      "wrmp_srv_name" => nil
    }
    |> Map.merge(overrides)
    |> Jason.encode!()
  end

  test "parses the fields delivery needs" do
    assert {:ok, r} = Record.parse(record())
    assert r.rcpt == "bob@acme.test"
    assert r.raw == @raw
    assert r.class == :other
    assert r.received_at == ~U[2026-11-26 10:02:51Z]
    assert byte_size(r.key) == 16
  end

  test "class from the flags" do
    assert {:ok, %{class: :warmup}} = Record.parse(record(%{"is_warmup" => true}))

    assert {:ok, %{class: :spam_test}} =
             Record.parse(record(%{"is_warmup" => true, "is_spamtest" => true}))
  end

  test "the key depends on recipient and bytes only" do
    {:ok, a} = Record.parse(record())
    {:ok, b} = Record.parse(record(%{"message_received_ts" => nil, "subject" => "other"}))
    {:ok, c} = Record.parse(record(%{"rcp_email" => "carol@acme.test"}))
    {:ok, d} = Record.parse(record(%{"envelope_content" => @raw <> "x"}))
    assert a.key == b.key
    assert b.received_at == nil
    refute a.key in [c.key, d.key]
  end

  test "broken records" do
    assert {:error, "invalid JSON" <> _} = Record.parse("{")
    assert {:error, "not a JSON object"} = Record.parse("[]")

    assert {:error, "envelope_content missing"} =
             Record.parse(record(%{"envelope_content" => ""}))

    assert {:error, "rcp_email missing"} = Record.parse(record(%{"rcp_email" => nil}))
    assert {:error, "rcp_email is not an address"} = Record.parse(record(%{"rcp_email" => "x"}))
  end
end
