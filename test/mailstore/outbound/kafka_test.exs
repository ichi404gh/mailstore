defmodule Mailstore.Outbound.KafkaTest do
  @moduledoc """
  The relay sender's record and partitioning; the tests tagged `:kafka` need a broker
  and are excluded by default:

      bench/ingest/kafka.sh start
      mix test --only kafka
  """
  use ExUnit.Case, async: true
  alias Mailstore.Outbound
  alias Mailstore.Outbound.Kafka
  alias Mailstore.Outbound.Kafka.Record

  @topic "mailstore.outbound"

  defp forward(raw, extra \\ %{}) do
    Outbound.new(
      Map.merge(
        %{
          account_id: 1,
          tenant_id: 1,
          delivery_key: "k",
          rcpt: "bob@acme.test",
          target: "crm@partner.example",
          raw: raw,
          class: :other
        },
        extra
      )
    )
  end

  test "murmur2 matches Kafka's (vectors from its UtilsTest and aiokafka)" do
    # Signed in Java: -973932308, -790332482.
    assert Kafka.murmur2("21") == 3_321_034_988
    assert Kafka.murmur2("foobar") == 3_504_634_814
    assert Kafka.murmur2("abc") == 479_470_107
    assert Kafka.murmur2("") == 275_646_681
    assert Kafka.partition("foobar", 12) == 6
    assert Kafka.partition("relay-a", 12) == 0
  end

  test "the fields the relay sender needs; 8-bit bytes survive as Latin-1" do
    raw =
      "Received: from mx.example.org by us\r\nReceived: from origin\r\n" <>
        "From: Alice <Alice@Example.org>\r\nSubject: =?UTF-8?B?0J/RgNC40LLQtdGC?=\r\n" <>
        "Message-ID: <m1@example.org>\r\n\r\ncaf\xE9 \xD0\x9F\r\n"

    f = forward(raw, %{sender: "bounce@example.org", received_at: ~U[2026-11-27 10:00:00Z]})
    json = f |> Record.build("front-relay", ~U[2026-11-27 10:00:01Z]) |> Jason.encode!()
    r = Jason.decode!(json)

    assert %{
             "server_name" => "front-relay",
             "sender_email" => "bounce@example.org",
             "rcp_email" => "crm@partner.example",
             "all_recipients" => ["crm@partner.example"],
             "original_sender_email" => "alice@example.org",
             "message_id" => "<m1@example.org>",
             "subject" => "Привет",
             "message_received_header" => "from mx.example.org by us",
             "message_received_ts" => "2026-11-27T10:00:00Z",
             "is_sent" => false,
             "is_warmup" => false,
             "envelope_encoding" => "latin-1",
             "forwarded_for" => "bob@acme.test"
           } = r

    assert :unicode.characters_to_binary(r["envelope_content"], :utf8, :latin1) == raw
    assert r["forward_key"] == Base.encode16(f.key, case: :lower)
  end

  test "a record over the limit fails for good, before anything is written" do
    config = [relay: "r", topic: @topic, max_record_bytes: 100]
    f = forward("Subject: x\r\n\r\n" <> String.duplicate("a", 200))

    assert {:error, %Outbound.Error{transient: false, reason: :message_too_large}} =
             Kafka.deliver(config, [f])
  end

  describe "against a broker" do
    @describetag :kafka

    setup do
      [host, port] = String.split(System.get_env("MAILSTORE_TEST_KAFKA", "localhost:9092"), ":")
      hosts = [{String.to_atom(host), String.to_integer(port)}]
      config = [hosts: hosts, topic: @topic, relay: "relay-a"]
      for child <- Kafka.children(config), do: start_supervised!(child)
      %{config: config}
    end

    test "records land in the partition the Java client would pick", %{config: c} do
      {:ok, count} = :brod.get_partitions_count(:mailstore_outbound, @topic)
      id = System.unique_integer([:positive])
      records = for relay <- ["relay-a", "relay-b", "relay-a"], do: {relay, "#{relay} #{id}"}

      offsets =
        Map.new(0..(count - 1), fn p ->
          {:ok, offset} = :brod.resolve_offset(c[:hosts], @topic, p, :latest)
          {p, offset}
        end)

      assert :ok = Kafka.publish(c, records)

      for {relay, value} <- records do
        p = Kafka.partition(relay, count)
        {:ok, {_hw, msgs}} = :brod.fetch(c[:hosts], @topic, p, offsets[p])
        assert Enum.any?(msgs, &(elem(&1, 2) == relay and elem(&1, 3) == value))
      end
    end

    test "deliver writes relay sender records", %{config: c} do
      {:ok, count} = :brod.get_partitions_count(:mailstore_outbound, @topic)
      p = Kafka.partition("relay-a", count)
      {:ok, offset} = :brod.resolve_offset(c[:hosts], @topic, p, :latest)
      id = "<kafka-#{System.unique_integer([:positive])}@example.org>"

      assert :ok = Kafka.deliver(c, [forward("Message-ID: #{id}\r\n\r\nhi\r\n")])
      {:ok, {_hw, msgs}} = :brod.fetch(c[:hosts], @topic, p, offset)
      assert Enum.any?(msgs, &(Jason.decode!(elem(&1, 3))["message_id"] == id))
    end

    test "an unknown topic is an error, not a crash", %{config: c} do
      assert {:error, _} = Kafka.publish(Keyword.put(c, :topic, "no.such.topic"), [{"k", "v"}])
    end
  end
end
