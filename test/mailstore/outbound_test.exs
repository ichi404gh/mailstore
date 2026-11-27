defmodule Mailstore.OutboundTest do
  @moduledoc """
  Forwards to external targets, whatever the adapter: each is handed over once, before
  anything is stored. The relay sender's record is tested with `Outbound.Kafka`.
  """
  use Mailstore.DataCase, async: false
  alias Mailstore.{Delivery, Directory, Mailbox, Outbound}
  alias Mailstore.Outbound.TestAdapter

  setup do
    TestAdapter.listen()
    on_exit(fn -> Application.delete_env(:mailstore, :outbound_test) end)
    tenant = tenant_fixture()
    alice = account_fixture("alice@acme.test", tenant: tenant)
    %{tenant: tenant, alice: alice}
  end

  describe "forwards" do
    test "the key depends on the delivery and the target" do
      assert Outbound.key("k", "CRM@partner.example") == Outbound.key("k", "crm@partner.example")
      refute Outbound.key("k", "a@x.example") == Outbound.key("k", "b@x.example")
      refute Outbound.key("k", "a@x.example") == Outbound.key("j", "a@x.example")
      assert Outbound.key(nil, "a@x.example") == nil
    end

    test "a forward the adapter can never send fails for good; others are retried" do
      f =
        Outbound.new(%{
          account_id: 1,
          tenant_id: 1,
          rcpt: "a@acme.test",
          target: "x@y.example",
          raw: raw_message(),
          class: :other
        })

      TestAdapter.fail_next(1)
      assert %Outbound.Error{transient: true} = catch_error(Outbound.deliver([f]))

      too_big = %Outbound.Error{message: "too big", reason: :message_too_large, transient: false}
      TestAdapter.fail_next(1, too_big)
      assert catch_error(Outbound.deliver([f])) == too_big

      assert :ok = Outbound.deliver([f])
      assert_received {:outbound, [^f]}
    end
  end

  describe "delivery" do
    setup %{alice: alice} do
      {:ok, _} =
        Directory.create_forwarding(alice, "crm@partner.example", %{mode: "copy"})

      :ok
    end

    test "a copy is kept and the forward sent with the original sender", %{alice: alice} do
      raw = raw_message(message_id: "<fw1@example.org>")

      assert {:ok, [{:stored, _, "INBOX", 1}, {:external, "crm@partner.example"}]} =
               Delivery.deliver(raw, "alice@acme.test", key: "k1", sender: "alice@example.org")

      assert_received {:outbound, [f]}

      assert %{
               target: "crm@partner.example",
               sender: "alice@example.org",
               raw: ^raw,
               rcpt: "alice@acme.test"
             } = f

      assert f.account_id == alice.id and f.key == Outbound.key("k1", "crm@partner.example")
    end

    test "a replayed delivery is not forwarded again", %{alice: alice} do
      raw = raw_message()
      {:ok, _} = Delivery.deliver(raw, "alice@acme.test", key: "k2")
      assert_received {:outbound, [_]}

      assert {:ok, [{:duplicate, _}, {:external, "crm@partner.example"}]} =
               Delivery.deliver(raw, "alice@acme.test", key: "k2")

      refute_received {:outbound, _}
      assert {:ok, %{messages_count: 1}} = Mailbox.status(alice.id, "INBOX")

      # The same message twice in one batch: one forward.
      entry = %{raw: raw_message(), rcpt: "alice@acme.test", key: "k3"}
      [{:ok, _}, {:ok, _}] = Delivery.deliver_many([entry, entry])
      assert_received {:outbound, [_]}
    end

    test "nothing is stored while outbound is unavailable; the retry stores and forwards",
         %{alice: alice} do
      raw = raw_message()
      TestAdapter.fail_next(1)

      assert_raise Outbound.Error, fn -> Delivery.deliver(raw, "alice@acme.test", key: "k4") end
      assert {:ok, %{messages_count: 0}} = Mailbox.status(alice.id, "INBOX")

      assert {:ok, [{:stored, _, _, 1}, {:external, _}]} =
               Delivery.deliver(raw, "alice@acme.test", key: "k4")

      assert_received {:outbound, [_]}
    end

    test "redirect to an external target only: the key is still recorded", %{tenant: t} do
      carol = account_fixture("carol@acme.test", tenant: t)
      {:ok, _} = Directory.create_forwarding(carol, "me@home.example", %{mode: "redirect"})
      raw = raw_message()

      assert {:ok, [{:external, "me@home.example"}]} =
               Delivery.deliver(raw, "carol@acme.test", key: "k5")

      assert_received {:outbound, [_]}
      {:ok, [{:external, _}]} = Delivery.deliver(raw, "carol@acme.test", key: "k5")
      refute_received {:outbound, _}
    end

    test "with outbound off, forwards are dropped and nothing is recorded", %{alice: alice} do
      config = Application.get_env(:mailstore, :outbound)
      Application.put_env(:mailstore, :outbound, Keyword.put(config, :enabled, false))
      on_exit(fn -> Application.put_env(:mailstore, :outbound, config) end)

      raw = raw_message()
      {:ok, [_, {:external, _}]} = Delivery.deliver(raw, "alice@acme.test", key: "k6")
      refute_received {:outbound, _}

      Application.put_env(:mailstore, :outbound, config)

      {:ok, [{:duplicate, _}, {:external, _}]} =
        Delivery.deliver(raw, "alice@acme.test", key: "k6")

      assert_received {:outbound, [_]}
      assert {:ok, %{messages_count: 1}} = Mailbox.status(alice.id, "INBOX")
    end
  end
end
