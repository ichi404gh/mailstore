defmodule Mailstore.DeliveryTest do
  use Mailstore.DataCase, async: false
  alias Mailstore.{Delivery, Directory, Mailbox}

  setup do
    tenant = tenant_fixture()
    bob = account_fixture("bob@acme.test", tenant: tenant)
    master = account_fixture("master@acme.test", tenant: tenant, kind: "master_inbox")
    {:ok, _} = Directory.create_forwarding(bob, master, %{only_classes: ["reply"]})
    %{tenant: tenant, bob: bob, master: master}
  end

  test "replies reach the master inbox, warmup does not", %{bob: bob, master: master} do
    assert {:ok, [{:stored, b, "INBOX", 1}, {:stored, m, "INBOX", 1}]} =
             Delivery.deliver(raw_message(), "Bob@Acme.test", class: :reply)

    assert {b, m} == {bob.id, master.id}

    assert {:ok, [{:stored, _, "INBOX", 2}]} =
             Delivery.deliver(raw_message(), "bob@acme.test", class: :warmup)

    assert {:ok, %{messages_count: 1}} = Mailbox.status(master.id, "INBOX")
  end

  test "redirect mode does not keep the original; external targets are returned", %{tenant: t} do
    alice = account_fixture("alice@acme.test", tenant: t)
    {:ok, _} = Directory.create_forwarding(alice, "crm@partner.example", %{mode: "redirect"})

    assert {:ok, [{:external, "crm@partner.example"}]} =
             Delivery.deliver(raw_message(), "alice@acme.test")

    assert {:ok, %{messages_count: 0}} = Mailbox.status(alice.id, "INBOX")
  end

  test "unknown recipient" do
    assert {:error, :unknown_recipient} = Delivery.deliver(raw_message(), "nobody@acme.test")
  end

  test "a message is findable by Message-ID across the tenant", %{bob: bob} do
    {:ok, _} =
      Delivery.deliver(raw_message(message_id: "<find-me@x>"), "bob@acme.test", class: :reply)

    # original in bob's INBOX + copy in the master inbox
    assert [{_, 1}, {_, 1}] = Mailbox.find_by_message_id(bob.tenant_id, "<find-me@x>")
  end
end
