defmodule Mailstore.Directory.SharedDomainTest do
  @moduledoc """
  Master inboxes of many customers on one shared domain: each account carries its own
  tenant, mail of a customer's mailboxes is copied to its master inbox, and deleting a
  customer takes its master inbox but not the domain.
  """
  use Mailstore.DataCase, async: false
  alias Mailstore.{Delivery, Directory, Mailbox}
  alias Mailstore.Directory.Schemas.Domain

  setup do
    {:ok, shared} = Directory.create_shared_domain("Inbox.Example")
    %{shared: shared, acme: tenant_fixture("acme"), globex: tenant_fixture("globex")}
  end

  test "accounts of different tenants in one shared domain", %{
    shared: d,
    acme: acme,
    globex: globex
  } do
    assert d.tenant_id == nil and d.name == "inbox.example"

    assert {:error, :tenant_required} = Directory.create_account(d, "nobody")

    {:ok, a} = Directory.create_account(d, "acme-1", tenant: acme, kind: "master_inbox")
    {:ok, g} = Directory.create_account(d, "globex-1", tenant_id: globex.id, kind: "master_inbox")
    assert a.tenant_id == acme.id and g.tenant_id == globex.id
    assert {:ok, %{tenant_id: tid}} = Directory.lookup_account("ACME-1@inbox.example")
    assert tid == acme.id

    # Addresses stay unique across tenants.
    assert {:error, %Ecto.Changeset{}} = Directory.create_account(d, "acme-1", tenant: globex)

    # An owned domain takes no foreign tenant.
    {:ok, own} = Directory.create_domain(acme, "acme.test")
    assert {:error, :tenant_mismatch} = Directory.create_account(own, "x", tenant: globex)
    assert {:ok, %{tenant_id: t}} = Directory.create_account(own, "y", tenant: acme)
    assert t == acme.id
  end

  test "mail of a customer's mailboxes is copied to its master inbox", %{shared: d, acme: acme} do
    bob = account_fixture("bob@acme.test", tenant: acme)
    {:ok, master} = Directory.create_account(d, "acme-1", tenant: acme, kind: "master_inbox")
    {:ok, _} = Directory.create_forwarding(bob, master, %{only_classes: ["reply"]})

    {:ok, results} =
      Delivery.deliver(raw_message(subject: "Re: hello"), bob.address, class: :reply)

    assert [{:stored, bob_id, "INBOX", 1}, {:stored, master_id, "INBOX", 1}] = results
    assert bob_id == bob.id and master_id == master.id

    {:ok, _} = Delivery.deliver(raw_message(), bob.address, class: :warmup)
    assert {:ok, %{messages_count: 1}} = Mailbox.status(master.id, "INBOX")
  end

  test "deleting a tenant removes its accounts in the shared domain, not the domain",
       %{shared: d, acme: acme, globex: globex} do
    {:ok, _} = Directory.create_account(d, "acme-1", tenant: acme)
    {:ok, _} = Directory.create_account(d, "globex-1", tenant: globex)
    Repo.delete!(acme)
    # Hard deletes leave the cache on its full reload.
    send(Directory.Cache, :full_reload)
    Directory.Cache.refresh()

    assert Repo.get(Domain, d.id)
    assert {:error, :unknown_recipient} = Directory.lookup_account("acme-1@inbox.example")
    assert {:ok, _} = Directory.lookup_account("globex-1@inbox.example")
  end
end
