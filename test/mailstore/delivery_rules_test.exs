defmodule Mailstore.DeliveryRulesTest do
  use Mailstore.DataCase, async: false
  alias Mailstore.{Delivery, Directory, Mailbox}
  alias Mailstore.Directory.Cache

  setup do
    %{"rules" => rules} =
      "priv/delivery_rules/example.json" |> File.read!() |> Jason.decode!()

    {:ok, 6} = Directory.import_global_rules(rules)
    tenant = tenant_fixture()
    bob = account_fixture("bob@acme.test", tenant: tenant)
    master = account_fixture("master@acme.test", tenant: tenant, kind: "master_inbox")
    {:ok, _} = Directory.create_forwarding(bob, master)
    %{tenant: tenant, bob: bob, master: master}
  end

  defp stored(account) do
    fid = folder_id(account, "INBOX")
    uids = Mailbox.uids(account.tenant_id, fid)
    for uid <- uids, do: elem(Mailbox.fetch_raw(account.tenant_id, fid, uid), 1)
  end

  defp class_of(account) do
    Repo.query!("SELECT class FROM messages WHERE folder_id = $1 ORDER BY uid", [
      folder_id(account, "INBOX")
    ]).rows
    |> List.flatten()
  end

  test "ordinary mail: original kept as is, the master inbox copy carries the forward header",
       %{bob: bob, master: master} do
    raw = raw_message(subject: "Re: proposal")

    assert {:ok, [{:stored, _, _, 1}, {:stored, _, _, 1}]} =
             Delivery.deliver(raw, "bob@acme.test", class: :reply)

    assert [^raw] = stored(bob)
    assert ["X-Forwarded-From: bob@acme.test\r\n" <> ^raw] = stored(master)
  end

  test "a capture from the subject is matched in the body", %{bob: bob} do
    raw = raw_message(subject: "Quick note | AB12CD", body: "Hi\r\nAB12CD\r\n")
    assert {:ok, [{:stored, _, _, 1}]} = Delivery.deliver(raw, "bob@acme.test")
    assert ["X-Warmup-Vendor: VendorB\r\n" <> _] = stored(bob)
  end

  test "a warmup vendor is tagged, classed warmup and not forwarded", %{bob: bob, master: master} do
    raw = raw_message(message_id: "<vendor-a-123@sender.example>")
    assert {:ok, [{:stored, _, _, 1}]} = Delivery.deliver(raw, "bob@acme.test")
    assert ["X-Warmup-Vendor: VendorA\r\n" <> ^raw] = stored(bob)
    assert class_of(bob) == [1]
    assert stored(master) == []
  end

  test "discard rules store nothing", %{bob: bob} do
    raw = raw_message(body: "hello example-drop-marker\r\n")
    assert {:ok, [{:discarded, id}]} = Delivery.deliver(raw, "bob@acme.test")
    assert id == bob.id
    assert stored(bob) == []
  end

  test "a bounce goes to the bounce mailbox when it is ours, else it is external", %{bob: bob} do
    raw = raw_message(body: "Delivery failed: 550 5.1.1 example-bounce\r\n")
    assert {:ok, [{:external, "bounces@example.test"}]} = Delivery.deliver(raw, "bob@acme.test")

    bounces = account_fixture("bounces@example.test")
    assert {:ok, [{:stored, id, "INBOX", 1}]} = Delivery.deliver(raw, "bob@acme.test")
    assert id == bounces.id
    assert stored(bob) == []
  end

  test "a mailbox filter (skip sequence) keeps matching mail out of forwarding",
       %{bob: bob, master: master} do
    {:ok, _} =
      Directory.create_rule(%{
        scope: "account",
        account_id: bob.id,
        stage: "forward",
        position: 2000,
        name: "skip_sequence",
        spec: %{
          "test" => %{
            "any" => [
              %{"header" => "Subject", "contains_any" => ["invoice"]},
              %{"body" => %{"contains_any" => ["invoice"]}}
            ]
          },
          "actions" => [%{"add_header" => ["X-Warmup-Vendor", "UserDefined"]}, "stop"]
        }
      })

    assert {:ok, [{:stored, _, _, 1}]} =
             Delivery.deliver(raw_message(subject: "Your Invoice #42"), "bob@acme.test")

    assert ["X-Warmup-Vendor: UserDefined\r\n" <> _] = stored(bob)
    assert stored(master) == []

    # Other mailboxes of the tenant are not affected.
    carol =
      account_fixture("carol@acme.test",
        tenant: bob |> Map.get(:tenant_id) |> Directory.get_tenant!()
      )

    assert [] = Directory.rules_for(carol) |> Enum.filter(&(&1.name == "skip_sequence"))
  end

  test "rule changes apply without a restart: through Directory at once, from the table on the next poll",
       %{bob: bob} do
    spec = %{
      "test" => %{"header" => "Subject", "contains" => "drop me"},
      # Without "stop" the forward stage would still run, as in Sieve.
      "actions" => ["discard", "stop"]
    }

    {:ok, rule} =
      Directory.create_rule(%{
        scope: "global",
        stage: "detect",
        position: 1,
        name: "t",
        spec: spec
      })

    assert {:ok, [{:discarded, _}]} =
             Delivery.deliver(raw_message(subject: "drop me"), "bob@acme.test")

    {:ok, _} = Directory.update_rule(rule, %{enabled: false})

    assert {:ok, [{:stored, _, _, _}, _]} =
             Delivery.deliver(raw_message(subject: "drop me"), "bob@acme.test")

    # Written behind Directory's back (another node, a migration): picked up by the poll.
    Repo.query!("UPDATE delivery_rules SET enabled = true, updated_at = now() WHERE id = $1", [
      rule.id
    ])

    Cache.refresh()

    assert {:ok, [{:discarded, _}]} =
             Delivery.deliver(raw_message(subject: "drop me"), "bob@acme.test")

    assert bob.id
  end

  test "an invalid rule is refused with the compiler's reason" do
    assert {:error, cs} =
             Directory.create_rule(%{
               scope: "global",
               stage: "detect",
               position: 1,
               name: "bad",
               spec: %{"test" => %{"header" => "Subject", "regex" => "("}, "actions" => ["stop"]}
             })

    assert [{:spec, {"regex" <> _, _}}] = cs.errors
  end
end
