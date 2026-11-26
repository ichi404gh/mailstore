defmodule Mailstore.Mailbox.DeliverTest do
  use Mailstore.DataCase, async: false
  alias Mailstore.Mailbox
  alias Mailstore.Mailbox.{AccountServer, Commands, Partitions}

  setup do
    tenant = tenant_fixture()
    a = account_fixture("a@acme.test", tenant: tenant)
    b = account_fixture("b@acme.test", tenant: tenant)
    %{a: a, b: b}
  end

  defp item(account, opts \\ []) do
    %{
      account_id: account.id,
      tenant_id: account.tenant_id,
      msg: new_message(opts),
      key: Keyword.get(opts, :key)
    }
  end

  defp key, do: :crypto.strong_rand_bytes(16)

  defp counters(account) do
    shard_repo(account).query!(
      "SELECT uidnext, messages_count, unseen_count FROM folders WHERE id = $1",
      [folder_id(account, "INBOX")]
    ).rows
  end

  # Commit without telling the actors: a writer whose notification is still on its way.
  defp commit_silently(items) do
    items = Enum.map(items, &Map.put(&1, :folder, "INBOX"))
    repo = shard_repo(%{tenant_id: hd(items).tenant_id})
    {:ok, {_results, updates}} = repo.transaction(fn -> Commands.deliver(repo, items) end)
    updates
  end

  defp exists_events(acc \\ []) do
    receive do
      {:mailstore, _fid, {:exists, uids, _view, _recent}} -> exists_events([uids | acc])
      {:mailstore, _fid, {:exists, uids, _view}} -> exists_events([uids | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  test "one transaction for many accounts: UIDs per folder, counters", %{a: a, b: b} do
    assert {:ok, results} = Mailbox.deliver([item(a), item(b), item(a), item(a), item(b)])

    assert [
             {:stored, a.id, "INBOX", 1},
             {:stored, b.id, "INBOX", 1},
             {:stored, a.id, "INBOX", 2},
             {:stored, a.id, "INBOX", 3},
             {:stored, b.id, "INBOX", 2}
           ] == results

    assert [[4, 3, 3]] == counters(a)
    assert [[3, 2, 2]] == counters(b)
    assert [1, 2, 3] == Mailbox.uids(a.tenant_id, folder_id(a, "INBOX"))
  end

  test "a replayed batch stores nothing and spends no UIDs", %{a: a, b: b} do
    batch = [item(a, key: key()), item(b, key: key()), item(a, key: key())]
    {:ok, _} = Mailbox.deliver(batch)
    before = {counters(a), counters(b)}

    assert {:ok, [{:duplicate, _}, {:duplicate, _}, {:duplicate, _}]} = Mailbox.deliver(batch)
    assert before == {counters(a), counters(b)}

    # Replayed together with new mail: the new one gets the next UID, no hole.
    assert {:ok, [{:duplicate, _}, {:stored, _, "INBOX", 3}]} =
             Mailbox.deliver([hd(batch), item(a, key: key())])
  end

  test "the same key twice in one batch is stored once; per account; no key never dedups",
       %{a: a, b: b} do
    k = key()

    assert {:ok, [{:stored, _, _, 1}, {:duplicate, _}, {:stored, _, _, 1}]} =
             Mailbox.deliver([item(a, key: k), item(a, key: k), item(b, key: k)])

    msg = item(a)
    assert {:ok, [{:stored, _, _, 2}, {:stored, _, _, 3}]} = Mailbox.deliver([msg, msg])
  end

  test "a key recorded on an earlier day is still a duplicate", %{a: a} do
    k = key()

    shard_repo(a).query!(
      "INSERT INTO deliveries (account_id, key, created_at) VALUES ($1, $2, now() - interval '1 day')",
      [a.id, k]
    )

    assert {:ok, [{:duplicate, _}]} = Mailbox.deliver([item(a, key: k)])
  end

  test "a session sees UIDs once and in order when notifications arrive out of order",
       %{a: a} do
    {:ok, _pid, view} = Mailbox.subscribe(a.id, "INBOX")
    assert view.uids.size == 0

    [first] = commit_silently([item(a)])
    [second] = commit_silently([item(a), item(a)])
    assert {first.uids, second.uids} == {[1], [2, 3]}

    AccountServer.delivered(a.id, second)
    AccountServer.delivered(a.id, first)
    assert [[1, 2, 3]] == exists_events()

    assert {:ok, %{uidnext: 4, messages_count: 3}} = Mailbox.status(a.id, "INBOX")
    assert {:ok, %{size: 3}} = Mailbox.unseen(a.id, "INBOX")
  end

  test "the actor's own write announces a delivery whose notification is late", %{a: a} do
    {:ok, _pid, _view} = Mailbox.subscribe(a.id, "INBOX")
    [late] = commit_silently([item(a)])

    {:ok, [2]} = Mailbox.append(a.id, "INBOX", [new_message()])
    AccountServer.delivered(a.id, late)
    assert [[1, 2]] == exists_events()
  end

  test "\\Recent survives the actor: claimed messages are not recent after a restart", %{a: a} do
    {:ok, _} = Mailbox.deliver([item(a), item(a)])
    {:ok, _pid, view} = Mailbox.subscribe(a.id, "INBOX")
    assert view.recent == [1, 2]

    pid = AccountServer.whereis(a.id)
    :ok = DynamicSupervisor.terminate_child(Mailstore.Mailbox.AccountSupervisor, pid)

    {:ok, _} = Mailbox.deliver([item(a)])
    {:ok, _pid, view} = Mailbox.subscribe(a.id, "INBOX")
    assert view.recent == [3]
  end

  test "dedup horizon: days before the cutoff are dropped" do
    today = Date.utc_today()
    Mailbox.ensure_partitions()
    tomorrow = "deliveries_" <> Calendar.strftime(Date.add(today, 1), "%Y%m%d")
    yesterday = "deliveries_" <> Calendar.strftime(Date.add(today, -1), "%Y%m%d")

    dropped = Partitions.drop_deliveries_before(Mailstore.Shards.repo_for(1), today)
    assert yesterday in dropped
    refute tomorrow in dropped
    Mailbox.ensure_partitions()
  end
end
