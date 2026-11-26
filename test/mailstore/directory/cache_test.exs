defmodule Mailstore.Directory.CacheTest do
  use Mailstore.DataCase, async: false
  import Ecto.Query
  alias Mailstore.Directory
  alias Mailstore.Directory.Cache
  alias Mailstore.Directory.Schemas.Account

  setup do
    tenant = tenant_fixture()
    bob = account_fixture("bob@acme.test", tenant: tenant, password: "secret")
    master = account_fixture("master@acme.test", tenant: tenant, kind: "master_inbox")
    {:ok, _} = Directory.create_forwarding(bob, master, %{only_classes: ["reply"]})
    %{bob: bob, master: master}
  end

  defp db_queries(fun) do
    test_pid = self()
    id = {:db_queries, make_ref()}

    :telemetry.attach(
      id,
      [:mailstore, :repo, :query],
      fn _, _, _, _ -> if self() == test_pid, do: send(test_pid, {id, :q}) end,
      nil
    )

    result = fun.()
    :telemetry.detach(id)
    {result, count(id, 0)}
  end

  defp count(id, n) do
    receive do
      {^id, :q} -> count(id, n + 1)
    after
      0 -> n
    end
  end

  test "lookup, LOGIN and routing rules come from ETS, not the database", %{
    bob: bob,
    master: master
  } do
    {result, n} =
      db_queries(fn ->
        {Directory.lookup_account("BOB@acme.test"), Directory.forwardings_for(bob.id, :reply),
         Directory.authenticate("bob@acme.test", "secret"), Directory.get_account(master.id)}
      end)

    assert {{:ok, %{id: id}}, [%{target_account_id: target}], {:ok, _}, %{kind: "master_inbox"}} =
             result

    assert id == bob.id and target == master.id
    assert n == 0
    assert Directory.forwardings_for(bob.id, :warmup) == []
  end

  test "changes made behind Directory's back are picked up by the poll", %{bob: bob} do
    Repo.update_all(from(a in Account, where: a.id == ^bob.id),
      set: [status: "disabled", updated_at: DateTime.utc_now()]
    )

    assert {:ok, _} = Directory.lookup_account("bob@acme.test")

    Cache.refresh()
    assert {:error, :unknown_recipient} = Directory.lookup_account("bob@acme.test")
  end

  test "rows stamped alike by a bulk change are read once, not on every poll" do
    tenant = tenant_fixture("bulk")
    {:ok, domain} = Directory.create_domain(tenant, "bulk.test")
    stamp = DateTime.add(DateTime.utc_now(), -60)
    # The bulk stamp is the newest one the cache has seen.
    Repo.query!("UPDATE accounts SET updated_at = $1", [DateTime.add(stamp, -60)])

    Repo.query!(
      """
      INSERT INTO accounts (tenant_id, domain_id, local_part, address, inserted_at, updated_at)
      SELECT $1, $2, 'u' || i, 'u' || i || '@bulk.test', $3, $3 FROM generate_series(1, 500) i
      """,
      [tenant.id, domain.id, stamp]
    )

    send(Cache, :full_reload)
    Cache.refresh()
    assert {:ok, _} = Directory.lookup_account("u500@bulk.test")
    assert rows_read(&Cache.refresh/0) == 0
  end

  # Rows the cache process read from accounts during `fun`.
  defp rows_read(fun) do
    test_pid = self()
    id = {:rows, make_ref()}
    cache = Process.whereis(Cache)

    :telemetry.attach(
      id,
      [:mailstore, :repo, :query],
      fn _, _, meta, _ ->
        with true <- self() == cache,
             "accounts" <- meta[:source],
             {:ok, %{num_rows: n}} <- meta[:result],
             do: send(test_pid, {id, n})
      end,
      nil
    )

    fun.()
    :telemetry.detach(id)
    sum_rows(id, 0)
  end

  defp sum_rows(id, n) do
    receive do
      {^id, rows} -> sum_rows(id, n + rows)
    after
      0 -> n
    end
  end

  test "a successful LOGIN is remembered; a password change invalidates it", %{bob: bob} do
    refute Cache.auth_ok?(Directory.get_account(bob.id), "secret")
    assert {:ok, _} = Directory.authenticate("bob@acme.test", "secret")
    assert Cache.auth_ok?(Directory.get_account(bob.id), "secret")
    assert {:error, :invalid_credentials} = Directory.authenticate("bob@acme.test", "wrong")
    refute Cache.auth_ok?(Directory.get_account(bob.id), "wrong")

    new_hash = Directory.hash_password("new-secret")

    Repo.update_all(from(a in Account, where: a.id == ^bob.id),
      set: [password_hash: new_hash, updated_at: DateTime.utc_now()]
    )

    Cache.refresh()
    assert {:error, :invalid_credentials} = Directory.authenticate("bob@acme.test", "secret")
    assert {:ok, _} = Directory.authenticate("bob@acme.test", "new-secret")
  end

  test "hard-deleted rows disappear on the full reload", %{bob: bob} do
    Repo.delete_all(from(a in Account, where: a.id == ^bob.id))
    assert {:ok, _} = Directory.lookup_account("bob@acme.test")

    send(Cache, :full_reload)
    :sys.get_state(Cache)
    assert {:error, :unknown_recipient} = Directory.lookup_account("bob@acme.test")
    assert Directory.get_account(bob.id) == nil
  end
end
