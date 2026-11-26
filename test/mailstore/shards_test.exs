defmodule Mailstore.ShardsTest do
  @moduledoc """
  Two shards (the test database and mailstore_test_shard2): each tenant's mail lives on
  its shard, and a tenant moves between shards while mail keeps arriving and flags
  keep changing, without losing or duplicating anything.
  """
  use Mailstore.DataCase, async: false
  import Bitwise
  alias Mailstore.{Delivery, Directory, Mailbox, Shards}
  alias Mailstore.Core.NewMessage

  @moduletag timeout: 120_000

  setup_all do
    url =
      Application.get_env(:mailstore, Mailstore.Repo)[:url]
      |> String.replace(~r{/[^/]+$}, "/mailstore_test_shard2")

    {:ok, shard} = Shards.register("test-shard-2", url, create: true, migrate: true, pool_size: 5)

    # Other test modules see a single shard again.
    on_exit(fn ->
      Repo.query!("UPDATE tenants SET shard = 1 WHERE shard = $1", [shard])
      Repo.query!("DELETE FROM shards WHERE id = $1", [shard])
      Shards.refresh_local()
    end)

    %{shard: shard}
  end

  setup %{shard: shard} do
    # The second shard is not truncated by DataCase; keep its folder id range.
    Shards.repo_for(shard).query!(
      "TRUNCATE folders, messages, message_bodies, cold_bodies, cold_packs, vanished, deliveries"
    )

    Mailbox.ensure_partitions()
    :ok
  end

  defp count(repo, sql, params), do: hd(hd(repo.query!(sql, params).rows))

  defp messages_of(repo, account) do
    repo.query!(
      """
      SELECT m.uid, m.flags, m.message_id FROM messages m JOIN folders f ON f.id = m.folder_id
      WHERE f.account_id = $1 AND f.name = 'INBOX' ORDER BY m.uid
      """,
      [account.id]
    ).rows
  end

  test "a shard behind the schema is refused; migrate_all brings every shard up to date" do
    url =
      Application.get_env(:mailstore, Mailstore.Repo)[:url]
      |> String.replace(~r{/[^/]+$}, "/mailstore_test_shard3")

    Ecto.Adapters.Postgres.storage_down(Ecto.Repo.Supervisor.parse_url(url))

    assert {:error, {:pending_migrations, [_ | _]}} =
             Shards.register("test-shard-3", url, create: true)

    assert Repo.query!("SELECT count(*) FROM shards WHERE name = 'test-shard-3'").rows == [[0]]

    {:ok, id} = Shards.register("test-shard-3", url, migrate: true)
    assert Shards.pending_migrations(Shards.repo_for(id)) == []
    assert Enum.all?(Shards.migrate_all(), fn {_repo, applied} -> applied == [] end)

    Repo.query!("DELETE FROM shards WHERE id = $1", [id])
    Shards.refresh_local()
  end

  test "each tenant's mail lives on its shard", %{shard: shard} do
    t1 = tenant_fixture("one")
    {:ok, t2} = Directory.create_tenant(%{name: "two", shard: shard})
    a = account_fixture("a@one.test", tenant: t1)
    b = account_fixture("b@two.test", tenant: t2)

    {:ok, _} = Delivery.deliver(raw_message(), a.address)
    {:ok, _} = Delivery.deliver(raw_message(), b.address)
    {:ok, [2]} = Mailbox.append(b.id, "INBOX", [new_message()])

    s2 = Shards.repo_for(shard)
    s1 = Shards.repo_for(1)
    assert length(messages_of(s1, a)) == 1 and messages_of(s2, a) == []
    assert length(messages_of(s2, b)) == 2 and messages_of(s1, b) == []
    assert {:ok, %{messages_count: 2}} = Mailbox.status(b.id, "INBOX")

    # Folder ids come from the shard's own range.
    assert folder_id(b, "INBOX") >= shard <<< 40
    assert folder_id(a, "INBOX") in (1 <<< 40)..((2 <<< 40) - 1)
  end

  test "one batch for both shards: each part in its shard's transaction, results in order",
       %{shard: shard} do
    t1 = tenant_fixture("one")
    {:ok, t2} = Directory.create_tenant(%{name: "two", shard: shard})
    a = account_fixture("a@one.test", tenant: t1)
    b = account_fixture("b@two.test", tenant: t2)

    items =
      for {account, i} <- Enum.with_index([a, b, a, b, b]) do
        %{account_id: account.id, tenant_id: account.tenant_id, msg: new_message(subject: "#{i}")}
      end

    assert {:ok, results} = Mailbox.deliver(items)

    assert results == [
             {:stored, a.id, "INBOX", 1},
             {:stored, b.id, "INBOX", 1},
             {:stored, a.id, "INBOX", 2},
             {:stored, b.id, "INBOX", 2},
             {:stored, b.id, "INBOX", 3}
           ]

    assert length(messages_of(Shards.repo_for(1), a)) == 2
    assert length(messages_of(Shards.repo_for(shard), b)) == 3
  end

  test "master inboxes on a shared domain live on their tenants' shards", %{shard: shard} do
    {:ok, shared} = Directory.create_shared_domain("inbox.test")
    one = tenant_fixture("one")
    {:ok, two} = Directory.create_tenant(%{name: "two", shard: shard})
    b = account_fixture("b@two.test", tenant: two)
    {:ok, m1} = Directory.create_account(shared, "one-1", tenant: one, kind: "master_inbox")
    {:ok, m2} = Directory.create_account(shared, "two-1", tenant: two, kind: "master_inbox")
    {:ok, _} = Directory.create_forwarding(b, m2)

    {:ok, [_, {:stored, m2_id, "INBOX", 1}]} = Delivery.deliver(raw_message(), b.address)
    assert m2_id == m2.id

    s2 = Shards.repo_for(shard)
    assert length(messages_of(s2, m2)) == 1
    assert count(s2, "SELECT count(*) FROM folders WHERE account_id = $1", [m1.id]) == 0
    assert folder_id(m1, "INBOX") in (1 <<< 40)..((2 <<< 40) - 1)

    # The master inbox moves with its tenant.
    {:ok, _} = Shards.move_tenant(two.id, 1)
    assert length(messages_of(Shards.repo_for(1), m2)) == 1
  end

  test "a tenant moves while mail arrives and flags change: nothing lost, nothing doubled",
       %{shard: shard} do
    tenant = tenant_fixture("moving")
    a = account_fixture("a@moving.test", tenant: tenant)
    s2 = Shards.repo_for(shard)

    for i <- 1..60, do: {:ok, _} = Delivery.deliver(raw_message(subject: "old #{i}"), a.address)
    {:ok, _} = Mailbox.store_flags(a.id, "INBOX", Enum.to_list(41..50), :add, ["\\Flagged"])
    {:ok, _} = Mailbox.store_flags(a.id, "INBOX", Enum.to_list(51..60), :add, ["\\Deleted"])
    {:ok, expunged} = Mailbox.expunge(a.id, "INBOX")
    assert expunged == Enum.to_list(51..60)

    {:ok, actor, _view} = Mailbox.subscribe(a.id, "INBOX")
    ref = Process.monitor(actor)

    # Writers during the move: deliveries (each with its key, as ingest sends them),
    # and \Seen on the old messages 1..20.
    stop = :atomics.new(1, [])

    deliverer =
      Task.async(fn ->
        Stream.iterate(1, &(&1 + 1))
        |> Enum.reduce_while(0, fn i, stored ->
          if :atomics.get(stop, 1) == 1 do
            {:halt, stored}
          else
            raw = raw_message(subject: "during #{i}")

            {:ok, [{:stored, _, _, _}]} =
              Delivery.deliver(raw, a.address, key: :crypto.hash(:md5, raw))

            {:cont, stored + 1}
          end
        end)
      end)

    seer =
      Task.async(fn ->
        for uid <- 1..20 do
          {:ok, _} = Mailbox.store_flags(a.id, "INBOX", [uid], :add, ["\\Seen"])
          Process.sleep(5)
        end

        :ok
      end)

    Process.sleep(100)
    assert {:ok, stats} = Shards.move_tenant(tenant.id, shard)
    Process.sleep(200)
    :atomics.put(stop, 1, 1)
    during = Task.await(deliverer, 30_000)
    :ok = Task.await(seer, 30_000)

    assert stats.from == 1 and stats.to == shard
    assert Directory.shard_of(tenant.id) == shard
    assert during > 0

    # Everything is on the new shard, once.
    rows = messages_of(s2, a)
    assert length(rows) == 50 + during
    assert rows |> Enum.map(&List.last/1) |> Enum.uniq() |> length() == length(rows)
    uids = Enum.map(rows, &hd/1)
    assert uids == Enum.to_list(1..50) ++ Enum.to_list(61..(60 + during))

    flags = Map.new(rows, fn [uid, flags, _] -> {uid, flags} end)
    assert Enum.all?(1..20, &((flags[&1] &&& 1) == 1)), "\\Seen set during the move is kept"
    assert Enum.all?(41..50, &((flags[&1] &&& 4) == 4)), "\\Flagged set before the move is kept"

    fid = folder_id(a, "INBOX")

    assert [[uidnext, count, unseen]] =
             s2.query!(
               "SELECT uidnext, messages_count, unseen_count FROM folders WHERE id = $1",
               [fid]
             ).rows

    assert uidnext == 61 + during and count == 50 + during
    assert unseen == count - 20

    # The session's actor was stopped; subscribing again lands on the new shard.
    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000
    {:ok, _actor, view} = Mailbox.subscribe(a.id, "INBOX")
    assert view.uids.size == 50 + during

    # Writing again works, and replaying a delivery made during the move is recognized.
    {:ok, [{:stored, _, _, uid}]} = Delivery.deliver(raw_message(), a.address)
    assert uid == 61 + during

    # The old shard is fenced, then cleaned up.
    s1 = Shards.repo_for(1)

    assert count(s1, "SELECT count(*) FROM folders WHERE account_id = $1 AND NOT fenced", [a.id]) ==
             0

    assert {:error, :tenant_still_there} = Shards.cleanup_moved(tenant.id, shard)
    assert {:ok, %{folders: 5}} = Shards.cleanup_moved(tenant.id, 1)
    assert count(s1, "SELECT count(*) FROM folders WHERE account_id = $1", [a.id]) == 0
    assert length(messages_of(s2, a)) == 51 + during
  end

  # Holds `sql` (an advisory lock) on `repo` in another process until told to let go.
  defp hold(repo, sql) do
    parent = self()

    pid =
      spawn_link(fn ->
        repo.checkout(fn ->
          repo.query!(sql)
          send(parent, :held)
          receive do: (:release -> repo.query!("SELECT pg_advisory_unlock_all()"))
        end)

        send(parent, :released)
      end)

    assert_receive :held
    pid
  end

  defp release(pid) do
    send(pid, :release)
    assert_receive :released
  end

  test "expiry skips a shard a tenant move holds, and a move waits for a running expiry",
       %{shard: shard} do
    tenant = tenant_fixture("busy")
    a = account_fixture("a@busy.test", tenant: tenant)
    s1 = Shards.repo_for(1)
    Mailbox.Partitions.ensure(s1, Date.add(Date.utc_today(), -50), 10)

    {:ok, [_]} =
      Mailbox.append(a.id, "INBOX", [new_message(class: :warmup)],
        stored_at: DateTime.add(DateTime.utc_now(), -40 * 86_400)
      )

    # A move holds the shard: expiry leaves it alone.
    parent = self()

    mover =
      spawn_link(fn ->
        Mailbox.Partitions.with_maintenance_lock([{1, s1}], fn ->
          send(parent, :held)
          receive do: (:release -> :ok)
        end)

        send(parent, :released)
      end)

    assert_receive :held
    # (Other shards' old weeks may go; shard 1 is left alone.)
    {:ok, _} = Mailbox.apply_retention()
    assert {:ok, %{messages_count: 1}} = Mailbox.status(a.id, "INBOX")
    release(mover)

    assert {:ok, %{weeks: [_ | _]}} = Mailbox.apply_retention()
    assert {:ok, %{messages_count: 0}} = Mailbox.status(a.id, "INBOX")

    # An expiry is running (it holds the lock shared): the move waits for it.
    expiry = hold(s1, "SELECT pg_advisory_lock_shared(7003, 0)")
    move = Task.async(fn -> Shards.move_tenant(tenant.id, shard) end)
    assert Task.yield(move, 300) == nil
    release(expiry)
    assert {:ok, %{to: ^shard}} = Task.await(move, 30_000)
  end

  test "a tenant's cold mail moves with it; the old shard's expiry leaves its packs alone",
       %{shard: shard} do
    root = Application.get_env(:mailstore, :cold_store)[:root]
    File.rm_rf!(root)
    tenant = tenant_fixture("cold")
    a = account_fixture("a@cold.test", tenant: tenant)
    s1 = Shards.repo_for(1)
    s2 = Shards.repo_for(shard)
    Mailbox.Partitions.ensure(s1, Date.add(Date.utc_today(), -60), 12)
    old = DateTime.add(DateTime.utc_now(), -40 * 86_400)

    raws =
      for i <- 1..3,
          do: raw_message(subject: "cold #{i}", body: String.duplicate("x#{i} ", 9_000))

    {:ok, uids} =
      Mailbox.append(a.id, "INBOX", Enum.map(raws, &NewMessage.from_raw(&1, class: :reply)),
        stored_at: old
      )

    {:ok, %{packs: packs}} = Mailbox.Cold.run()
    assert packs > 0
    {:ok, _} = Shards.move_tenant(tenant.id, shard)

    # The target has the locations and the packs; the source keeps no pack rows.
    assert hd(
             hd(
               s2.query!("SELECT count(*) FROM cold_packs WHERE tenant_id = $1", [tenant.id]).rows
             )
           ) == packs

    assert s1.query!("SELECT count(*) FROM cold_packs WHERE tenant_id = $1", [tenant.id]).rows ==
             [[0]]

    fid = folder_id(a, "INBOX")
    assert Mailbox.fetch_raw_many(tenant.id, fid, uids) == Map.new(Enum.zip(uids, raws))

    # Expiry on the old shard does not delete what the tenant now reads on the new one.
    cutoff = Date.add(Date.utc_today(), -20)
    Mailbox.Partitions.expire(s1, 0, cutoff)
    assert length(Path.wildcard(Path.join(root, "**/*.pack"))) == packs
    Mailbox.Partitions.expire(s2, 0, cutoff)
    assert Path.wildcard(Path.join(root, "**/*.pack")) == []
  end

  test "IMAP APPEND racing the switch is retried on the new shard", %{shard: shard} do
    tenant = tenant_fixture("appender")
    a = account_fixture("a@appender.test", tenant: tenant)

    appender =
      Task.async(fn ->
        for i <- 1..40 do
          {:ok, [uid]} =
            Mailbox.append(a.id, "INBOX", [NewMessage.from_raw(raw_message(subject: "#{i}"))])

          uid
        end
      end)

    Process.sleep(30)
    {:ok, _} = Shards.move_tenant(tenant.id, shard)
    uids = Task.await(appender, 30_000)

    assert uids == Enum.to_list(1..40)
    assert length(messages_of(Shards.repo_for(shard), a)) == 40
  end
end
