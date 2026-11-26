defmodule Mailstore.Mailbox.AccountServerTest do
  @moduledoc "Actor lifecycle (placement, idle shutdown, races) and its in-memory folder state."
  use Mailstore.DataCase, async: false
  alias Mailstore.Core.UidMap
  alias Mailstore.Mailbox
  alias Mailstore.Mailbox.AccountServer

  setup do
    %{account: account_fixture("bob@acme.test")}
  end

  test "concurrent starts give one actor", %{account: a} do
    pids =
      1..20
      |> Task.async_stream(fn _ -> AccountServer.ensure(a.id) end)
      |> Enum.map(fn {:ok, {:ok, pid}} -> pid end)

    assert pids |> Enum.uniq() |> length() == 1
  end

  test "unknown account does not start an actor" do
    assert {:error, :unknown_account} = AccountServer.ensure(999_999)
  end

  test "stops after idle timeout, but not while someone is subscribed", %{account: a} do
    Application.put_env(:mailstore, :account_idle_timeout, 100)
    on_exit(fn -> Application.put_env(:mailstore, :account_idle_timeout, :timer.minutes(10)) end)

    # Nobody subscribed: stops on its own.
    {:ok, pid} = AccountServer.ensure(a.id)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    # A subscribed session keeps it alive; when the session dies, idle timeout resumes.
    test_pid = self()

    session =
      spawn(fn ->
        {:ok, actor, _view} = Mailbox.subscribe(a.id, "INBOX")
        send(test_pid, {:subscribed, actor})
        Process.sleep(:infinity)
      end)

    assert_receive {:subscribed, actor}
    ref = Process.monitor(actor)
    refute_receive {:DOWN, ^ref, _, _, _}, 300

    Process.exit(session, :kill)
    assert_receive {:DOWN, ^ref, :process, ^actor, :normal}, 1_000
  end

  test "a call racing with actor shutdown is retried on a fresh actor", %{account: a} do
    {:ok, pid} = AccountServer.ensure(a.id)
    :ok = DynamicSupervisor.terminate_child(Mailstore.Mailbox.AccountSupervisor, pid)
    assert {:ok, [1]} = Mailbox.append(a.id, "INBOX", [new_message()])
  end

  # The actor's memory against the database after a random mix of every write.
  test "folder state in memory matches the database under random writes", %{account: a} do
    :rand.seed(:exsss, {7, 8, 9})
    inbox = folder_id(a, "INBOX")
    junk = folder_id(a, "Junk")
    # Load the state of both folders first, so every write has to keep it current.
    {:ok, _, _} = Mailbox.subscribe(a.id, "Junk")
    {:ok, _, _} = Mailbox.subscribe(a.id, "INBOX")

    for _ <- 1..60 do
      uids = UidMap.to_list(elem(Mailbox.subscribe(a.id, "INBOX"), 2).uids)
      some = Enum.take_random(uids, :rand.uniform(4))

      case :rand.uniform(7) do
        1 ->
          Mailbox.append(a.id, "INBOX", for(_ <- 1..:rand.uniform(3), do: new_message()))

        2 ->
          Mailbox.append(a.id, "INBOX", [new_message(flags: ["\\Seen"])])

        3 ->
          Mailbox.store_flags(a.id, "INBOX", some, :add, ["\\Seen"])

        4 ->
          Mailbox.store_flags(a.id, "INBOX", some, :remove, ["\\Seen"])

        5 ->
          Mailbox.store_flags(a.id, "INBOX", some, :replace, ["\\Flagged"])

        6 ->
          Mailbox.store_flags(a.id, "INBOX", some, :add, ["\\Deleted"]) &&
            Mailbox.expunge(a.id, "INBOX")

        7 ->
          if some != [], do: Mailbox.move(a.id, "INBOX", some, "Junk")
      end

      for {name, fid} <- [{"INBOX", inbox}, {"Junk", junk}] do
        {:ok, _, view} = Mailbox.subscribe(a.id, name)
        {:ok, unseen} = Mailbox.unseen(a.id, name)
        assert UidMap.ranges(view.uids) == Mailbox.uid_ranges(a.tenant_id, fid)
        assert UidMap.to_list(unseen) == Mailbox.unseen_uids(a.tenant_id, fid)
        assert view.first_unseen == Mailbox.first_unseen_uid(a.tenant_id, fid)
      end
    end
  end

  test "retention reload drops the state; it is read again on next use", %{account: a} do
    {:ok, _} = Mailbox.append(a.id, "INBOX", [new_message(), new_message()])
    {:ok, _, view} = Mailbox.subscribe(a.id, "INBOX")
    assert UidMap.to_list(view.uids) == [1, 2]

    # A retention job removes rows behind the actor's back, then asks for a reload.
    shard_repo(a).query!("DELETE FROM messages WHERE uid = 1")
    :ok = AccountServer.reload_all()
    {:ok, _, view} = Mailbox.subscribe(a.id, "INBOX")
    assert UidMap.to_list(view.uids) == [2]
  end
end
