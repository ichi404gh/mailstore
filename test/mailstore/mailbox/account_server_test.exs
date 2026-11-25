defmodule Mailstore.Mailbox.AccountServerTest do
  @moduledoc "Actor lifecycle: placement, idle shutdown, races."
  use Mailstore.DataCase, async: false
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
end
