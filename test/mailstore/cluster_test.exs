defmodule Mailstore.ClusterTest do
  @moduledoc """
  Two (and three) real BEAM nodes on the same test database: the actor of an account
  runs on its owner node, calls and deliveries from any node reach it, sessions on any
  node get its pushes, and accounts move when a node leaves or joins.
  """
  use Mailstore.DataCase, async: false
  alias Mailstore.{Delivery, Mailbox}
  alias Mailstore.Core.NewMessage
  alias Mailstore.Mailbox.{AccountServer, Placement}

  @moduletag :cluster
  @moduletag timeout: 120_000
  @sup Mailstore.Mailbox.AccountSupervisor

  setup_all do
    unless Node.alive?() do
      {_, 0} = System.cmd("epmd", ["-daemon"])
      {:ok, _} = :net_kernel.start(:"mailstore_test@127.0.0.1", %{name_domain: :longnames})
    end

    {pid, peer} = start_peer("peer_b")
    on_exit(fn -> stop_peer(pid) end)
    %{peer: peer}
  end

  setup %{peer: peer} do
    # The database was truncated (ids restart): forget the peer's actors and directory.
    for {_, pid, _, _} <- :erpc.call(peer, DynamicSupervisor, :which_children, [@sup]),
        is_pid(pid),
        do: DynamicSupervisor.terminate_child({@sup, peer}, pid)

    :ok = :erpc.call(peer, Mailstore.Directory.Cache, :reset, [])
    :ok
  end

  defp start_peer(name) do
    paths = Enum.flat_map(:code.get_path(), &[~c"-pa", &1])

    {:ok, pid, node} =
      :peer.start_link(%{
        name: :"#{name}_#{System.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", Atom.to_charlist(Node.get_cookie()) | paths]
      })

    :ok =
      :erpc.call(node, Application, :put_all_env, [
        [mailstore: Application.get_all_env(:mailstore)]
      ])

    :ok = :erpc.call(node, Application, :put_env, [:logger, :level, :warning])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:mailstore])
    wait_until(fn -> node in Placement.members() end)
    {pid, node}
  end

  defp stop_peer(pid) do
    :peer.stop(pid)
  catch
    _, _ -> :ok
  end

  defp wait_until(fun, tries \\ 100) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition not met")
      true -> Process.sleep(50) && wait_until(fun, tries - 1)
    end
  end

  # An account whose owner satisfies `pred` (rendezvous hashing: try ids until one fits).
  defp account_where(tenant, pred, peers) do
    Stream.iterate(1, &(&1 + 1))
    |> Enum.find_value(fn i ->
      a = account_fixture("u#{System.unique_integer([:positive])}-#{i}@acme.test", tenant: tenant)
      if pred.(a.id), do: a
    end)
    |> tap(fn _ -> for p <- peers, do: :erpc.call(p, Mailstore.Directory.Cache, :refresh, []) end)
  end

  defp exists_pushed(uid) do
    assert_receive {:mailstore, _fid, {:exists, [^uid], _view, _recent}}, 3_000
  end

  test "an account owned by the other node: its actor runs there, deliveries here push to a session here",
       %{peer: peer} do
    a = account_where(tenant_fixture(), &(Placement.owner(&1) == peer), [peer])

    {:ok, actor, view} = Mailbox.subscribe(a.id, "INBOX")
    assert node(actor) == peer and view.uids.size == 0
    assert AccountServer.whereis(a.id) == nil

    {:ok, [{:stored, _, _, 1}]} = Delivery.deliver(raw_message(), a.address)
    exists_pushed(1)
    assert {:ok, %{messages_count: 1, uidnext: 2}} = Mailbox.status(a.id, "INBOX")
  end

  test "an account owned by this node: delivery and APPEND on the other node reach the actor here",
       %{peer: peer} do
    b = account_where(tenant_fixture(), &(Placement.owner(&1) == node()), [peer])

    {:ok, actor, _view} = Mailbox.subscribe(b.id, "INBOX")
    assert node(actor) == node()

    {:ok, [{:stored, _, _, 1}]} =
      :erpc.call(peer, Delivery, :deliver, [raw_message(), b.address, []])

    exists_pushed(1)

    msg = NewMessage.from_raw(raw_message(subject: "appended on the peer"))
    assert {:ok, [2]} = :erpc.call(peer, Mailbox, :append, [b.id, "INBOX", [msg]])
    exists_pushed(2)
    # Only one actor in the cluster served it.
    assert :erpc.call(peer, AccountServer, :whereis, [b.id]) == nil
  end

  test "IMAP end to end: a client in IDLE on the other node gets mail delivered here", %{
    peer: peer
  } do
    tenant = tenant_fixture()

    a =
      Stream.repeatedly(fn ->
        account_fixture("imap#{System.unique_integer([:positive])}@acme.test",
          tenant: tenant,
          password: "pw"
        )
      end)
      |> Enum.find(&(Placement.owner(&1.id) == node()))

    :ok = :erpc.call(peer, Mailstore.Directory.Cache, :refresh, [])
    port = :erpc.call(peer, Mailstore.Imap, :port, [])
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :line])
    {:ok, "* OK" <> _} = :gen_tcp.recv(sock, 0, 2_000)

    send_line = fn line -> :ok = :gen_tcp.send(sock, line <> "\r\n") end

    read_until = fn prefix ->
      Stream.repeatedly(fn -> :gen_tcp.recv(sock, 0, 3_000) end)
      |> Enum.reduce_while([], fn {:ok, line}, acc ->
        if String.starts_with?(line, prefix),
          do: {:halt, [line | acc]},
          else: {:cont, [line | acc]}
      end)
      |> Enum.reverse()
    end

    send_line.("a LOGIN #{a.address} pw")
    assert List.last(read_until.("a ")) =~ "a OK"
    send_line.("b SELECT INBOX")
    assert Enum.any?(read_until.("b "), &(&1 =~ "* 0 EXISTS"))
    send_line.("c IDLE")
    assert ["+ idling\r\n"] = read_until.("+")

    {:ok, [{:stored, _, _, 1}]} = Delivery.deliver(raw_message(), a.address)
    assert ["* 1 EXISTS\r\n" | _] = read_until.("* 1 EXISTS")
    send_line.("DONE")
    :gen_tcp.close(sock)
  end

  test "a node leaves: its accounts move, sessions resubscribe at the new owner", %{peer: peer} do
    {pid, leaving} = start_peer("peer_c")
    a = account_where(tenant_fixture(), &(Placement.owner(&1) == leaving), [peer, leaving])

    {:ok, actor, _} = Mailbox.subscribe(a.id, "INBOX")
    assert node(actor) == leaving
    ref = Process.monitor(actor)

    stop_peer(pid)
    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000
    wait_until(fn -> leaving not in Placement.members() end)

    {:ok, actor, _} = Mailbox.subscribe(a.id, "INBOX")
    assert node(actor) == Placement.owner(a.id) and node(actor) != leaving

    {:ok, _} = Delivery.deliver(raw_message(), a.address)
    exists_pushed(1)
  end

  test "a node joins: actors of the accounts it wins are handed over", %{peer: peer} do
    joining = :"peer_d_#{System.unique_integer([:positive])}@127.0.0.1"
    now = Placement.members()
    later = Enum.sort([joining | now])

    a =
      account_where(
        tenant_fixture(),
        fn id -> Placement.owner(id, now) == node() and Placement.owner(id, later) == joining end,
        [peer]
      )

    {:ok, actor, _} = Mailbox.subscribe(a.id, "INBOX")
    assert node(actor) == node()
    ref = Process.monitor(actor)

    # start_peer names nodes itself; start one with the precomputed name instead.
    [name, _host] = joining |> Atom.to_string() |> String.split("@")
    paths = Enum.flat_map(:code.get_path(), &[~c"-pa", &1])

    {:ok, pid, ^joining} =
      :peer.start_link(%{
        name: String.to_atom(name),
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", Atom.to_charlist(Node.get_cookie()) | paths]
      })

    on_exit(fn -> stop_peer(pid) end)

    :ok =
      :erpc.call(joining, Application, :put_all_env, [
        [mailstore: Application.get_all_env(:mailstore)]
      ])

    :ok = :erpc.call(joining, Application, :put_env, [:logger, :level, :warning])
    {:ok, _} = :erpc.call(joining, Application, :ensure_all_started, [:mailstore])

    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000
    {:ok, actor, _} = Mailbox.subscribe(a.id, "INBOX")
    assert node(actor) == joining
    stop_peer(pid)
  end
end
