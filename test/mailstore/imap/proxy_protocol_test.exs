defmodule Mailstore.Imap.ProxyProtocolTest do
  @moduledoc "The listener HAProxy connects to: PROXY protocol header first, then IMAP."
  use Mailstore.DataCase, async: false

  setup do
    test_pid = self()
    id = {:proxy_test, make_ref()}

    :telemetry.attach(
      id,
      [:mailstore, :imap, :connection, :open],
      fn _, _, meta, _ -> send(test_pid, {:opened, meta.client}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
    :ok
  end

  defp connect do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", Mailstore.Imap.proxy_port(), [:binary, active: false])

    sock
  end

  test "the client address comes from the PROXY header, then IMAP works as usual" do
    account_fixture("bob@acme.test", password: "secret")
    sock = connect()
    :ok = :gen_tcp.send(sock, "PROXY TCP4 203.0.113.7 10.0.0.1 51234 993\r\n")
    assert {:ok, "* OK" <> _} = :gen_tcp.recv(sock, 0, 2_000)
    assert_receive {:opened, {203, 0, 113, 7}}

    :ok = :gen_tcp.send(sock, "a1 LOGIN bob@acme.test secret\r\n")
    assert {:ok, "a1 OK" <> _} = :gen_tcp.recv(sock, 0, 2_000)
    :gen_tcp.close(sock)
  end

  test "a connection without a PROXY header is closed" do
    sock = connect()
    :ok = :gen_tcp.send(sock, "a1 CAPABILITY\r\n")
    assert {:error, :closed} = :gen_tcp.recv(sock, 0, 2_000)
    refute_receive {:opened, _}
  end
end
