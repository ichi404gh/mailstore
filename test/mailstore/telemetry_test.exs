defmodule Mailstore.TelemetryTest do
  use Mailstore.DataCase, async: false
  alias Mailstore.Imap.Session
  alias Mailstore.Telemetry
  alias Mailstore.Telemetry.CommandStats

  test "IMAP commands are counted per command and status" do
    account_fixture("bob@acme.test", password: "secret")
    CommandStats.reset()

    {_, s} = Session.handle_command("a1 LOGIN bob@acme.test wrong\r\n", Session.new())
    {_, s} = Session.handle_command("a2 LOGIN bob@acme.test secret\r\n", s)
    {_, _} = Session.handle_command("a3 SELECT INBOX\r\n", s)

    summary = CommandStats.summary()
    assert %{count: 1} = summary[{"LOGIN", :no}]
    assert %{count: 1} = summary[{"LOGIN", :ok}]
    assert %{count: 1, avg_us: avg} = summary[{"SELECT", :ok}]
    assert avg > 0
  end

  test "snapshot sees open IMAP connections and their memory" do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", Mailstore.Imap.port(), [:binary, active: false])
    {:ok, "* OK" <> _} = :gen_tcp.recv(sock, 0, 1_000)

    snap = Telemetry.snapshot(connections: true)
    assert snap.imap_connections >= 1
    assert snap.imap_connection_process_bytes > 0
    assert snap.memory.total > 0

    :gen_tcp.close(sock)
  end
end
