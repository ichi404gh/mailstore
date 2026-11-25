defmodule Mailstore.Imap.ImaplibTest do
  @moduledoc """
  End to end over TCP with a real client: Python's stock imaplib
  (`test/imaplib/client.py`). While the client sits in IDLE, the test delivers a
  message through `Mailstore.Delivery`, the same path Kafka consumers will use.
  """
  use Mailstore.DataCase, async: false
  alias Mailstore.Delivery

  @script Path.expand("../../imaplib/client.py", __DIR__)

  @tag timeout: 60_000
  test "python imaplib: login, list, select, append, fetch, store, search, copy, move, expunge, idle, bodystructure" do
    python = System.find_executable("python3") || flunk("python3 not found")
    account_fixture("bob@acme.test", password: "s3cret pass")

    port =
      Port.open({:spawn_executable, python}, [
        :binary,
        :exit_status,
        {:line, 8192},
        args: [
          "-I",
          @script,
          "127.0.0.1",
          Integer.to_string(Mailstore.Imap.port()),
          "bob@acme.test",
          "s3cret pass"
        ]
      ])

    {status, lines} = collect(port, [])
    output = Enum.join(lines, "\n")
    assert status == 0, "imaplib client failed:\n" <> output
    assert "ALL OK" in lines
  end

  defp collect(port, acc) do
    receive do
      {^port, {:data, {:eol, "IDLING"}}} ->
        {:ok, _} =
          Delivery.deliver(raw_message(subject: "pushed while idle"), "bob@acme.test",
            class: :reply
          )

        collect(port, ["IDLING" | acc])

      {^port, {:data, {:eol, line}}} ->
        collect(port, [line | acc])

      {^port, {:data, {:noeol, chunk}}} ->
        collect(port, [chunk | acc])

      {^port, {:exit_status, status}} ->
        {status, Enum.reverse(acc)}
    after
      30_000 -> {:timeout, Enum.reverse(acc)}
    end
  end
end
