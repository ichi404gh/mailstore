defmodule Mailstore.Ingest.PipelineTest do
  use Mailstore.DataCase, async: false
  # Dead letters are logged.
  @moduletag :capture_log
  alias Mailstore.{Directory, Mailbox}
  alias Mailstore.Ingest.{Pipeline, RecordTest}
  alias Mailstore.Outbound.TestAdapter

  setup do
    tenant = tenant_fixture()
    bob = account_fixture("bob@acme.test", tenant: tenant)
    name = :"pipeline_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Pipeline, name: name, producer: {Broadway.DummyProducer, []}, processors: 2, batchers: 1}
    )

    handler = "ingest-#{name}"
    me = self()

    :telemetry.attach(
      handler,
      [:mailstore, :ingest, :batch],
      fn _, measurements, _, _ -> send(me, {:batch, measurements}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    %{bob: bob, name: name}
  end

  defp raw(n), do: "From: a@example.org\r\nSubject: m#{n}\r\nMessage-ID: <m#{n}@x>\r\n\r\nhi\r\n"

  # Acks come per stage (a record that fails to parse is acked by its processor).
  defp run(name, records) do
    ref = Broadway.test_batch(name, records, batch_mode: :bulk)
    {successful, failed} = acks(ref, length(records), [], [])
    assert_receive {:batch, counts}
    {successful, failed, counts}
  end

  defp acks(_ref, 0, ok, failed), do: {ok, failed}

  defp acks(ref, left, ok, failed) do
    assert_receive {:ack, ^ref, more_ok, more_failed}, 5_000
    acks(ref, left - length(more_ok) - length(more_failed), ok ++ more_ok, failed ++ more_failed)
  end

  test "stores a batch, recognizes it when replayed, dead-letters what it cannot parse",
       %{bob: bob, name: name} do
    records = [
      RecordTest.record(%{"envelope_content" => raw(1), "rcp_email" => "bob@acme.test"}),
      RecordTest.record(%{"envelope_content" => raw(2), "rcp_email" => "BOB@acme.test"}),
      RecordTest.record(%{"envelope_content" => raw(3), "rcp_email" => "nobody@acme.test"}),
      "not json"
    ]

    {ok, failed, counts} = run(name, records)
    assert length(ok) == 3
    assert [%{status: {:failed, "invalid JSON" <> _}}] = failed
    assert %{messages: 3, stored: 2, unknown: 1, duplicate: 0} = counts
    assert {:ok, %{messages_count: 2}} = Mailbox.status(bob.id, "INBOX")

    # A restart before the offset commit: the same records again.
    {ok, [], counts} = run(name, Enum.take(records, 3))
    assert length(ok) == 3
    assert %{stored: 0, duplicate: 2, unknown: 1} = counts
    assert {:ok, %{messages_count: 2, uidnext: 3}} = Mailbox.status(bob.id, "INBOX")
  end

  test "forwards go out before the ack, through a topic outage, and once",
       %{bob: bob, name: name} do
    TestAdapter.listen()
    on_exit(fn -> Application.delete_env(:mailstore, :outbound_test) end)
    {:ok, _} = Directory.create_forwarding(bob, "crm@partner.example")

    record =
      RecordTest.record(%{
        "envelope_content" => raw(20),
        "rcp_email" => "bob@acme.test",
        "sender_email" => "a@example.org"
      })

    TestAdapter.fail_next(2)
    {[_], [], %{stored: 1, external: 1}} = run(name, [record])
    assert_received {:outbound, [%{sender: "a@example.org", target: "crm@partner.example"}]}

    # Replayed (a restart before the offset commit): neither stored nor sent again.
    {[_], [], %{duplicate: 1, external: 1}} = run(name, [record])
    refute_received {:outbound, _}
  end

  test "class and internal date come from the record", %{bob: bob, name: name} do
    record =
      RecordTest.record(%{
        "envelope_content" => raw(9),
        "rcp_email" => "bob@acme.test",
        "is_warmup" => true
      })

    {[_], [], %{stored: 1}} = run(name, [record])
    [row] = Mailbox.fetch_flag_rows(bob.tenant_id, folder_id(bob, "INBOX"), [1])
    assert DateTime.compare(row.internal_date, ~U[2026-11-26 10:02:51Z]) == :eq

    assert [[1]] ==
             shard_repo(bob).query!("SELECT class FROM messages WHERE folder_id = $1", [
               folder_id(bob, "INBOX")
             ]).rows
  end
end
