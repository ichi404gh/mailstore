defmodule Mailstore.Mailbox.PartitionsTest do
  use Mailstore.DataCase, async: false
  alias Mailstore.Mailbox

  test "dropping old weeks removes their messages and moves retained_from_uid" do
    a = account_fixture("bob@acme.test")
    last_week = Date.utc_today() |> Date.beginning_of_week() |> Date.add(-6)
    old = DateTime.new!(last_week, ~T[12:00:00.000000], "Etc/UTC")

    {:ok, [1, 2, 3]} =
      Mailbox.append(a.id, "INBOX", for(_ <- 1..3, do: new_message()), stored_at: old)

    {:ok, [4, 5]} = Mailbox.append(a.id, "INBOX", [new_message(), new_message()])
    {:ok, before} = Mailbox.status(a.id, "INBOX")

    this_week =
      DateTime.new!(Date.beginning_of_week(Date.utc_today()), ~T[00:00:00.000000], "Etc/UTC")

    assert {:ok, %{dropped: [_], affected_folders: [fid]}} =
             Mailbox.drop_partitions_before(this_week)

    assert Mailbox.uids(a.tenant_id, fid) == [4, 5]

    # Reload is a cast; a call queued after it sees the reloaded state.
    {:ok, now} = Mailbox.status(a.id, "INBOX")
    assert now.retained_from_uid == 4
    assert now.messages_count == 2
    assert now.highest_modseq == before.highest_modseq + 1
  end
end
