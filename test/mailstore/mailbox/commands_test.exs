defmodule Mailstore.Mailbox.CommandsTest do
  @moduledoc "Database-level guarantees that do not depend on the actor."
  use Mailstore.DataCase, async: false
  alias Mailstore.Mailbox
  alias Mailstore.Mailbox.Commands

  test "split brain: writers bypassing the actor never collide on UIDs" do
    a = account_fixture("bob@acme.test")
    fid = folder_id(a, "INBOX")

    # Two "actors" (e.g. on both sides of a netsplit) writing straight to the database.
    1..40
    |> Task.async_stream(
      fn _ ->
        Repo.transaction(fn ->
          Commands.append(Repo, fid, a.tenant_id, [new_message(), new_message()])
        end)
      end,
      max_concurrency: 10
    )
    |> Enum.each(fn {:ok, {:ok, {_uids, _counters}}} -> :ok end)

    assert Mailbox.uids(a.tenant_id, fid) == Enum.to_list(1..80)
  end
end
