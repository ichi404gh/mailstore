defmodule Mailstore.MailboxTest do
  @moduledoc "Behaviour through the public API only."
  use Mailstore.DataCase, async: false
  alias Mailstore.Mailbox
  alias Mailstore.Core.NewMessage

  setup do
    %{account: account_fixture("bob@acme.test")}
  end

  test "a new account has the standard folders", %{account: a} do
    names = a.tenant_id |> Mailbox.list_folders(a.id) |> Enum.map(&{&1.name, &1.special_use})

    assert names == [
             {"INBOX", nil},
             {"Sent", "\\Sent"},
             {"Drafts", "\\Drafts"},
             {"Trash", "\\Trash"},
             {"Junk", "\\Junk"}
           ]
  end

  test "concurrent appends get unique, contiguous UIDs", %{account: a} do
    1..20
    |> Task.async_stream(fn _ ->
      Mailbox.append(a.id, "INBOX", for(_ <- 1..5, do: new_message()))
    end)
    |> Enum.each(fn {:ok, {:ok, uids}} -> assert length(uids) == 5 end)

    assert Mailbox.uids(a.tenant_id, folder_id(a, "INBOX")) == Enum.to_list(1..100)

    assert {:ok, %{uidnext: 101, messages_count: 100, unseen_count: 100}} =
             Mailbox.status(a.id, "inbox")
  end

  test "subscribers get EXISTS, FLAGS and EXPUNGE pushes", %{account: a} do
    {:ok, _pid, view} = Mailbox.subscribe(a.id, "INBOX")
    fid = view.id

    {:ok, [1]} = Mailbox.append(a.id, "INBOX", [new_message()])
    assert_receive {:mailstore, ^fid, {:exists, [1], %{uidnext: 2, unseen_count: 1}}}

    {:ok, [%{uid: 1, flags: ["\\Seen"], modseq: m1}]} =
      Mailbox.store_flags(a.id, "INBOX", [1], :add, ["\\Seen"])

    assert_receive {:mailstore, ^fid, {:flags, [%{uid: 1, flags: ["\\Seen"]}], ^m1}}
    assert {:ok, %{unseen_count: 0, highest_modseq: ^m1}} = Mailbox.status(a.id, "INBOX")

    # The same STORE again changes nothing: no new MODSEQ, no push.
    assert {:ok, []} = Mailbox.store_flags(a.id, "INBOX", [1], :add, ["\\Seen"])
    refute_receive {:mailstore, _, {:flags, _, _}}, 50

    {:ok, _} = Mailbox.store_flags(a.id, "INBOX", [1], :add, ["\\Deleted"])
    {:ok, [1]} = Mailbox.expunge(a.id, "INBOX")
    assert_receive {:mailstore, ^fid, {:expunge, [1], _modseq}}

    assert Mailbox.vanished_since(a.tenant_id, fid, m1) == [1]
    assert {:ok, %{messages_count: 0, unseen_count: 0}} = Mailbox.status(a.id, "INBOX")
  end

  test "keywords: add, remove, replace", %{account: a} do
    {:ok, [1]} = Mailbox.append(a.id, "INBOX", [new_message(flags: ["b"])])
    {:ok, [%{flags: ["a", "b"]}]} = Mailbox.store_flags(a.id, "INBOX", [1], :add, ["a", "b"])
    {:ok, [%{flags: ["a"]}]} = Mailbox.store_flags(a.id, "INBOX", [1], :remove, ["b"])

    {:ok, [%{flags: ["\\Flagged", "z"]}]} =
      Mailbox.store_flags(a.id, "INBOX", [1], :replace, ["z", "\\Flagged"])
  end

  test "MOVE keeps bytes, allocates new UIDs and expunges the source", %{account: a} do
    raw = raw_message(subject: "move me")
    {:ok, [1, 2]} = Mailbox.append(a.id, "INBOX", [NewMessage.from_raw(raw), new_message()])

    assert {:ok, %{mapping: [{1, 1}, {2, 2}]}} = Mailbox.move(a.id, "INBOX", [1, 2], "Trash")
    assert Mailbox.uids(a.tenant_id, folder_id(a, "INBOX")) == []
    assert {:ok, ^raw} = Mailbox.fetch_raw(a.tenant_id, folder_id(a, "Trash"), 1)
    assert {:ok, %{messages_count: 0}} = Mailbox.status(a.id, "INBOX")
    assert {:ok, %{messages_count: 2, uidnext: 3}} = Mailbox.status(a.id, "Trash")
  end

  test "uid_ranges returns runs of consecutive UIDs", %{account: a} do
    {:ok, _} = Mailbox.append(a.id, "INBOX", for(_ <- 1..6, do: new_message()))
    {:ok, _} = Mailbox.store_flags(a.id, "INBOX", [2, 5], :add, ["\\Deleted"])
    {:ok, [2, 5]} = Mailbox.expunge(a.id, "INBOX")
    assert Mailbox.uid_ranges(a.tenant_id, folder_id(a, "INBOX")) == [{1, 1}, {3, 4}, {6, 6}]
    assert Mailbox.first_unseen_uid(a.tenant_id, folder_id(a, "INBOX")) == 1
  end

  test "unknown folder and unknown account", %{account: a} do
    assert {:error, :no_such_folder} = Mailbox.status(a.id, "Nope")
    assert {:error, :unknown_account} = Mailbox.status(999_999, "INBOX")
  end
end
