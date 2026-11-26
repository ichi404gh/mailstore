defmodule Mailstore.Imap.SessionTest do
  @moduledoc """
  The session state machine without sockets: feed command lines, check the bytes a
  client would receive. Actor pushes arrive in the test process (it is the subscriber).
  """
  use Mailstore.DataCase, async: false
  alias Mailstore.{Delivery, Mailbox}
  alias Mailstore.Imap.Session

  setup do
    %{account: account_fixture("bob@acme.test", password: "secret")}
  end

  defp cmd(s, line) do
    {out, s} = Session.handle_command(line <> "\r\n", s)
    {IO.iodata_to_binary(out), s}
  end

  defp login do
    {"a0 OK" <> _, s} = cmd(Session.new(), "a0 LOGIN bob@acme.test secret")
    s
  end

  defp selected(folder \\ "INBOX") do
    {out, s} = cmd(login(), "s1 SELECT #{folder}")
    assert out =~ "s1 OK [READ-WRITE]"
    s
  end

  # Delivery tells the account actor asynchronously; wait until it has pushed to us.
  defp deliver(opts \\ []) do
    {:ok, [{:stored, account_id, _, _}]} =
      Delivery.deliver(raw_message(opts), "bob@acme.test", class: :reply)

    if pid = Mailstore.Mailbox.AccountServer.whereis(account_id), do: :sys.get_state(pid)
  end

  describe "not authenticated" do
    test "greeting, capability, wrong and right password" do
      assert Session.greeting() =~ "* OK [CAPABILITY IMAP4rev1"
      s = Session.new()
      {out, s} = cmd(s, "a1 CAPABILITY")
      assert out =~ "* CAPABILITY IMAP4rev1" and out =~ "IDLE" and out =~ "a1 OK"

      {out, s} = cmd(s, "a2 SELECT INBOX")
      assert out =~ "a2 BAD"

      {out, s} = cmd(s, "a3 LOGIN bob@acme.test wrong")
      assert out =~ "a3 NO [AUTHENTICATIONFAILED]"

      {out, _s} = cmd(s, ~s(a4 LOGIN "bob@acme.test" "secret"))
      assert out =~ "a4 OK"
    end

    test "garbage gets BAD with the tag" do
      {out, _} = cmd(Session.new(), "a1 FROBNICATE")
      assert out == "a1 BAD Unknown command\r\n"
    end
  end

  describe "folders" do
    test "LIST shows special-use folders, STATUS comes from the actor" do
      deliver()
      s = login()
      {out, s} = cmd(s, ~s(a1 LIST "" "*"))
      assert out =~ ~s{* LIST (\\HasNoChildren) "/" "INBOX"}
      assert out =~ ~s{* LIST (\\HasNoChildren \\Sent) "/" "Sent"}

      {out, _} = cmd(s, "a2 STATUS INBOX (MESSAGES UNSEEN UIDNEXT)")
      assert out =~ ~s{* STATUS "INBOX" (MESSAGES 1 UNSEEN 1 UIDNEXT 2)}
    end

    test "SELECT reports counts, UIDVALIDITY, UIDNEXT, first unseen" do
      deliver()
      deliver()
      {out, _} = cmd(login(), "a1 SELECT inbox")
      assert out =~ "* 2 EXISTS"
      assert out =~ "* OK [UNSEEN 1]"
      assert out =~ "* OK [UIDVALIDITY "
      assert out =~ "* OK [UIDNEXT 3]"
      assert out =~ "a1 OK [READ-WRITE] SELECT completed"
    end

    test "unknown folder" do
      {out, _} = cmd(login(), "a1 SELECT Nope")
      assert out =~ "a1 NO"
    end

    test "CREATE with parents, LIST children, RENAME subtree, DELETE rules" do
      s = login()
      {out, s} = cmd(s, "a1 CREATE Work/2026/Q4/")
      assert out =~ "a1 OK"
      {out, s} = cmd(s, "a2 CREATE Work")
      assert out =~ "a2 NO [ALREADYEXISTS]"
      {out, s} = cmd(s, "a3 CREATE inbox")
      assert out =~ "a3 NO [ALREADYEXISTS]"
      {out, s} = cmd(s, ~s(a4 CREATE "bad*name"))
      assert out =~ "a4 NO"

      {out, s} = cmd(s, ~s(a5 LIST "" "Work/%"))
      assert out == ~s{* LIST (\\HasChildren) "/" "Work/2026"\r\na5 OK LIST completed\r\n}

      {out, s} = cmd(s, "a6 DELETE Work")
      assert out =~ "a6 NO [HASCHILDREN]"
      {out, s} = cmd(s, "a7 RENAME Work Archive/Work")
      assert out =~ "a7 OK"
      {out, s} = cmd(s, ~s(a8 LIST "" "*Q4"))
      assert out =~ ~s{"Archive/Work/2026/Q4"} and not (out =~ ~s{"Work/2026/Q4"})
      {out, s} = cmd(s, "a9 RENAME Archive Archive/Inside")
      assert out =~ "a9 NO [CANNOT]"

      for {tag, name} <- [b1: "Archive/Work/2026/Q4", b2: "Archive/Work/2026", b3: "Archive/Work"],
          do: assert(elem(cmd(s, "#{tag} DELETE #{name}"), 0) =~ "#{tag} OK")

      {out, _} = cmd(s, "c1 DELETE INBOX")
      assert out =~ "c1 NO [CANNOT]"
    end

    test "a folder created again under the same name gets a new UIDVALIDITY" do
      s = login()
      {_, s} = cmd(s, "a1 CREATE Box")
      {out, s} = cmd(s, "a2 STATUS Box (UIDVALIDITY)")
      [_, v1] = Regex.run(~r/UIDVALIDITY (\d+)/, out)
      {_, s} = cmd(s, "a3 DELETE Box")
      {_, s} = cmd(s, "a4 CREATE Box")
      {out, _} = cmd(s, "a5 STATUS Box (UIDVALIDITY)")
      [_, v2] = Regex.run(~r/UIDVALIDITY (\d+)/, out)
      assert v1 != v2
    end

    test "LSUB shows subscribed folders; an unsubscribed parent is \\Noselect" do
      s = login()
      {_, s} = cmd(s, "a1 CREATE P/child")
      {_, s} = cmd(s, ~s(a2 LSUB "" "P*"))
      {out, s} = cmd(s, ~s(a3 LSUB "" "P*"))
      assert out == "a3 OK LSUB completed\r\n"

      {out, s} = cmd(s, "a4 SUBSCRIBE P/child")
      assert out =~ "a4 OK"
      {out, s} = cmd(s, ~s(a5 LSUB "" "%"))
      assert out =~ ~s{* LSUB (\\Noselect) "/" "P"} and out =~ ~s{* LSUB () "/" "INBOX"}
      {out, s} = cmd(s, ~s(a6 LSUB "" "P/%"))
      assert out =~ ~s{* LSUB () "/" "P/child"}

      {out, s} = cmd(s, "a7 UNSUBSCRIBE P/child")
      assert out =~ "a7 OK"
      {out, _} = cmd(s, "a8 SUBSCRIBE Nope")
      assert out =~ "a8 NO [NONEXISTENT]"
    end

    test "another session renaming or deleting the selected folder" do
      {_, other} = cmd(login(), "o1 CREATE Shared")
      {_, s} = cmd(login(), "s1 SELECT Shared")

      {_, _} = cmd(other, "o2 RENAME Shared Renamed")
      # The rename applies at once: the next command uses the new name.
      {out, s} = cmd(s, "s2 STORE 1:* +FLAGS (\\Seen)")
      assert out =~ "s2 OK"
      assert s.folder.name == "Renamed"

      {_, _} = cmd(other, "o3 DELETE Renamed")
      {out, s} = cmd(s, "s3 NOOP")
      assert out =~ "* BYE Selected folder was deleted" and Session.logout?(s)
    end
  end

  describe "\\Recent and keywords" do
    test "new mail is \\Recent for one read-write session; EXAMINE does not take it" do
      deliver()
      deliver()
      {out, s1} = cmd(login(), "e1 EXAMINE INBOX")
      assert out =~ "* 2 RECENT"
      {out, _} = cmd(s1, "e2 STATUS INBOX (RECENT)")
      assert out =~ "RECENT 2"

      {out, s2} = cmd(login(), "s1 SELECT INBOX")
      assert out =~ "* 2 RECENT"
      {out, s2} = cmd(s2, "s2 SEARCH RECENT")
      assert out =~ "* SEARCH 1 2\r\n"
      {out, _} = cmd(login(), "s3 SELECT INBOX")
      assert out =~ "* 0 RECENT"

      # Arriving while selected: EXISTS and RECENT for the read-write session.
      deliver()
      {out, s2} = cmd(s2, "s4 NOOP")
      assert out =~ "* 3 EXISTS\r\n* 3 RECENT\r\n"
      {out, _} = cmd(s2, "s5 FETCH 3 FLAGS")
      assert out =~ "FLAGS (\\Recent)"
    end

    test "keywords in use are listed in FLAGS, and a new one is announced first" do
      deliver()
      {out, s} = cmd(login(), "a1 SELECT INBOX")
      assert out =~ "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n"

      {out, s} = cmd(s, "a2 STORE 1 +FLAGS ($Work)")

      assert out =~
               ~r/\* FLAGS \(.*\$Work\)\r\n\* OK \[PERMANENTFLAGS \(.*\$Work \\\*\)\].*\* 1 FETCH/s

      {out, _} = cmd(login(), "b1 SELECT INBOX")
      assert out =~ "\\Draft $Work)"
      assert s.keywords == MapSet.new(["$Work"])
    end
  end

  describe "fetch" do
    test "BODY.PEEK keeps unseen, BODY[] sets \\Seen and reports it" do
      deliver(subject: "hello")
      s = selected()

      {out, s} = cmd(s, "a1 FETCH 1 (FLAGS BODY.PEEK[HEADER.FIELDS (SUBJECT)])")
      assert out =~ "* 1 FETCH (FLAGS (\\Recent) BODY[HEADER.FIELDS (SUBJECT)] {"
      assert out =~ "Subject: hello\r\n\r\n)"

      {out, s} = cmd(s, "a2 UID FETCH 1 (BODY[TEXT]<0.5>)")
      assert out =~ "* 1 FETCH (UID 1 BODY[TEXT]<0> {5}\r\nHi Bo FLAGS (\\Seen \\Recent))"

      {out, _} = cmd(s, "a3 FETCH 1 FLAGS")
      assert out =~ "* 1 FETCH (FLAGS (\\Seen \\Recent))"
    end

    test "ENVELOPE, size, internal date" do
      deliver(subject: "env test", from: ~s("Alice B" <alice@example.org>))
      {out, _} = cmd(selected(), "a1 FETCH 1 (ENVELOPE RFC822.SIZE INTERNALDATE)")

      assert out =~
               ~s{ENVELOPE ("Fri, 20 Nov 2026 12:00:00 +0000" "env test" (("Alice B" NIL "alice" "example.org"))}

      assert out =~ ~r/RFC822.SIZE \d+ INTERNALDATE "\d\d-\w{3}-\d{4} /
    end

    test "BODYSTRUCTURE without the body, part sections by stored offsets" do
      raw =
        "From: alice@example.org\r\nSubject: parts\r\n" <>
          "Content-Type: multipart/mixed; boundary=b\r\n\r\n" <>
          "--b\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nhello\r\n" <>
          "--b\r\nContent-Type: text/html\r\nContent-Disposition: inline\r\n\r\n<b>hi</b>\r\n" <>
          "--b--\r\n"

      {:ok, _} = Delivery.deliver(raw, "bob@acme.test", class: :reply)
      s = selected()

      # Metadata only: one query, no body read.
      {{out, s}, n} = queries(fn -> cmd(s, "a1 FETCH 1 (BODYSTRUCTURE)") end)
      assert n == 1

      assert out =~
               ~s{* 1 FETCH (BODYSTRUCTURE (("text" "plain" ("charset" "utf-8") NIL NIL "7bit" 5 0 } <>
                 ~s{NIL NIL NIL NIL)("text" "html" ("charset" "us-ascii") NIL NIL "7bit" 9 0 } <>
                 ~s{NIL ("inline" NIL) NIL NIL) "mixed" ("boundary" "b") NIL NIL NIL))}

      {out, s} = cmd(s, "a2 FETCH 1 (BODY.PEEK[2] BODY.PEEK[2.MIME] BODY.PEEK[1]<1.3> BODY[9])")
      assert out =~ "BODY[2] {9}\r\n<b>hi</b> "
      assert out =~ "BODY[2.MIME] {56}\r\nContent-Type: text/html\r\n"
      assert out =~ "BODY[1]<1> {3}\r\nell "
      assert out =~ "BODY[9] NIL FLAGS (\\Seen \\Recent))"

      {out, _} = cmd(s, "a3 FETCH 1 FULL")

      assert out =~
               ~r/ENVELOPE \(.*\) BODY \(\("text" "plain" .* 5 0\)\("text" "html" .* 9 0\) "mixed"\)\)/
    end

    test "a row stored before MIME parsing gets its tree from the body" do
      deliver(body: "short")
      Mailstore.Shards.repo_for(1).query!("UPDATE messages SET bodystructure = NULL")
      {out, _} = cmd(selected(), "a1 FETCH 1 (BODY BODY.PEEK[1])")

      assert out =~
               ~s|BODY ("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 7 1) BODY[1] {7}|
    end
  end

  describe "store and search" do
    setup do
      deliver(subject: "first", from: "alice@example.org", message_id: "<m1@x>")
      deliver(subject: "second", from: "carol@example.org", body: "the magic word is xyzzy")
      :ok
    end

    test "STORE answers with new flags; SEARCH by flags, headers, text, uid" do
      s = selected()
      {out, s} = cmd(s, "a1 STORE 1 +FLAGS (\\Flagged)")
      assert out =~ "* 1 FETCH (FLAGS (\\Flagged \\Recent))"

      {out, s} = cmd(s, "a2 STORE 2 +FLAGS.SILENT (\\Seen)")
      refute out =~ "FETCH"

      search = fn s, q ->
        {out, s} = cmd(s, "q SEARCH " <> q)
        [line | _] = String.split(out, "\r\n")
        {line, s}
      end

      assert {"* SEARCH 1", s} = search.(s, "FLAGGED")
      assert {"* SEARCH 1", s} = search.(s, "UNSEEN")
      assert {"* SEARCH 2", s} = search.(s, "FROM carol")
      assert {"* SEARCH 1", s} = search.(s, "HEADER Message-ID <m1@x>")
      assert {"* SEARCH 2", s} = search.(s, "TEXT xyzzy")
      assert {"* SEARCH 1 2", s} = search.(s, "OR FLAGGED SEEN")
      assert {"* SEARCH 2", s} = search.(s, "NOT SUBJECT first")
      assert {"* SEARCH 1 2", s} = search.(s, "CHARSET UTF-8 ALL")
      {out, _} = cmd(s, "a3 UID SEARCH UID 2:*")
      assert out =~ "* SEARCH 2\r\n"
    end

    test "EXAMINE is read-only" do
      {_, s} = cmd(login(), "a1 EXAMINE INBOX")
      {out, _} = cmd(s, "a2 STORE 1 +FLAGS (\\Seen)")
      assert out =~ "a2 NO"
    end
  end

  describe "search fast paths" do
    setup do
      for i <- 1..5, do: deliver(subject: "msg #{i}")
      :ok
    end

    # Count Ecto queries issued while running `fun`: by this process, or with `:all` by
    # any process (the account actor included).
    defp queries(fun, scope \\ :self) do
      test_pid = self()
      ref = make_ref()
      id = {:count_queries, ref}

      :telemetry.attach(
        id,
        [:mailstore, :repo, :query],
        fn _e, _m, _meta, _ ->
          if scope == :all or self() == test_pid, do: send(test_pid, {ref, :query})
        end,
        nil
      )

      result = fun.()
      :telemetry.detach(id)
      {result, count_messages(ref, 0)}
    end

    defp count_messages(ref, n) do
      receive do
        {^ref, :query} -> count_messages(ref, n + 1)
      after
        0 -> n
      end
    end

    test "UID and sequence searches are answered from the UID map, without the database" do
      s = selected()

      {{out, s}, n} = queries(fn -> cmd(s, "a1 UID SEARCH UID 3:*") end)
      assert out =~ "* SEARCH 3 4 5\r\n" and n == 0

      {{out, s}, n} = queries(fn -> cmd(s, "a2 SEARCH 2:3") end)
      assert out =~ "* SEARCH 2 3\r\n" and n == 0

      # beyond the last UID still matches the last message (RFC 3501)
      {{out, _}, n} = queries(fn -> cmd(s, "a3 UID SEARCH UID 100:*") end)
      assert out =~ "* SEARCH 5\r\n" and n == 0
    end

    test "SELECT and UNSEEN come from the account actor's memory, without the database" do
      # The first SELECT loads the folder state into the actor; later ones do not query.
      selected()
      {{out, s}, n} = queries(fn -> cmd(login(), "s2 SELECT INBOX") end, :all)
      assert out =~ "* 5 EXISTS" and out =~ "[UNSEEN 1]" and n == 0

      {_, s} = cmd(s, "a1 STORE 1,2 +FLAGS.SILENT (\\Seen)")
      {{out, s}, n} = queries(fn -> cmd(s, "a2 UID SEARCH UNSEEN") end, :all)
      assert out =~ "* SEARCH 3 4 5\r\n" and n == 0

      # The actor keeps the state current through every write.
      {_, s} = cmd(s, "a3 STORE 2 -FLAGS.SILENT (\\Seen)")
      {_, s} = cmd(s, "a4 STORE 4 +FLAGS.SILENT (\\Deleted)")
      {_, s} = cmd(s, "a5 EXPUNGE")
      deliver(subject: "new")
      {{out, s}, n} = queries(fn -> cmd(s, "a6 UID SEARCH UNSEEN") end, :all)
      assert out =~ "* SEARCH 2 3 5 6\r\n" and n == 0

      {out, _} = cmd(login(), "s3 SELECT INBOX")
      assert out =~ "* 5 EXISTS" and out =~ "[UNSEEN 2]"
      {out, _} = cmd(s, "a7 UID SEARCH UID 1:* SEEN")
      assert out =~ "* SEARCH 1\r\n"
    end

    test "flag searches other than UNSEEN read narrow rows, not envelopes" do
      s = selected()
      {_, s} = cmd(s, "a1 STORE 1,2 +FLAGS.SILENT (\\Seen)")

      {{out, _}, n} = queries(fn -> cmd(s, "a2 UID SEARCH UID 2:4 SEEN") end)
      assert out =~ "* SEARCH 2\r\n" and n == 1
    end

    test "mail that arrived meanwhile is announced before the result and included in it" do
      s = selected()
      deliver(subject: "late")
      {out, _} = cmd(s, "a1 UID SEARCH UID 5:*")
      assert out =~ "* 6 EXISTS\r\n* 6 RECENT\r\n* SEARCH 5 6\r\n"
    end
  end

  describe "append, copy, move, expunge" do
    test "APPEND with a literal returns APPENDUID and the selected folder sees EXISTS" do
      s = selected()
      raw = raw_message(subject: "appended")

      {out, s} =
        cmd(
          s,
          "a1 APPEND INBOX (\\Seen) \" 5-Nov-2026 12:00:00 +0000\" {#{byte_size(raw)}}\r\n" <> raw
        )

      assert out =~ ~r/\* 1 EXISTS\r\n\* 1 RECENT\r\na1 OK \[APPENDUID \d+ 1\] APPEND completed/

      {out, _} = cmd(s, "a2 FETCH 1 (FLAGS INTERNALDATE)")
      assert out =~ ~s{FLAGS (\\Seen \\Recent) INTERNALDATE "05-Nov-2026 12:00:00 +0000"}
    end

    test "COPY and MOVE report COPYUID; MOVE expunges in this session" do
      deliver()
      deliver()
      s = selected()
      {out, s} = cmd(s, "a1 UID COPY 1:2 Trash")
      assert out =~ ~r/a1 OK \[COPYUID \d+ 1:2 1:2\] COPY completed/

      {out, s} = cmd(s, "a2 MOVE 1 Junk")
      assert out =~ ~r/\* OK \[COPYUID \d+ 1 1\] Moved\r\n\* 1 EXPUNGE\r\na2 OK MOVE completed/

      {out, _} = cmd(s, "a3 UID SEARCH ALL")
      assert out =~ "* SEARCH 2\r\n"
    end

    test "EXPUNGE removes \\Deleted and renumbers" do
      for _ <- 1..3, do: deliver()
      s = selected()
      {_, s} = cmd(s, "a1 STORE 1,3 +FLAGS.SILENT (\\Deleted)")
      {out, s} = cmd(s, "a2 EXPUNGE")
      assert out =~ "* 3 EXPUNGE\r\n* 1 EXPUNGE\r\na2 OK"

      {out, _} = cmd(s, "a3 FETCH 1 UID")
      assert out =~ "* 1 FETCH (UID 2)"
    end
  end

  describe "live updates" do
    test "IDLE gets new mail pushed immediately" do
      s = selected()
      {out, s} = cmd(s, "i1 IDLE")
      assert out == "+ idling\r\n"

      deliver(subject: "while idle")
      assert_receive {:mailstore, _, _} = push
      {out, s} = Session.handle_push(push, s)
      assert IO.iodata_to_binary(out) == "* 1 EXISTS\r\n* 1 RECENT\r\n"

      {out, _} = cmd(s, "DONE")
      assert out == "i1 OK IDLE terminated\r\n"
    end

    test "another client's EXPUNGE is held back during FETCH and sent after NOOP" do
      deliver()
      deliver()
      s = selected()
      %{account: a, folder: f} = s

      # Someone else deletes message 1 (a different process, not subscribed).
      Task.await(
        Task.async(fn ->
          Mailbox.store_flags(a.id, f.name, [1], :add, ["\\Deleted"])
          Mailbox.expunge(a.id, f.name, :all)
        end)
      )

      {out, s} = cmd(s, "a1 FETCH 1:* (UID)")
      refute out =~ "EXPUNGE"
      # Message 1 is already gone from the database, so there is no data for it, but
      # sequence number 2 still means UID 2 until the EXPUNGE is reported.
      assert out =~ "* 2 FETCH (UID 2)"

      {out, _} = cmd(s, "a2 NOOP")
      assert out =~ "* 1 EXPUNGE\r\na2 OK"
    end

    test "if the account actor dies, the session resubscribes and catches up" do
      s = selected()
      Process.exit(s.actor, :kill)
      deliver(subject: "arrived while actor was down")

      {out, s} = Session.handle_actor_down(s)
      assert IO.iodata_to_binary(out) =~ "* 1 EXISTS"
      assert is_pid(s.actor) and Process.alive?(s.actor)
    end
  end
end
