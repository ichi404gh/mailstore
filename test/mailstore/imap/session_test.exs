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

  defp deliver(opts \\ []) do
    {:ok, _} = Delivery.deliver(raw_message(opts), "bob@acme.test", class: :reply)
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
  end

  describe "fetch" do
    test "BODY.PEEK keeps unseen, BODY[] sets \\Seen and reports it" do
      deliver(subject: "hello")
      s = selected()

      {out, s} = cmd(s, "a1 FETCH 1 (FLAGS BODY.PEEK[HEADER.FIELDS (SUBJECT)])")
      assert out =~ "* 1 FETCH (FLAGS () BODY[HEADER.FIELDS (SUBJECT)] {"
      assert out =~ "Subject: hello\r\n\r\n)"

      {out, s} = cmd(s, "a2 UID FETCH 1 (BODY[TEXT]<0.5>)")
      assert out =~ "* 1 FETCH (UID 1 BODY[TEXT]<0> {5}\r\nHi Bo FLAGS (\\Seen))"

      {out, _} = cmd(s, "a3 FETCH 1 FLAGS")
      assert out =~ "* 1 FETCH (FLAGS (\\Seen))"
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
               ~s{* 1 FETCH (BODYSTRUCTURE (("text" "plain" ("charset" "utf-8") NIL NIL "7bit" 5 1 } <>
                 ~s{NIL NIL NIL NIL)("text" "html" ("charset" "us-ascii") NIL NIL "7bit" 9 1 } <>
                 ~s{NIL ("inline" NIL) NIL NIL) "mixed" ("boundary" "b") NIL NIL NIL))}

      {out, s} = cmd(s, "a2 FETCH 1 (BODY.PEEK[2] BODY.PEEK[2.MIME] BODY.PEEK[1]<1.3> BODY[9])")
      assert out =~ "BODY[2] {9}\r\n<b>hi</b> "
      assert out =~ "BODY[2.MIME] {56}\r\nContent-Type: text/html\r\n"
      assert out =~ "BODY[1]<1> {3}\r\nell "
      assert out =~ "BODY[9] NIL FLAGS (\\Seen))"

      {out, _} = cmd(s, "a3 FETCH 1 FULL")

      assert out =~
               ~r/ENVELOPE \(.*\) BODY \(\("text" "plain" .* 5 1\)\("text" "html" .* 9 1\) "mixed"\)\)/
    end

    test "a row stored before MIME parsing gets its tree from the body" do
      deliver(body: "short")
      Mailstore.Repo.query!("UPDATE messages SET bodystructure = NULL")
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
      assert out =~ "* 1 FETCH (FLAGS (\\Flagged))"

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

    # Count Ecto queries issued by this process while running `fun`.
    defp queries(fun) do
      test_pid = self()
      ref = make_ref()
      id = {:count_queries, ref}

      :telemetry.attach(
        id,
        [:mailstore, :repo, :query],
        fn _e, _m, _meta, _ ->
          if self() == test_pid, do: send(test_pid, {ref, :query})
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

    test "UNSEEN uses one indexed query; flag searches skip envelopes" do
      s = selected()
      {_, s} = cmd(s, "a1 STORE 1,2 +FLAGS.SILENT (\\Seen)")

      {{out, s}, n} = queries(fn -> cmd(s, "a2 UID SEARCH UNSEEN") end)
      assert out =~ "* SEARCH 3 4 5\r\n" and n == 1

      {{out, _}, n} = queries(fn -> cmd(s, "a3 UID SEARCH UID 2:4 SEEN") end)
      assert out =~ "* SEARCH 2\r\n" and n == 1
    end

    test "mail that arrived meanwhile is announced before the result and included in it" do
      s = selected()
      deliver(subject: "late")
      {out, _} = cmd(s, "a1 UID SEARCH UID 5:*")
      assert out =~ "* 6 EXISTS\r\n* SEARCH 5 6\r\n"
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

      assert out =~ ~r/\* 1 EXISTS\r\na1 OK \[APPENDUID \d+ 1\] APPEND completed/

      {out, _} = cmd(s, "a2 FETCH 1 (FLAGS INTERNALDATE)")
      assert out =~ ~s{FLAGS (\\Seen) INTERNALDATE "05-Nov-2026 12:00:00 +0000"}
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
      assert IO.iodata_to_binary(out) == "* 1 EXISTS\r\n"

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
