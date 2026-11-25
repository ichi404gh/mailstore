defmodule Mailstore.Imap.ProtocolTest do
  @moduledoc "Pure protocol pieces: framing, parsing, sequence sets, UID map, formatting."
  use ExUnit.Case, async: true
  alias Mailstore.Core.Address
  alias Mailstore.Core.UidMap
  alias Mailstore.Imap.{Fetch, Format, Framing, Parser, SequenceSet}

  describe "framing" do
    test "complete lines, partial lines, two commands in one read" do
      f = Framing.new()
      assert {:more, _} = Framing.next("a1 NOOP", f)
      assert {:command, "a1 NOOP\r\n", "a2 NO", _} = Framing.next("a1 NOOP\r\na2 NO", f)
    end

    test "synchronizing literal asks for continuation once, then waits for the bytes" do
      f = Framing.new()
      assert {:continue, f} = Framing.next("a1 APPEND INBOX {5}\r\n", f)
      assert {:more, f} = Framing.next("a1 APPEND INBOX {5}\r\nhel", f)
      buf = "a1 APPEND INBOX {5}\r\nhello\r\n"
      assert {:command, ^buf, "", _} = Framing.next(buf, f)
    end

    test "LITERAL+ needs no continuation" do
      assert {:more, _} = Framing.next("a1 APPEND INBOX {5+}\r\nhe", Framing.new())
    end
  end

  describe "parser" do
    test "tag, command, UID prefix" do
      assert {:ok, "a1", "UID FETCH", [{:atom, "1:*"}, {:list, [{:atom, "FLAGS"}]}]} =
               Parser.parse("a1 uid fetch 1:* (FLAGS)\r\n")
    end

    test "quoted strings with escapes, literals, nested lists" do
      assert {:ok,
              [
                {:string, ~s(a "b" \\c)},
                {:string, "hi\r\nyo"},
                {:list, [{:list, [{:atom, "X"}]}]}
              ]} =
               Parser.tokenize("\"a \\\"b\\\" \\\\c\" {6}\r\nhi\r\nyo ((X))")
    end

    test "bracketed fetch attribute stays one atom" do
      assert {:ok,
              [{:list, [{:atom, "UID"}, {:atom, "BODY.PEEK[HEADER.FIELDS (FROM TO)]<0.100>"}]}]} =
               Parser.tokenize("(UID BODY.PEEK[HEADER.FIELDS (FROM TO)]<0.100>)")
    end

    test "errors keep the tag" do
      assert {:error, "a1", _} = Parser.parse("a1 LOGIN \"unterminated\r\n")
    end
  end

  describe "sequence sets and the UID map" do
    test "parse, membership and * semantics" do
      {:ok, set} = SequenceSet.parse("1:3,5,7:*")
      assert Enum.filter(1..9, &SequenceSet.member?(set, &1, 9)) == [1, 2, 3, 5, 7, 8, 9]
      # n:* beyond the last message still matches the last one (RFC 3501)
      {:ok, set} = SequenceSet.parse("10:*")
      assert SequenceSet.uids(set, UidMap.new([1, 2, 3])) == [3]
      assert :error = SequenceSet.parse("1:x")
    end

    test "format compacts runs" do
      assert SequenceSet.format([1, 2, 3, 5, 7, 8]) == "1:3,5,7:8"
    end

    test "uid <-> seq and removal order" do
      map = UidMap.new([10, 20, 30, 40])
      assert UidMap.uid(map, 2) == 20 and UidMap.seq(map, 30) == 3 and UidMap.seq(map, 31) == nil
      {seqs, map} = UidMap.remove(map, [20, 40, 99])
      assert seqs == [4, 2]
      assert UidMap.to_list(map) == [10, 30]
      assert map |> UidMap.append([30, 50]) |> UidMap.to_list() == [10, 30, 50]
    end
  end

  describe "formatting" do
    test "addresses and envelope" do
      assert [
               %{name: "Alice B", mailbox: "alice", host: "ex.org"},
               %{name: nil, mailbox: "bob", host: "x.io"}
             ] =
               Address.parse_list(~s("Alice B" <alice@ex.org>, bob@x.io))

      env = %{"subject" => "Hi", "from" => "Alice <a@ex.org>", "message_id" => "<1@x>"}

      assert env |> Format.envelope() |> IO.iodata_to_binary() ==
               ~s{(NIL "Hi" (("Alice" NIL "a" "ex.org")) (("Alice" NIL "a" "ex.org")) (("Alice" NIL "a" "ex.org")) NIL NIL NIL NIL "<1@x>")}
    end

    test "strings: quoted when safe, literal otherwise" do
      assert Format.string("a\"b") |> IO.iodata_to_binary() == ~s("a\\"b")
      assert Format.string("café") |> IO.iodata_to_binary() == "{5}\r\ncafé"
    end

    test "date-time round trip with offset" do
      {:ok, dt} = Format.parse_datetime(" 5-Nov-2026 15:30:00 +0300")
      assert dt.hour == 12 and dt.day == 5
      assert dt |> Format.datetime() |> IO.iodata_to_binary() == ~s("05-Nov-2026 12:30:00 +0000")
    end

    test "header field extraction keeps folded lines" do
      raw = "From: a@x\r\nSubject: one\r\n two\r\nTo: b@y\r\n\r\nbody"
      assert Fetch.extract(raw, {:fields, ["subject"]}) == "Subject: one\r\n two\r\n\r\n"
      assert Fetch.extract(raw, :text) == "body"
      assert Fetch.extract(raw, :header) == "From: a@x\r\nSubject: one\r\n two\r\nTo: b@y\r\n\r\n"
    end
  end
end
