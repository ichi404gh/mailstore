defmodule Mailstore.Core.MimeTest do
  use ExUnit.Case, async: true
  alias Mailstore.Core.Mime
  alias Mailstore.Imap.{Fetch, Format}

  # mixed
  # ├── 1   alternative
  # │   ├── 1.1 text/plain (quoted-printable)
  # │   └── 1.2 text/html
  # ├── 2   application/pdf attachment, RFC 2231 filename continuations
  # └── 3   message/rfc822
  #         └── 3.1 text/plain (the forwarded message is not multipart)
  @raw Enum.join(
         [
           "From: Alice <alice@example.org>",
           "To: bob@acme.test",
           "Subject: report",
           "MIME-Version: 1.0",
           ~s(Content-Type: multipart/mixed; boundary="outer"),
           "",
           "This is a preamble.",
           "--outer",
           "Content-Type: multipart/alternative; boundary=inner",
           "",
           "--inner",
           "Content-Type: text/plain; charset=utf-8",
           "Content-Transfer-Encoding: quoted-printable",
           "",
           "Hello Bob,",
           "see attached.",
           "--inner",
           "Content-Type: text/html; charset=utf-8",
           "",
           "<p>Hello Bob</p>",
           "--inner--",
           "",
           "--outer",
           "Content-Type: application/pdf; name=report.pdf",
           "Content-Transfer-Encoding: base64",
           "Content-Disposition: attachment;",
           " filename*0=\"quarterly \"; filename*1=\"report.pdf\"",
           "",
           "JVBERi0xLjQK",
           "--outer",
           "Content-Type: message/rfc822",
           "",
           "From: carol@example.org",
           "Subject: original",
           "Message-ID: <orig@example.org>",
           "",
           "Original text.",
           "--outer--",
           "epilogue"
         ],
         "\r\n"
       )

  setup_all do
    %{tree: Mime.parse(@raw)}
  end

  defp section(tree, spec) do
    {:ok, [{:body, _, section, _, _}]} = Fetch.parse([{:atom, "BODY[#{spec}]"}])
    Fetch.extract(@raw, tree, section)
  end

  test "parts, offsets and parameters", %{tree: tree} do
    assert %{"t" => "multipart", "s" => "mixed", "c" => [alt, pdf, fwd]} = tree
    assert %{"s" => "alternative", "c" => [plain, html]} = alt
    assert %{"t" => "text", "s" => "plain", "e" => "quoted-printable", "n" => 1} = plain
    assert plain["p"] == [["charset", "utf-8"]]
    assert Mime.slice(@raw, plain["b"]) == "Hello Bob,\r\nsee attached."
    assert Mime.slice(@raw, html["b"]) == "<p>Hello Bob</p>"

    assert pdf["dp"] == ["attachment", [["filename", "quarterly report.pdf"]]]
    assert Mime.slice(@raw, pdf["b"]) == "JVBERi0xLjQK"

    assert fwd["env"]["subject"] == "original"
    assert %{"t" => "text", "s" => "plain", "n" => 0} = fwd["m"]
    assert Mime.slice(@raw, fwd["m"]["b"]) == "Original text."
  end

  test "section paths", %{tree: tree} do
    assert section(tree, "1.1") == "Hello Bob,\r\nsee attached."
    assert section(tree, "1.2.MIME") == "Content-Type: text/html; charset=utf-8\r\n\r\n"
    assert section(tree, "2") == "JVBERi0xLjQK"
    assert section(tree, "3.HEADER") =~ ~r/^From: carol@example.org\r\n.*\r\n\r\n$/s
    assert section(tree, "3.HEADER.FIELDS (SUBJECT)") == "Subject: original\r\n\r\n"
    assert section(tree, "3.TEXT") == "Original text."
    # The forwarded message is not multipart, so its only part is its body.
    assert section(tree, "3.1") == "Original text."
    assert section(tree, "3") =~ ~r/^From: carol.*Original text\.$/s
    # Multipart body runs from after its header to before the closing line break.
    assert section(tree, "1") =~ ~r/^--inner\r\n.*--inner--$/s
    # HEADER only exists for messages; missing parts are NIL.
    assert section(tree, "2.HEADER") == ""
    assert section(tree, "4") == nil
    assert section(tree, "1.3") == nil
    assert section(tree, "TEXT") =~ ~r/^This is a preamble\./
  end

  test "BODYSTRUCTURE and BODY", %{tree: tree} do
    bs = tree |> Format.body_structure(true) |> IO.iodata_to_binary()

    carol = ~s{((NIL NIL "carol" "example.org"))}

    assert bs ==
             ~s{((("text" "plain" ("charset" "utf-8") NIL NIL "quoted-printable" 25 1 NIL NIL NIL NIL)} <>
               ~s{("text" "html" ("charset" "utf-8") NIL NIL "7bit" 16 0 NIL NIL NIL NIL)} <>
               ~s{ "alternative" ("boundary" "inner") NIL NIL NIL)} <>
               ~s{("application" "pdf" ("name" "report.pdf") NIL NIL "base64" 12 NIL } <>
               ~s{("attachment" ("filename" "quarterly report.pdf")) NIL NIL)} <>
               ~s{("message" "rfc822" NIL NIL NIL "7bit" 92 } <>
               ~s{(NIL "original" #{carol} #{carol} #{carol} NIL NIL NIL NIL "<orig@example.org>") } <>
               ~s{("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 14 0 NIL NIL NIL NIL) 4 } <>
               ~s{NIL NIL NIL NIL)} <>
               ~s{ "mixed" ("boundary" "outer") NIL NIL NIL)}

    body = tree |> Format.body_structure(false) |> IO.iodata_to_binary()

    assert body =~
             ~r/^\(\(\("text" "plain" \("charset" "utf-8"\) NIL NIL "quoted-printable" 25 1\)/

    assert body =~ ~r/ "mixed"\)$/
  end

  test "a plain message is text/plain us-ascii, part 1 is its body" do
    raw = "Subject: hi\r\n\r\nline one\r\nline two\r\n"
    tree = Mime.parse(raw)

    assert tree |> Format.body_structure(true) |> IO.iodata_to_binary() ==
             ~s{("text" "plain" ("charset" "us-ascii") NIL NIL "7bit" 20 2 NIL NIL NIL NIL)}

    assert Fetch.extract(raw, tree, {[1], :full}) == "line one\r\nline two\r\n"
    assert Fetch.extract(raw, tree, {[1], :mime}) == "Subject: hi\r\n\r\n"
  end

  test "bare LF, headerless part, missing close delimiter, digest default" do
    raw =
      "Content-Type: multipart/digest; boundary=b\n\n--b\n\nFrom: x@y\n\nbody\n--b\n" <>
        "Content-Type: text/plain\n\nunterminated"

    assert %{"c" => [digest_part, last]} = Mime.parse(raw)
    assert %{"t" => "message", "s" => "rfc822", "h" => [_, 1]} = digest_part
    assert digest_part["env"]["from"] == "x@y"
    assert Mime.slice(raw, digest_part["m"]["b"]) == "body"
    assert Mime.slice(raw, last["b"]) == "unterminated"
  end

  test "hostile structure is capped: depth, and parts beyond the limit stay in the last one" do
    many =
      "Content-Type: multipart/mixed; boundary=b\r\n\r\n" <>
        String.duplicate("--b\r\n\r\nx\r\n", 5000) <> "--b--"

    %{"c" => children} = Mime.parse(many)
    assert length(children) == 999
    assert many |> Mime.slice(List.last(children)["b"]) |> String.starts_with?("x\r\n--b\r\n")

    nested = String.duplicate("Content-Type: message/rfc822\r\n\r\n", 100) <> "x"
    depth = Stream.iterate(Mime.parse(nested), & &1["m"]) |> Enum.take_while(& &1) |> length()
    assert depth == 33

    deep_multipart =
      Enum.map_join(1..40, &"Content-Type: multipart/mixed; boundary=b#{&1}\r\n\r\n--b#{&1}\r\n")

    chain =
      Stream.iterate(Mime.parse(deep_multipart), &hd(&1["c"] || [nil]))
      |> Enum.take_while(& &1)

    assert length(chain) == 33
    assert %{"t" => "text", "s" => "plain"} = List.last(chain)
  end

  test "parameters keep their case; RFC 2231 values are decoded like Dovecot does" do
    raw =
      "Content-Type: application/octet-stream; Name=f.bin\r\n" <>
        "Content-Disposition: attachment; filename*=utf-8''%D1%84%D0%B0%D0%B9%D0%BB.bin;\r\n" <>
        " title*0*=iso-8859-1''caf%E9; title*1=\" au lait\"; other*=koi8-r''%C6%C1\r\n\r\nx"

    tree = Mime.parse(raw)
    assert tree["p"] == [["Name", "f.bin"]]
    assert Mime.param(tree["p"], "name") == "f.bin"

    assert tree["dp"] ==
             [
               "attachment",
               [["filename", "файл.bin"], ["title", "café au lait"], ["other*", "koi8-r''%C6%C1"]]
             ]
  end

  test "multipart without a usable boundary degrades to text/plain" do
    raw = "Content-Type: multipart/mixed\r\n\r\nno parts here"
    assert %{"t" => "text", "s" => "plain", "n" => 0} = Mime.parse(raw)
  end

  test "a delimiter must start a line and end it" do
    raw =
      "Content-Type: multipart/mixed; boundary=b\r\n\r\n--b\r\n\r\nsee --b and\r\n--bb\r\n" <>
        "--b  \r\n\r\nsecond\r\n--b--"

    assert %{"c" => [first, second]} = tree = Mime.parse(raw)
    assert Mime.slice(raw, first["b"]) == "see --b and\r\n--bb"
    assert Mime.slice(raw, second["b"]) == "second"
    assert Jason.encode!(tree) |> Jason.decode!() == tree
  end
end
