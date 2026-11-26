defmodule Mailstore.Core.RulesTest do
  use ExUnit.Case, async: true
  alias Mailstore.Core.Rules

  defp rule!(map) do
    {:ok, rule} = Rules.compile(Map.merge(%{"stage" => "detect", "position" => 0}, map))
    rule
  end

  defp msg(headers, body \\ "hello\r\n") do
    Enum.map_join(headers, "", fn {k, v} -> "#{k}: #{v}\r\n" end) <> "\r\n" <> body
  end

  @plain "From: Ann <ann@example.org>\r\nSubject: Hi\r\nMessage-ID: <m1@x>\r\n\r\nhello\r\n"

  test "tag and stop: the header goes on top, the message is kept, later rules do not run" do
    rules = [
      rule!(%{
        "name" => "tag",
        "position" => 1,
        "test" => %{"header" => "Message-ID", "contains" => "<m1@"},
        "actions" => [%{"add_header" => ["X-Tag", "one"]}, %{"set_class" => "warmup"}, "stop"]
      }),
      rule!(%{"name" => "never", "position" => 2, "actions" => ["discard"]})
    ]

    out = Rules.evaluate(rules, @plain)
    assert out.keep == "X-Tag: one\r\n" <> @plain
    assert out.class == :warmup
    assert out.matched == ["tag"]
    refute out.discarded
  end

  test "rules run by position whatever the list order; discard and redirect cancel the keep" do
    discard = rule!(%{"name" => "d", "position" => 2, "actions" => ["discard"]})
    redirect = rule!(%{"name" => "r", "position" => 1, "actions" => [%{"redirect" => "b@x"}]})

    out = Rules.evaluate([discard, redirect], @plain)
    assert out.matched == ["r", "d"]
    assert out.keep == nil and out.discarded
    assert out.copies == [{{:address, "b@x"}, @plain}]
  end

  test "keep stores the message as it is at that moment; later edits go to later copies" do
    r =
      rule!(%{
        "name" => "k",
        "actions" => ["keep", %{"set_header" => ["Subject", "Changed"]}, %{"redirect" => "c@x"}]
      })

    out = Rules.evaluate([r], @plain)
    assert out.keep == @plain
    [{_, copy}] = out.copies
    assert copy =~ "Subject: Changed\r\n"
    refute copy =~ "Subject: Hi"
  end

  describe "forward stage" do
    @fwd_copy [%{target: {:account, 7, 1}, mode: "copy"}]
    @fwd_redirect [%{target: {:account, 7, 1}, mode: "redirect"}]

    test "runs only for mailboxes with forwarding; forwards at its end unless a rule stops" do
      tagger =
        rule!(%{
          "name" => "f",
          "stage" => "forward",
          "actions" => [%{"add_header" => ["X-F", "1"]}]
        })

      assert %{copies: [], matched: []} = Rules.evaluate([tagger], @plain)

      out = Rules.evaluate([tagger], @plain, forwardings: @fwd_copy)
      assert out.keep == "X-F: 1\r\n" <> @plain
      assert [{{:account, 7, 1}, "X-F: 1\r\n" <> _}] = out.copies

      stop = rule!(%{"name" => "s", "stage" => "forward", "actions" => ["stop"]})
      assert %{copies: [], keep: @plain} = Rules.evaluate([stop], @plain, forwardings: @fwd_copy)
    end

    test "forward headers go on the copies only; redirect mode drops the original" do
      fwd =
        rule!(%{
          "name" => "fw",
          "stage" => "forward",
          "actions" => [
            %{"forward" => %{"headers" => [["X-Receiver", "${from_address}"]]}},
            "stop"
          ]
        })

      out = Rules.evaluate([fwd], @plain, forwardings: @fwd_copy)
      assert out.keep == @plain
      assert [{_, "X-Receiver: ann@example.org\r\n" <> _}] = out.copies

      assert %{keep: nil, discarded: false} =
               Rules.evaluate([fwd], @plain, forwardings: @fwd_redirect)
    end

    test "a detect rule that stops keeps the message out of forwarding" do
      stop = rule!(%{"name" => "warm", "actions" => ["stop"]})
      assert %{copies: [], keep: @plain} = Rules.evaluate([stop], @plain, forwardings: @fwd_copy)
    end
  end

  describe "tests" do
    test "captures are inserted escaped: a dot in the subject is a dot" do
      r =
        rule!(%{
          "name" => "cap",
          "test" => %{
            "all" => [
              %{"header" => "Subject", "regex" => "\\| (\\S+) (\\S+)$", "capture" => true},
              %{"body" => %{"regex" => "\n${2} ${1}\r?\n"}}
            ]
          },
          "actions" => ["stop"]
        })

      subject = fn s, body -> msg([{"Subject", s}], body) end
      assert %{matched: ["cap"]} = Rules.evaluate([r], subject.("x | a.b c", "text\nc a.b\n"))
      assert %{matched: []} = Rules.evaluate([r], subject.("x | a.b c", "text\nc aXb\n"))
    end

    test "case: insensitive by default (i;ascii-casemap), sensitive on request (i;octet)" do
      t = fn test -> rule!(%{"name" => "c", "test" => test, "actions" => ["stop"]}) end
      m = msg([{"Subject", "Automatic Reply: away"}])

      assert %{matched: ["c"]} =
               Rules.evaluate([t.(%{"header" => "subject", "contains" => "automatic reply"})], m)

      sensitive = %{"header" => "Subject", "contains" => "automatic reply", "case" => "sensitive"}
      assert %{matched: []} = Rules.evaluate([t.(sensitive)], m)
    end

    test "headers are RFC 2047 decoded; bodies are decoded per text part" do
      ru = "=?UTF-8?B?0J/RgNC40LLQtdGC?= =?UTF-8?Q?_=D0=BC=D0=B8=D1=80?="

      body =
        "--b\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n" <>
          "marker-=\r\nsoft\r\n--b\r\nContent-Type: text/html\r\nContent-Transfer-Encoding: base64\r\n\r\n" <>
          Base.encode64("<p>html marker</p>") <> "\r\n--b--\r\n"

      m = msg([{"Subject", ru}, {"Content-Type", "multipart/alternative; boundary=b"}], body)
      t = fn test -> rule!(%{"name" => "x", "test" => test, "actions" => ["stop"]}) end

      assert %{matched: ["x"]} =
               Rules.evaluate([t.(%{"header" => "Subject", "is" => "Привет мир"})], m)

      assert %{matched: ["x"]} =
               Rules.evaluate([t.(%{"body" => %{"contains" => "marker-soft"}})], m)

      assert %{matched: ["x"]} =
               Rules.evaluate([t.(%{"body" => %{"regex" => "<p>html marker</p>"}})], m)
    end

    test "exists, not, class, matches" do
      t = fn test -> rule!(%{"name" => "x", "test" => test, "actions" => ["stop"]}) end

      assert %{matched: ["x"]} =
               Rules.evaluate([t.(%{"header" => "message-id", "exists" => true})], @plain)

      assert %{matched: []} =
               Rules.evaluate([t.(%{"not" => %{"header" => "From", "exists" => true}})], @plain)

      assert %{matched: ["x"]} =
               Rules.evaluate([t.(%{"class" => ["warmup"]})], @plain, class: :warmup)

      assert %{matched: ["x"]} =
               Rules.evaluate([t.(%{"header" => "From", "matches" => "Ann *"})], @plain)
    end

    test "a pathological regex is no match, not a hang" do
      r =
        rule!(%{
          "name" => "evil",
          "test" => %{"body" => %{"regex" => "^(a+)+$"}},
          "actions" => ["stop"]
        })

      m = msg([{"Subject", "x"}], String.duplicate("a", 5_000) <> "!")
      {us, out} = :timer.tc(fn -> Rules.evaluate([r], m) end)
      assert out.matched == []
      assert us < 2_000_000
    end
  end

  test "invalid rules are rejected with a reason" do
    base = %{"name" => "x", "stage" => "detect", "actions" => ["stop"]}

    assert {:error, "regex" <> _} =
             Rules.compile(Map.put(base, "test", %{"header" => "S", "regex" => "("}))

    assert {:error, "unknown action" <> _} = Rules.compile(%{base | "actions" => ["explode"]})
    assert {:error, "unknown test" <> _} = Rules.compile(Map.put(base, "test", %{"size" => 1}))
    assert {:error, "stage" <> _} = Rules.compile(%{base | "stage" => "late"})

    assert {:error, "add_header" <> _} =
             Rules.compile(%{base | "actions" => [%{"add_header" => ["X", "a\r\nb"]}]})
  end
end
