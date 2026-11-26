defmodule Mailstore.Core.MimeText do
  @moduledoc """
  Text of a message the way Sieve tests see it (delivery rules, `Mailstore.Core.Rules`):

    * `decode_header/1`: a header value with RFC 2047 encoded-words decoded, as
      Sieve's `header` test compares it;
    * `parts/2`: the decoded content of each text/* leaf part (transfer encoding
      undone, charset converted to UTF-8 where known), as `foreverypart` +
      `extracttext` sees it. HTML is not converted to text: the rules match on `<p>`
      and `<br` as the Pigeonhole ones do.

  Charsets: UTF-8 and US-ASCII pass through, ISO-8859-1 and Windows-1252 are
  converted (1252 as Latin-1), anything else keeps its bytes, so ASCII markers still
  match. The functions never fail on malformed input.
  """
  alias Mailstore.Core.Mime

  @hex ~c"0123456789ABCDEFabcdef"

  @doc "RFC 2047: `=?charset?B|Q?text?=` words decoded; whitespace between two words dropped."
  def decode_header(nil), do: nil

  def decode_header(value) do
    if String.contains?(value, "=?") do
      ~r/=\?([^?\s]+)\?([bBqQ])\?([^?\s]*)\?=(?:\s+(?==\?[^?\s]+\?[bBqQ]\?))?/
      |> Regex.replace(value, fn whole, charset, enc, text ->
        case decode_word(enc, text) do
          {:ok, bytes} -> to_utf8(bytes, charset)
          :error -> whole
        end
      end)
    else
      value
    end
  end

  defp decode_word(enc, text) when enc in ["B", "b"] do
    case Base.decode64(text, padding: false) do
      {:ok, bytes} -> {:ok, bytes}
      :error -> Base.decode64(text)
    end
  end

  defp decode_word(_q, text), do: {:ok, text |> String.replace("_", " ") |> qp_decode()}

  @doc "Decoded text of every text/* leaf part, in order (`tree` from `Mime.parse/1`)."
  def parts(raw, tree \\ nil) do
    tree = tree || Mime.parse(raw)

    tree
    |> leaves([])
    |> Enum.reverse()
    |> Enum.flat_map(fn
      %{"t" => "text"} = node ->
        [decode_body(Mime.slice(raw, node["b"]), node["e"], Mime.param(node["p"], "charset"))]

      _ ->
        []
    end)
  end

  defp leaves(%{"c" => children}, acc), do: Enum.reduce(children, acc, &leaves/2)
  defp leaves(%{"m" => message}, acc), do: leaves(message, acc)
  defp leaves(node, acc), do: [node | acc]

  @doc "A part body with its transfer encoding undone and charset converted."
  def decode_body(bytes, encoding, charset) do
    bytes
    |> undo_transfer_encoding(encoding && String.downcase(encoding))
    |> to_utf8(charset)
  end

  defp undo_transfer_encoding(bytes, "base64") do
    clean =
      for <<c <- bytes>>, c in ?A..?Z or c in ?a..?z or c in ?0..?9 or c in [?+, ?/],
        into: "",
        do: <<c>>

    case Base.decode64(clean, padding: false) do
      {:ok, decoded} -> decoded
      :error -> bytes
    end
  end

  defp undo_transfer_encoding(bytes, "quoted-printable"), do: qp_decode(bytes, true)
  defp undo_transfer_encoding(bytes, _), do: bytes

  # =XX escapes and (in bodies) =CRLF soft breaks; a malformed escape is kept as is.
  # Jumps from one "=" to the next instead of walking byte by byte.
  defp qp_decode(text, soft_breaks? \\ false), do: qp(text, soft_breaks?, [])

  defp qp(bin, soft, acc) do
    case :binary.match(bin, "=") do
      :nomatch ->
        IO.iodata_to_binary(Enum.reverse([bin | acc]))

      {pos, 1} ->
        <<before::binary-size(^pos), ?=, rest::binary>> = bin

        case rest do
          <<"\r\n", r::binary>> when soft ->
            qp(r, soft, [before | acc])

          <<"\n", r::binary>> when soft ->
            qp(r, soft, [before | acc])

          <<a, b, r::binary>> when a in @hex and b in @hex ->
            qp(r, soft, [<<hex(a) * 16 + hex(b)>>, before | acc])

          _ ->
            qp(rest, soft, ["=", before | acc])
        end
    end
  end

  defp hex(c) when c in ?0..?9, do: c - ?0
  defp hex(c) when c in ?A..?F, do: c - ?A + 10
  defp hex(c) when c in ?a..?f, do: c - ?a + 10

  defp to_utf8(bytes, charset) do
    case charset && String.downcase(charset) do
      cs when cs in ["iso-8859-1", "latin1", "latin-1", "windows-1252", "cp1252", "us-ascii"] ->
        if String.valid?(bytes), do: bytes, else: :unicode.characters_to_binary(bytes, :latin1)

      _ ->
        bytes
    end
  end
end
