defmodule Mailstore.Core.Mime do
  @moduledoc """
  MIME structure of a message (RFC 2045/2046), parsed once at delivery.

  The result is a tree of parts with byte offsets into the raw message, stored as jsonb
  in `messages.bodystructure`. IMAP renders BODY/BODYSTRUCTURE from it without reading
  the body, and FETCH `BODY[1.2]`, `BODY[2.HEADER]`, `BODY[1.MIME]` slice the raw bytes
  by these offsets instead of parsing again (later: range reads from cold storage).

  A node describes one entity, i.e. a header block followed by a body. The root is the
  message itself; a `message/rfc822` part keeps its encapsulated message under `"m"`,
  with the same shape. Keys are short because the tree is stored in every row:

      "t", "s"   type and subtype, lowercase       "p"   params [[name, value], ...]
      "id"       Content-ID                        "ds"  Content-Description
      "e"        Content-Transfer-Encoding         "md5" Content-MD5
      "dp"       disposition [type, params]        "l"   Content-Language [tag, ...]
      "lo"       Content-Location
      "h"        [offset, length] of the header block, including the empty line
      "b"        [offset, length] of the body (transfer-encoded bytes)
      "n"        lines in the body: LFs, as Dovecot counts (text/* and message/rfc822)
      "c"        child nodes (multipart)
      "m", "env" encapsulated message node and its ENVELOPE fields (message/rfc822)

  Absent keys mean NIL. Header values are made valid UTF-8 (jsonb rejects anything
  else); parameters keep their names' case and values as written, except that RFC 2231
  continuations are joined and charset-encoded values decoded to UTF-8, like Dovecot. The parser never fails: malformed
  input degrades to text/plain or to fewer parts.
  """
  alias Mailstore.Core.Headers

  @max_depth 32
  @max_parts 1000

  @doc "MIME tree of a raw message."
  def parse(raw) when is_binary(raw) do
    {node, _count} = entity(raw, 0, byte_size(raw), {"text", "plain"}, 0, 0)
    node
  end

  ## Entities

  # Parses the entity at raw[start, len). `count` is the number of parts seen so far,
  # threaded through to cap the work a hostile message can cause.
  defp entity(raw, start, len, default_type, depth, count) do
    hlen = header_length(raw, start, len)
    headers = Headers.parse(binary_part(raw, start, hlen))
    {type, subtype, params} = content_type(Headers.get(headers, "content-type"), default_type)

    node =
      %{
        "t" => type,
        "s" => subtype,
        "p" => params,
        "e" => enc = encoding(Headers.get(headers, "content-transfer-encoding")),
        "h" => [start, hlen],
        "b" => [start + hlen, len - hlen]
      }
      |> put("id", Headers.get(headers, "content-id"))
      |> put("ds", Headers.get(headers, "content-description"))
      |> put("md5", Headers.get(headers, "content-md5"))
      |> put("dp", disposition(Headers.get(headers, "content-disposition")))
      |> put("l", language(Headers.get(headers, "content-language")))
      |> put("lo", Headers.get(headers, "content-location"))

    body_start = start + hlen
    body_len = len - hlen
    count = count + 1
    deep? = depth >= @max_depth or count >= @max_parts

    cond do
      type == "multipart" and not deep? ->
        multipart(raw, node, body_start, body_len, depth, count)

      # BODYSTRUCTURE has no multipart without children.
      type == "multipart" ->
        {opaque(raw, node, body_start, body_len), count}

      # An encoded message/rfc822 is not a message we can look into; still a leaf.
      type == "message" and subtype == "rfc822" and enc in ["7bit", "8bit", "binary"] and
          not deep? ->
        {inner, count} = entity(raw, body_start, body_len, {"text", "plain"}, depth + 1, count)
        [_, inner_hlen] = inner["h"]
        inner_headers = Headers.parse(binary_part(raw, body_start, inner_hlen))

        node =
          node
          |> Map.put("m", inner)
          |> Map.put("env", Headers.envelope(inner_headers))
          |> Map.put("n", lines(raw, body_start, body_len))

        {node, count}

      type == "text" or (type == "message" and subtype == "rfc822") ->
        {Map.put(node, "n", lines(raw, body_start, body_len)), count}

      true ->
        {node, count}
    end
  end

  defp multipart(raw, node, body_start, body_len, depth, count) do
    child_type = if node["s"] == "digest", do: {"message", "rfc822"}, else: {"text", "plain"}

    ranges =
      case param(node["p"], "boundary") do
        b when is_binary(b) and b != "" -> split_parts(raw, body_start, body_len, b)
        _ -> []
      end

    case ranges do
      [] ->
        {opaque(raw, node, body_start, body_len), count}

      ranges ->
        {children, count} =
          ranges
          |> cap(max(@max_parts - count, 1))
          |> Enum.map_reduce(count, fn {s, l}, count ->
            entity(raw, s, l, child_type, depth + 1, count)
          end)

        {Map.put(node, "c", children), count}
    end
  end

  # A multipart we cannot split (no boundary, or too deep) is shown as plain text.
  defp opaque(raw, node, body_start, body_len) do
    node = %{node | "t" => "text", "s" => "plain", "p" => [["charset", "us-ascii"]]}
    Map.put(node, "n", lines(raw, body_start, body_len))
  end

  # Past the part limit, the remaining parts stay inside the last one, as in Dovecot:
  # the bytes are all still reachable, the tree stays small.
  defp cap(ranges, max) do
    case Enum.split(ranges, max - 1) do
      {_, [_]} ->
        ranges

      {_, []} ->
        ranges

      {kept, [{s, _} | _] = rest} ->
        {last_s, last_l} = List.last(rest)
        kept ++ [{s, last_s + last_l - s}]
    end
  end

  # Byte ranges of the body parts between boundary delimiters. The line break before a
  # delimiter belongs to the delimiter (RFC 2046 5.1.1); preamble and epilogue are
  # dropped. Without a close delimiter the last part runs to the end of the body.
  defp split_parts(raw, body_start, body_len, boundary) do
    body = binary_part(raw, body_start, body_len)
    dash = "--" <> boundary

    delimiters =
      body
      |> :binary.matches(dash)
      |> Stream.flat_map(fn {pos, dlen} ->
        line_start? = pos == 0 or :binary.at(body, pos - 1) == ?\n
        if line_start?, do: delimiter(body, pos, pos + dlen), else: []
      end)
      # Beyond the part limit the rest is one part anyway (see cap/2).
      |> Enum.take(@max_parts + 1)

    delimiters
    |> Enum.chunk_every(2, 1, [:eof])
    |> Enum.flat_map(fn
      [{_pos, :close, _}, _] ->
        []

      [{_pos, :open, content_start}, next] ->
        content_end =
          case next do
            {npos, _, _} -> before_line_break(body, npos)
            :eof -> body_len
          end

        [{body_start + content_start, max(content_end - content_start, 0)}]
    end)
    |> take_until_close(delimiters)
  end

  # Parts after a close delimiter are epilogue, even if more delimiters follow.
  defp take_until_close(parts, delimiters) do
    case Enum.find_index(delimiters, &match?({_, :close, _}, &1)) do
      nil -> parts
      i -> Enum.take(parts, i)
    end
  end

  # A delimiter line is "--boundary" followed by "--" (close) or by optional
  # whitespace and the line break. Anything else is a longer boundary or content.
  defp delimiter(body, pos, after_dash) do
    rest = binary_part(body, after_dash, byte_size(body) - after_dash)

    case rest do
      <<"--", _::binary>> ->
        [{pos, :close, nil}]

      _ ->
        {line_len, next} =
          case :binary.match(rest, "\n") do
            {i, 1} -> {i, after_dash + i + 1}
            :nomatch -> {byte_size(rest), byte_size(body)}
          end

        if blank?(binary_part(rest, 0, line_len)), do: [{pos, :open, next}], else: []
    end
  end

  defp blank?(s), do: s =~ ~r/^[ \t\r]*$/

  defp before_line_break(body, pos) when pos >= 2 do
    case binary_part(body, pos - 2, 2) do
      "\r\n" -> pos - 2
      <<_, ?\n>> -> pos - 1
      _ -> pos
    end
  end

  defp before_line_break(body, 1), do: if(:binary.at(body, 0) == ?\n, do: 0, else: 1)
  defp before_line_break(_body, 0), do: 0

  # Length of the header block at raw[start, len), including the empty line ending it.
  # An entity that starts with an empty line has no headers.
  defp header_length(raw, start, len) do
    region = binary_part(raw, start, len)

    case region do
      <<"\r\n", _::binary>> ->
        2

      <<"\n", _::binary>> ->
        1

      _ ->
        case :binary.match(region, ["\r\n\r\n", "\n\n", "\n\r\n"]) do
          {pos, mlen} -> pos + mlen
          :nomatch -> len
        end
    end
  end

  # Line count as Dovecot reports it: the number of LFs (a last line without one does
  # not count).
  defp lines(_raw, _start, 0), do: 0
  defp lines(raw, start, len), do: length(:binary.matches(binary_part(raw, start, len), "\n"))

  ## Header values

  defp content_type(nil, {t, s}), do: {t, s, default_params(t)}

  defp content_type(value, {dt, ds} = default) do
    {main, params} = split_params(value)

    case String.split(main, "/", parts: 2) do
      [t, s] when t != "" and s != "" ->
        t = t |> String.trim() |> String.downcase()
        s = s |> String.trim() |> String.downcase()

        params =
          if t == "text" and param(params, "charset") == nil,
            do: params ++ [["charset", "us-ascii"]],
            else: params

        {t, s, params}

      # Invalid Content-Type: RFC 2045 5.2 says treat as text/plain; keep the
      # multipart/digest default for its children.
      _ ->
        if default == {"message", "rfc822"},
          do: {dt, ds, []},
          else: {"text", "plain", default_params("text")}
    end
  end

  defp default_params("text"), do: [["charset", "us-ascii"]]
  defp default_params(_), do: []

  defp encoding(nil), do: "7bit"

  defp encoding(value) do
    case value |> String.trim() |> String.downcase() do
      "" -> "7bit"
      enc -> enc
    end
  end

  defp disposition(nil), do: nil

  defp disposition(value) do
    {main, params} = split_params(value)

    case main |> String.trim() |> String.downcase() do
      "" -> nil
      type -> [type, params]
    end
  end

  defp language(nil), do: nil

  defp language(value) do
    case value |> String.split(",") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == "")) do
      [] -> nil
      tags -> tags
    end
  end

  @doc false
  # Parameter names keep the case they were written in (as Dovecot shows them), so
  # lookups ignore case.
  def param(params, name) do
    Enum.find_value(params, fn [k, v] -> if String.downcase(k) == name, do: v end)
  end

  # "type/sub; a=1; b=\"x;y\"" -> {"type/sub", [["a", "1"], ["b", "x;y"]]}
  defp split_params(value) do
    [main | rest] = split_unquoted(value, ?;)

    params =
      rest
      |> Enum.flat_map(fn p ->
        case :binary.split(p, "=") do
          [k, v] ->
            k = String.trim(k)
            if k == "", do: [], else: [[k, unquote_value(String.trim(v))]]

          _ ->
            []
        end
      end)
      |> rfc2231()

    {String.trim(main), params}
  end

  defp split_unquoted(s, sep), do: split_unquoted(s, sep, false, [], [])

  defp split_unquoted(<<>>, _sep, _q, cur, acc),
    do: Enum.reverse([cur |> Enum.reverse() |> IO.iodata_to_binary() | acc])

  defp split_unquoted(<<?\\, c, rest::binary>>, sep, true, cur, acc),
    do: split_unquoted(rest, sep, true, [c, ?\\ | cur], acc)

  defp split_unquoted(<<?", rest::binary>>, sep, q, cur, acc),
    do: split_unquoted(rest, sep, not q, [?" | cur], acc)

  defp split_unquoted(<<c, rest::binary>>, sep, false, cur, acc) when c == sep,
    do:
      split_unquoted(rest, sep, false, [], [cur |> Enum.reverse() |> IO.iodata_to_binary() | acc])

  defp split_unquoted(<<c, rest::binary>>, sep, q, cur, acc),
    do: split_unquoted(rest, sep, q, [c | cur], acc)

  defp unquote_value(<<?", rest::binary>>) do
    rest
    |> String.replace_suffix(~s("), "")
    |> String.replace(~r/\\(.)/s, "\\1")
  end

  defp unquote_value(v), do: v

  # RFC 2231, decoded as Dovecot does: continuations (`name*0`, `name*1*`) are joined,
  # charset-encoded values (`name*=utf-8''%D1%84...`) become plain UTF-8 under `name`.
  # A charset we cannot convert keeps the encoded form under `name*`.
  defp rfc2231(params) do
    {order, groups} =
      Enum.reduce(params, {[], %{}}, fn [k, v], {order, groups} ->
        case Regex.run(~r/^(.+?)\*(\d+)?(\*)?$/, k) do
          nil ->
            {[{:plain, k, v} | order], groups}

          [_, name | rest] ->
            {n, ext?} =
              case rest do
                [] -> {0, true}
                ["", "*"] -> {0, true}
                [n] -> {String.to_integer(n), false}
                [n, star] -> {String.to_integer(n), star == "*"}
              end

            key = String.downcase(name)
            order = if Map.has_key?(groups, key), do: order, else: [{:group, key} | order]
            {order, Map.update(groups, key, [{n, ext?, v, name}], &[{n, ext?, v, name} | &1])}
        end
      end)

    order
    |> Enum.reverse()
    |> Enum.map(fn
      {:plain, k, v} -> [k, v]
      {:group, key} -> decode_segments(Enum.sort_by(groups[key], &elem(&1, 0)))
    end)
  end

  defp decode_segments([{_, first_ext?, _, name} | _] = segs) do
    {charset, parts} =
      segs
      |> Enum.with_index()
      |> Enum.map_reduce(nil, fn
        {{_, true, v, _}, 0}, _ ->
          case String.split(v, "'", parts: 3) do
            [charset, _lang, text] -> {percent_decode(text), String.downcase(charset)}
            _ -> {percent_decode(v), nil}
          end

        {{_, true, v, _}, _}, cs ->
          {percent_decode(v), cs}

        {{_, false, v, _}, _}, cs ->
          {v, cs}
      end)
      |> then(fn {parts, cs} -> {cs, parts} end)

    raw = Enum.join(parts)

    case to_utf8(charset, raw, first_ext?) do
      {:ok, text} -> [name, text]
      :error -> [name <> "*", Enum.map_join(segs, &elem(&1, 2))]
    end
  end

  defp to_utf8(_charset, raw, false), do: if(String.valid?(raw), do: {:ok, raw}, else: :error)

  defp to_utf8(charset, raw, true) when charset in [nil, "", "utf-8", "utf8", "us-ascii"],
    do: if(String.valid?(raw), do: {:ok, raw}, else: :error)

  defp to_utf8(charset, raw, true) when charset in ["iso-8859-1", "latin1"],
    do: {:ok, :unicode.characters_to_binary(raw, :latin1)}

  defp to_utf8(_charset, _raw, true), do: :error

  defp percent_decode(text) do
    URI.decode(text)
  rescue
    ArgumentError -> text
  end

  defp put(map, _key, nil), do: map
  defp put(map, key, value), do: Map.put(map, key, value)

  ## Sections

  @doc """
  Resolves a section part path (`[1, 2]` for `BODY[1.2]`) to a node, or nil.

  Every message has a part 1: for a non-multipart entity it is the entity itself.
  Below a message/rfc822 part, numbering continues inside the encapsulated message.
  """
  def part(node, []), do: node

  def part(node, [n | rest]) do
    sub =
      case node do
        %{"c" => children} -> if n <= length(children), do: Enum.at(children, n - 1)
        _ -> if n == 1, do: node
      end

    cond do
      sub == nil -> nil
      rest == [] -> sub
      Map.has_key?(sub, "m") -> part(sub["m"], rest)
      true -> part(sub, rest)
    end
  end

  @doc "Bytes of an [offset, length] region."
  def slice(raw, [offset, len]) do
    if offset >= byte_size(raw),
      do: "",
      else: binary_part(raw, offset, min(len, byte_size(raw) - offset))
  end

  @doc """
  The tree of the same message with `delta` bytes of header lines put in front (what
  delivery rules do when they add a header): offsets move, the root header grows.
  """
  def shift(tree, 0), do: tree

  def shift(%{"h" => [0, hlen]} = root, delta),
    do: shift_node(%{root | "h" => [-delta, hlen + delta]}, delta)

  defp shift_node(node, delta) do
    node
    |> Map.update!("h", fn [o, l] -> [o + delta, l] end)
    |> Map.update!("b", fn [o, l] -> [o + delta, l] end)
    |> then(fn n ->
      if n["c"], do: Map.update!(n, "c", &Enum.map(&1, fn c -> shift_node(c, delta) end)), else: n
    end)
    |> then(fn n -> if n["m"], do: Map.update!(n, "m", &shift_node(&1, delta)), else: n end)
  end
end
