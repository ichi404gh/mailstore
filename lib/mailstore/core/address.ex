defmodule Mailstore.Core.Address do
  @moduledoc """
  RFC 5322 address lists, as IMAP ENVELOPE needs them (RFC 3501 7.4.2).

  Handles display names (atoms, quoted strings), comments anywhere (also inside an
  addr-spec: `user (c)@ (c) host`), a comment as the name when there is no display name
  (`user@host (Real Name)`), source routes (`<@route:user@host>`) and groups
  (`team: a@x, b@y;`). A group becomes a start marker `mailbox: "team", host: nil`,
  its members, and an end marker with every field nil, which is how ENVELOPE encodes it.

  Never fails: unparsable pieces are skipped. Encoded words are passed through.
  """

  @type t :: %{name: binary | nil, adl: binary | nil, mailbox: binary | nil, host: binary | nil}

  @doc "Parse a header value into a list of `t:t/0`, group markers included."
  @spec parse_list(binary | nil) :: [t]
  def parse_list(nil), do: []
  def parse_list(value), do: value |> tokenize([]) |> items([])

  @doc "Addresses only (no group markers), e.g. for search."
  def mailboxes(value), do: value |> parse_list() |> Enum.filter(&(&1.host != nil))

  ## Tokens: {:word, text} (atom or quoted string), {:comment, text}, {:literal, text}
  ## (domain literal) and the specials < > @ , ; : as atoms.

  @specials ~c"<>@,;:"

  defp tokenize(<<>>, acc), do: Enum.reverse(acc)

  defp tokenize(<<c, rest::binary>>, acc) when c in [?\s, ?\t, ?\r, ?\n],
    do: tokenize(rest, acc)

  defp tokenize(<<?", rest::binary>>, acc) do
    {text, rest} = quoted(rest, [])
    tokenize(rest, [{:word, text, :quoted} | acc])
  end

  defp tokenize(<<?(, rest::binary>>, acc) do
    {text, rest} = comment(rest, 1, [])
    tokenize(rest, [{:comment, text} | acc])
  end

  defp tokenize(<<?[, rest::binary>>, acc) do
    case :binary.split(rest, "]") do
      [lit, rest] -> tokenize(rest, [{:literal, "[" <> lit <> "]"} | acc])
      [lit] -> tokenize("", [{:literal, "[" <> lit <> "]"} | acc])
    end
  end

  defp tokenize(<<c, rest::binary>>, acc) when c in @specials,
    do: tokenize(rest, [<<c>> | acc])

  defp tokenize(bin, acc) do
    {word, rest} = atom(bin, [])
    tokenize(rest, [{:word, word, :atom} | acc])
  end

  defp quoted(<<>>, acc), do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), ""}
  defp quoted(<<?\\, c, rest::binary>>, acc), do: quoted(rest, [c | acc])

  defp quoted(<<?", rest::binary>>, acc),
    do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp quoted(<<c, rest::binary>>, acc), do: quoted(rest, [c | acc])

  defp comment(<<>>, _depth, acc), do: {finish_comment(acc), ""}
  defp comment(<<?\\, c, rest::binary>>, depth, acc), do: comment(rest, depth, [c | acc])
  defp comment(<<?(, rest::binary>>, depth, acc), do: comment(rest, depth + 1, [?( | acc])
  defp comment(<<?), rest::binary>>, 1, acc), do: {finish_comment(acc), rest}
  defp comment(<<?), rest::binary>>, depth, acc), do: comment(rest, depth - 1, [?) | acc])
  defp comment(<<c, rest::binary>>, depth, acc), do: comment(rest, depth, [c | acc])

  defp finish_comment(acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary() |> String.trim()

  defp atom(<<c, _::binary>> = rest, acc)
       when c in [?\s, ?\t, ?\r, ?\n, ?", ?(, ?[] or c in @specials,
       do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp atom(<<c, rest::binary>>, acc), do: atom(rest, [c | acc])
  defp atom(<<>>, acc), do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), ""}

  ## Items: groups and mailboxes, separated by commas.

  defp items([], acc), do: Enum.reverse(acc)
  defp items(["," | rest], acc), do: items(rest, acc)
  defp items([";" | rest], acc), do: items(rest, acc)

  defp items(tokens, acc) do
    if group?(tokens) do
      {phrase, [":" | rest]} = Enum.split_while(tokens, &(&1 != ":"))
      {members, rest} = group_members(rest, [])
      start = %{name: nil, adl: nil, mailbox: phrase_text(phrase) || "", host: nil}
      finish = %{name: nil, adl: nil, mailbox: nil, host: nil}
      items(rest, Enum.reverse([start | members] ++ [finish], acc))
    else
      {one, rest} = take_mailbox(tokens, 0, [])
      items(rest, Enum.reverse(mailbox(one), acc))
    end
  end

  # A phrase followed by ":" before any "<", "@", "," or ";" starts a group.
  defp group?([":" | _]), do: true
  defp group?([t | rest]) when is_tuple(t), do: group?(rest)
  defp group?(_), do: false

  defp group_members([], acc), do: {Enum.reverse(acc), []}
  defp group_members([";" | rest], acc), do: {Enum.reverse(acc), rest}
  defp group_members(["," | rest], acc), do: group_members(rest, acc)

  defp group_members(tokens, acc) do
    {one, rest} = take_mailbox(tokens, 0, [])
    group_members(rest, Enum.reverse(mailbox(one), acc))
  end

  # Tokens of one mailbox: up to a "," or ";" outside angle brackets.
  defp take_mailbox([], _depth, acc), do: {Enum.reverse(acc), []}

  defp take_mailbox([t | _] = rest, 0, acc) when t in [",", ";"],
    do: {Enum.reverse(acc), rest}

  defp take_mailbox(["<" | rest], depth, acc), do: take_mailbox(rest, depth + 1, ["<" | acc])

  defp take_mailbox([">" | rest], depth, acc),
    do: take_mailbox(rest, max(depth - 1, 0), [">" | acc])

  defp take_mailbox([t | rest], depth, acc), do: take_mailbox(rest, depth, [t | acc])

  defp mailbox([]), do: []

  defp mailbox(tokens) do
    case Enum.split_while(tokens, &(&1 != "<")) do
      {phrase, ["<" | inside]} ->
        inside = Enum.take_while(inside, &(&1 != ">"))
        {adl, spec} = route(inside)
        addr_spec(spec, phrase_text(phrase), adl)

      {spec, []} ->
        # No angle brackets: a trailing comment is the name (`user@host (Real Name)`).
        name =
          Enum.find_value(Enum.reverse(spec), fn t -> match?({:comment, _}, t) && elem(t, 1) end)

        addr_spec(spec, if(name == "", do: nil, else: name), nil)
    end
  end

  # `@a,@b:` before the addr-spec.
  defp route(["@" | _] = tokens) do
    case Enum.split_while(tokens, &(&1 != ":")) do
      {route, [":" | spec]} -> {route |> Enum.map(&token_text/1) |> Enum.join(), spec}
      {_, []} -> {nil, tokens}
    end
  end

  defp route(tokens), do: {nil, tokens}

  defp addr_spec(tokens, name, adl) do
    tokens = Enum.reject(tokens, &match?({:comment, _}, &1))

    {local, domain} =
      case Enum.split_while(tokens, &(&1 != "@")) do
        {local, ["@" | domain]} -> {local, domain}
        {local, []} -> {local, []}
      end

    # Placeholders for a missing part, as Dovecot sends them (host NIL means a group).
    case {join(local), join(domain)} do
      {"", ""} ->
        []

      {local, host} ->
        [
          %{
            name: name,
            adl: adl,
            mailbox: or_missing(local, "MISSING_MAILBOX"),
            host: or_missing(host, "MISSING_DOMAIN")
          }
        ]
    end
  end

  defp or_missing("", placeholder), do: placeholder
  defp or_missing(value, _placeholder), do: value

  defp join(tokens), do: tokens |> Enum.map(&token_text/1) |> Enum.join()

  defp token_text({:word, text, _}), do: text
  defp token_text({:literal, text}), do: text
  defp token_text({:comment, _}), do: ""
  defp token_text(special) when is_binary(special), do: special

  # Display name: words joined by spaces, comments dropped.
  defp phrase_text(tokens) do
    case for({:word, text, _} <- tokens, do: text) do
      [] -> nil
      words -> Enum.join(words, " ")
    end
  end
end
