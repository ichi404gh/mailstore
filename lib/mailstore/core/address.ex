defmodule Mailstore.Core.Address do
  @moduledoc """
  Address-list parsing for IMAP ENVELOPE: `"Name" <user@host>, other@host`.
  Enough for real-world headers; groups and comments are skipped.
  """

  @doc "Parse a header value into [%{name, mailbox, host}]."
  def parse_list(nil), do: []

  def parse_list(value) do
    value
    |> split_top_level()
    |> Enum.flat_map(&parse_one/1)
  end

  defp parse_one(part) do
    part = String.trim(part)

    {name, addr} =
      case Regex.run(~r/^(.*)<([^<>]*)>\s*$/s, part) do
        [_, name, addr] -> {clean_name(name), String.trim(addr)}
        nil -> {nil, part}
      end

    case String.split(addr, "@") do
      [_] ->
        []

      parts ->
        {host, mailbox} = List.pop_at(parts, -1)
        [%{name: name, mailbox: Enum.join(mailbox, "@"), host: host}]
    end
  end

  defp clean_name(name) do
    case name |> String.trim() |> String.trim(~s(")) do
      "" -> nil
      name -> String.replace(name, ~s(\\"), ~s("))
    end
  end

  # Split on commas that are not inside quotes or angle brackets.
  defp split_top_level(value) do
    {parts, current, _} =
      for <<c <- value>>, reduce: {[], [], {false, 0}} do
        {parts, cur, {quoted, angle}} ->
          case c do
            ?" -> {parts, [c | cur], {not quoted, angle}}
            ?< when not quoted -> {parts, [c | cur], {quoted, angle + 1}}
            ?> when not quoted -> {parts, [c | cur], {quoted, max(angle - 1, 0)}}
            ?, when not quoted and angle == 0 -> {[cur | parts], [], {quoted, angle}}
            _ -> {parts, [c | cur], {quoted, angle}}
          end
      end

    [current | parts]
    |> Enum.reverse()
    |> Enum.map(&(&1 |> Enum.reverse() |> IO.iodata_to_binary()))
    |> Enum.reject(&(String.trim(&1) == ""))
  end
end
