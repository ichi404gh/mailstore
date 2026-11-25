defmodule Mailstore.Imap.Parser do
  @moduledoc """
  Turns one complete client command (as cut by `Mailstore.Imap.Framing`, literals
  included) into `{tag, command, args}`.

  Tokens: `{:atom, bin}`, `{:string, bin}` (quoted or literal), `{:list, tokens}`.
  An atom may contain a bracketed section with spaces, so fetch attributes such as
  `BODY.PEEK[HEADER.FIELDS (FROM TO)]<0.100>` stay one token.
  """

  @type token :: {:atom, binary} | {:string, binary} | {:list, [token]}

  @spec parse(binary) :: {:ok, binary, binary, [token]} | {:error, binary | nil, binary}
  def parse(command) do
    command = strip_crlf(command)

    case tokenize(command) do
      {:ok, [{:atom, tag}, {:atom, cmd} | args]} ->
        case {String.upcase(cmd), args} do
          {"UID", [{:atom, sub} | rest]} -> {:ok, tag, "UID " <> String.upcase(sub), rest}
          {cmd, args} -> {:ok, tag, cmd, args}
        end

      {:ok, [{:atom, tag} | _]} ->
        {:error, tag, "Missing command"}

      {:ok, _} ->
        {:error, nil, "Invalid tag"}

      {:error, reason} ->
        {:error, tag_of(command), reason}
    end
  end

  @spec tokenize(binary) :: {:ok, [token]} | {:error, binary}
  def tokenize(bin) do
    case items(bin, [], false) do
      {:ok, tokens, _rest} -> {:ok, tokens}
      {:error, _} = error -> error
    end
  end

  defp items(<<>>, acc, false), do: {:ok, Enum.reverse(acc), <<>>}
  defp items(<<>>, _acc, true), do: {:error, "Unterminated list"}
  defp items(<<" ", rest::binary>>, acc, nested), do: items(rest, acc, nested)
  defp items(<<")", rest::binary>>, acc, true), do: {:ok, Enum.reverse(acc), rest}
  defp items(<<")", _::binary>>, _acc, false), do: {:error, "Unexpected )"}

  defp items(<<"(", rest::binary>>, acc, nested) do
    with {:ok, list, rest} <- items(rest, [], true),
         do: items(rest, [{:list, list} | acc], nested)
  end

  defp items(<<?", rest::binary>>, acc, nested) do
    with {:ok, string, rest} <- quoted(rest, []),
         do: items(rest, [{:string, string} | acc], nested)
  end

  defp items(<<"{", rest::binary>>, acc, nested) do
    with {:ok, string, rest} <- literal(rest),
         do: items(rest, [{:string, string} | acc], nested)
  end

  defp items(bin, acc, nested) do
    case atom_length(bin, 0, false) do
      0 ->
        {:error, "Unexpected character"}

      n ->
        items(
          binary_part(bin, n, byte_size(bin) - n),
          [{:atom, binary_part(bin, 0, n)} | acc],
          nested
        )
    end
  end

  defp atom_length(bin, i, _bracket) when i == byte_size(bin), do: i

  defp atom_length(bin, i, bracket) do
    case :binary.at(bin, i) do
      ?] when bracket -> atom_length(bin, i + 1, false)
      _ when bracket -> atom_length(bin, i + 1, true)
      ?[ -> atom_length(bin, i + 1, true)
      c when c in [?\s, ?(, ?), ?", ?\r, ?\n] -> i
      _ -> atom_length(bin, i + 1, false)
    end
  end

  defp quoted(<<?\\, c, rest::binary>>, acc), do: quoted(rest, [c | acc])

  defp quoted(<<?", rest::binary>>, acc),
    do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp quoted(<<c, rest::binary>>, acc), do: quoted(rest, [c | acc])
  defp quoted(<<>>, _acc), do: {:error, "Unterminated quoted string"}

  defp literal(bin) do
    with [_, digits] <- Regex.run(~r/^(\d+)\+?\}\r\n/, bin),
         n = String.to_integer(digits),
         header = byte_size(Regex.run(~r/^\d+\+?\}\r\n/, bin) |> hd()),
         true <- byte_size(bin) >= header + n do
      {:ok, binary_part(bin, header, n),
       binary_part(bin, header + n, byte_size(bin) - header - n)}
    else
      _ -> {:error, "Malformed literal"}
    end
  end

  defp strip_crlf(bin) do
    cond do
      String.ends_with?(bin, "\r\n") -> binary_part(bin, 0, byte_size(bin) - 2)
      String.ends_with?(bin, "\n") -> binary_part(bin, 0, byte_size(bin) - 1)
      true -> bin
    end
  end

  defp tag_of(command) do
    case :binary.split(command, " ") do
      [tag, _] when tag != "" -> tag
      _ -> nil
    end
  end
end
