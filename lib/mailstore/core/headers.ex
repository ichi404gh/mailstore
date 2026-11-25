defmodule Mailstore.Core.Headers do
  @moduledoc """
  Minimal RFC 5322 header parsing: enough for envelope, threading and indexes.
  A full MIME parser (BODYSTRUCTURE, part offsets) comes later, likely as a Rust NIF
  (`mail-parser`). Values are made valid UTF-8 here, because Postgres text and jsonb
  reject anything else and raw 8-bit headers do occur.
  """

  @doc "Split a raw message into {header_block, body}."
  def split(raw) do
    case :binary.match(raw, ["\r\n\r\n", "\n\n"]) do
      {pos, len} ->
        {binary_part(raw, 0, pos), binary_part(raw, pos + len, byte_size(raw) - pos - len)}

      :nomatch ->
        {raw, ""}
    end
  end

  @doc "Parse headers into [{lowercase_name, value}] in original order."
  def parse(raw) do
    {block, _body} = split(raw)

    block
    |> :binary.split(["\r\n", "\n"], [:global])
    |> unfold([])
    |> Enum.flat_map(fn line ->
      case :binary.split(line, ":") do
        [name, value] -> [{name |> String.trim() |> String.downcase(), clean(String.trim(value))}]
        _ -> []
      end
    end)
  end

  defp unfold([], acc), do: Enum.reverse(acc)

  defp unfold([<<c, _::binary>> = line | rest], [prev | acc]) when c in [?\s, ?\t],
    do: unfold(rest, [prev <> " " <> String.trim_leading(line) | acc])

  defp unfold(["" | rest], acc), do: unfold(rest, acc)
  defp unfold([line | rest], acc), do: unfold(rest, [line | acc])

  def get(headers, name) do
    case List.keyfind(headers, name, 0) do
      {_, value} -> value
      nil -> nil
    end
  end

  @doc "All <msg-id> tokens in a header value."
  def msg_ids(nil), do: []
  def msg_ids(value), do: ~r/<[^<>\s]+>/ |> Regex.scan(value) |> List.flatten()

  def first_msg_id(value), do: value |> msg_ids() |> List.first()

  @doc "Bare lowercase addr-spec from a From/To-like header."
  def address(nil), do: nil

  def address(value) do
    case Regex.run(~r/<([^<>\s]+@[^<>\s]+)>/, value) do
      [_, addr] ->
        String.downcase(addr)

      nil ->
        case Regex.run(~r/[^\s<>,;"]+@[^\s<>,;"]+/, value) do
          [addr] -> String.downcase(addr)
          nil -> nil
        end
    end
  end

  @envelope_fields ~w(date subject from sender reply-to to cc bcc in-reply-to message-id)

  @doc "Header values IMAP ENVELOPE is built from (formatting happens in the IMAP layer)."
  def envelope(headers) do
    Map.new(@envelope_fields, fn f -> {String.replace(f, "-", "_"), get(headers, f)} end)
  end

  defp clean(value) do
    value =
      if String.valid?(value), do: value, else: :unicode.characters_to_binary(value, :latin1)

    String.replace(value, <<0>>, "")
  end
end
