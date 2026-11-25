defmodule Mailstore.Imap.Fetch do
  @moduledoc """
  FETCH data items: parsing the request and rendering responses.

  Supported: UID, FLAGS, INTERNALDATE, RFC822.SIZE, ENVELOPE, RFC822, RFC822.HEADER,
  RFC822.TEXT, BODY[]/BODY.PEEK[] with sections "", HEADER, TEXT,
  HEADER.FIELDS (...), HEADER.FIELDS.NOT (...), and partial <start.len>; macros ALL and
  FAST. Not yet: BODYSTRUCTURE / BODY / FULL and MIME part sections (need a MIME parser).
  """
  alias Mailstore.Imap.Format

  @type section :: :full | :header | :text | {:fields, [binary]} | {:fields_not, [binary]}
  @type item ::
          :uid
          | :flags
          | :internaldate
          | :size
          | :envelope
          | {:body, peek :: boolean, section, partial :: nil | {non_neg_integer, pos_integer},
             name :: binary}

  @spec parse([Mailstore.Imap.Parser.token()]) :: {:ok, [item]} | {:error, binary}
  def parse([{:list, tokens}]), do: parse_items(tokens)
  def parse(tokens), do: parse_items(tokens)

  defp parse_items([]), do: {:error, "No fetch items"}

  defp parse_items(tokens) do
    tokens
    |> Enum.reduce_while({:ok, []}, fn
      {:atom, a}, {:ok, acc} ->
        case item(a) do
          {:ok, items} -> {:cont, {:ok, acc ++ items}}
          {:error, _} = e -> {:halt, e}
        end

      _, _ ->
        {:halt, {:error, "Invalid fetch item"}}
    end)
  end

  defp item(a) do
    case String.upcase(a) do
      "UID" -> {:ok, [:uid]}
      "FLAGS" -> {:ok, [:flags]}
      "INTERNALDATE" -> {:ok, [:internaldate]}
      "RFC822.SIZE" -> {:ok, [:size]}
      "ENVELOPE" -> {:ok, [:envelope]}
      "FAST" -> {:ok, [:flags, :internaldate, :size]}
      "ALL" -> {:ok, [:flags, :internaldate, :size, :envelope]}
      "RFC822" -> {:ok, [{:body, false, :full, nil, "RFC822"}]}
      "RFC822.HEADER" -> {:ok, [{:body, true, :header, nil, "RFC822.HEADER"}]}
      "RFC822.TEXT" -> {:ok, [{:body, false, :text, nil, "RFC822.TEXT"}]}
      x when x in ["BODY", "BODYSTRUCTURE", "FULL"] -> {:error, "#{x} is not implemented yet"}
      _ -> body_item(a)
    end
  end

  defp body_item(a) do
    re = ~r/^(BODY|BODY\.PEEK)\[([^\]]*)\](?:<(\d+)\.(\d+)>)?$/i

    case Regex.run(re, a) do
      [_, kind | rest] ->
        [spec | partial] = rest

        with {:ok, section, label} <- section(spec) do
          partial =
            case partial do
              [start, len] -> {String.to_integer(start), String.to_integer(len)}
              _ -> nil
            end

          name = "BODY[#{label}]" <> if(partial, do: "<#{elem(partial, 0)}>", else: "")
          {:ok, [{:body, String.upcase(kind) == "BODY.PEEK", section, partial, name}]}
        end

      nil ->
        {:error, "Unknown fetch item #{a}"}
    end
  end

  defp section(spec) do
    up = String.upcase(String.trim(spec))

    case Regex.run(~r/^HEADER\.FIELDS(\.NOT)?\s*\((.*)\)$/s, up) do
      [_, not_, names] ->
        names = names |> String.split() |> Enum.map(&String.trim(&1, ~s(")))
        kind = if not_ == "", do: :fields, else: :fields_not

        {:ok, {kind, Enum.map(names, &String.downcase/1)},
         "HEADER.FIELDS#{not_} (#{Enum.join(names, " ")})"}

      nil ->
        case up do
          "" -> {:ok, :full, ""}
          "HEADER" -> {:ok, :header, "HEADER"}
          "TEXT" -> {:ok, :text, "TEXT"}
          _ -> {:error, "Section #{spec} is not implemented yet"}
        end
    end
  end

  def needs_body?(items), do: Enum.any?(items, &match?({:body, _, _, _, _}, &1))
  def sets_seen?(items), do: Enum.any?(items, &match?({:body, false, _, _, _}, &1))

  @doc "One `* n FETCH (...)` line. `msg` is a `Mailbox.fetch_meta/3` row, `raw` the bytes or nil."
  def render(seq, items, msg, raw) do
    parts =
      items
      |> Enum.map(fn
        :uid ->
          ["UID ", Integer.to_string(msg.uid)]

        :flags ->
          ["FLAGS ", Format.flags(msg.flags)]

        :internaldate ->
          ["INTERNALDATE ", Format.datetime(msg.internal_date)]

        :size ->
          ["RFC822.SIZE ", Integer.to_string(msg.size)]

        :envelope ->
          ["ENVELOPE ", Format.envelope(msg.envelope)]

        {:body, _peek, section, partial, name} ->
          [name, " ", Format.literal(slice(extract(raw || "", section), partial))]
      end)
      |> Enum.intersperse(" ")

    ["* ", Integer.to_string(seq), " FETCH (", parts, ")\r\n"]
  end

  @doc false
  def extract(raw, :full), do: raw
  def extract(raw, :header), do: raw |> split() |> elem(0)
  def extract(raw, :text), do: raw |> split() |> elem(1)
  def extract(raw, {:fields, names}), do: header_fields(raw, &(&1 in names))
  def extract(raw, {:fields_not, names}), do: header_fields(raw, &(&1 not in names))

  # Header block including the empty line that ends it, and the text after it.
  defp split(raw) do
    case :binary.match(raw, ["\r\n\r\n", "\n\n"]) do
      {pos, len} ->
        {binary_part(raw, 0, pos + len), binary_part(raw, pos + len, byte_size(raw) - pos - len)}

      :nomatch ->
        {raw, ""}
    end
  end

  defp header_fields(raw, keep?) do
    {header, _} = split(raw)

    header
    |> String.split(~r/\r?\n/)
    |> Enum.reject(&(&1 == ""))
    |> Enum.chunk_while(
      [],
      fn
        <<c, _::binary>> = line, acc when c in [?\s, ?\t] and acc != [] -> {:cont, [line | acc]}
        line, [] -> {:cont, [line]}
        line, acc -> {:cont, Enum.reverse(acc), [line]}
      end,
      fn
        [] -> {:cont, []}
        acc -> {:cont, Enum.reverse(acc), []}
      end
    )
    |> Enum.filter(fn [first | _] ->
      case :binary.split(first, ":") do
        [name, _] -> keep?.(name |> String.trim() |> String.downcase())
        _ -> false
      end
    end)
    |> Enum.map_join(fn lines -> Enum.map_join(lines, &(&1 <> "\r\n")) end)
    |> Kernel.<>("\r\n")
  end

  defp slice(data, nil), do: data
  defp slice(data, {start, _len}) when start >= byte_size(data), do: ""
  defp slice(data, {start, len}), do: binary_part(data, start, min(len, byte_size(data) - start))
end
