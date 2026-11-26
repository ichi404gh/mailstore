defmodule Mailstore.Imap.Format do
  @moduledoc "Rendering IMAP data items: strings, flag lists, dates, ENVELOPE, BODYSTRUCTURE."
  alias Mailstore.Core.Address

  @months ~w(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)

  @doc "nstring: NIL, a quoted string when safe, otherwise a literal."
  def string(nil), do: "NIL"

  def string(s) when is_binary(s) do
    if quotable?(s), do: [?", escape(s), ?"], else: literal(s)
  end

  def literal(s), do: ["{", Integer.to_string(byte_size(s)), "}\r\n", s]

  defp quotable?(s), do: byte_size(s) <= 1024 and s =~ ~r/^[\x20-\x7e]*$/

  defp escape(s), do: s |> String.replace("\\", "\\\\") |> String.replace(~s("), ~s(\\"))

  def flags(list), do: ["(", Enum.join(list, " "), ")"]

  @doc ~s(date-time: "20-Nov-2026 12:00:00 +0000")
  def datetime(%DateTime{} = dt) do
    dt = DateTime.shift_zone!(dt, "Etc/UTC")

    [
      ?",
      pad(dt.day),
      ?-,
      Enum.at(@months, dt.month - 1),
      ?-,
      Integer.to_string(dt.year),
      ?\s,
      pad(dt.hour),
      ?:,
      pad(dt.minute),
      ?:,
      pad(dt.second),
      " +0000",
      ?"
    ]
  end

  @doc "date-time from APPEND, e.g. \" 5-Nov-2026 12:00:00 +0300\"."
  def parse_datetime(s) do
    re = ~r/^\s?(\d{1,2})-([A-Za-z]{3})-(\d{4}) (\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})$/

    with [_, d, mon, y, h, mi, sec, sign, oh, om] <- Regex.run(re, s),
         month when month != nil <- month_number(mon),
         {:ok, naive} <-
           NaiveDateTime.new(int(y), month, int(d), int(h), int(mi), int(sec), {0, 6}) do
      offset = (int(oh) * 60 + int(om)) * 60 * if(sign == "-", do: -1, else: 1)
      {:ok, naive |> NaiveDateTime.add(-offset) |> DateTime.from_naive!("Etc/UTC")}
    else
      _ -> :error
    end
  end

  @doc "date from SEARCH, e.g. 1-Feb-1994."
  def parse_date(s) do
    with [_, d, mon, y] <- Regex.run(~r/^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/, s),
         month when month != nil <- month_number(mon),
         {:ok, date} <- Date.new(int(y), month, int(d)) do
      {:ok, date}
    else
      _ -> :error
    end
  end

  @doc "ENVELOPE from the header values stored at delivery."
  def envelope(env) when is_map(env) do
    from = Address.parse_list(env["from"])
    # Sender and Reply-To default to From, also when present but empty (RFC 3501 7.4.2).
    or_from = fn value -> with [] <- Address.parse_list(value), do: from end

    [
      "(",
      Enum.intersperse(
        [
          string(squeeze(env["date"])),
          string(squeeze(env["subject"])),
          addresses(from),
          addresses(or_from.(env["sender"])),
          addresses(or_from.(env["reply_to"])),
          addresses(Address.parse_list(env["to"])),
          addresses(Address.parse_list(env["cc"])),
          addresses(Address.parse_list(env["bcc"])),
          string(squeeze(env["in_reply_to"])),
          string(squeeze(env["message_id"]))
        ],
        " "
      ),
      ")"
    ]
  end

  def envelope(nil), do: envelope(%{})

  # Runs of spaces and tabs become one space, as in Dovecot's ENVELOPE.
  defp squeeze(nil), do: nil
  defp squeeze(value), do: String.replace(value, ~r/[ \t]+/, " ")

  @doc """
  BODY (`extensible? = false`) or BODYSTRUCTURE from a `Mailstore.Core.Mime` tree.
  Types and parameters are sent as stored (lowercase types, parameters as written),
  the way Dovecot does.
  """
  def body_structure(node, extensible?)

  def body_structure(%{"c" => children} = node, ext?) do
    [
      "(",
      Enum.map(children, &body_structure(&1, ext?)),
      " ",
      string(node["s"]),
      if(ext?,
        do: [
          " ",
          params(node["p"]),
          " ",
          disposition(node["dp"]),
          " ",
          language(node["l"]),
          " ",
          string(node["lo"])
        ],
        else: []
      ),
      ")"
    ]
  end

  def body_structure(node, ext?) do
    [_, size] = node["b"]

    fields = [
      string(node["t"]),
      string(node["s"]),
      params(node["p"]),
      string(node["id"]),
      string(node["ds"]),
      string(node["e"]),
      Integer.to_string(size)
    ]

    type_specific =
      case node do
        %{"t" => "message", "s" => "rfc822"} ->
          # An encoded or too deeply nested message is not parsed; the grammar still
          # wants an envelope and a body, so send empty ones.
          inner =
            node["m"] ||
              %{"t" => "text", "s" => "plain", "p" => [], "e" => "7bit", "b" => [0, 0], "n" => 0}

          [envelope(node["env"]), body_structure(inner, ext?), Integer.to_string(node["n"] || 0)]

        %{"t" => "text"} ->
          [Integer.to_string(node["n"] || 0)]

        _ ->
          []
      end

    ext =
      if ext?,
        do: [
          string(node["md5"]),
          disposition(node["dp"]),
          language(node["l"]),
          string(node["lo"])
        ],
        else: []

    ["(", Enum.intersperse(fields ++ type_specific ++ ext, " "), ")"]
  end

  defp params(nil), do: "NIL"
  defp params([]), do: "NIL"

  defp params(list) do
    [
      "(",
      list |> Enum.flat_map(fn [k, v] -> [string(k), string(v)] end) |> Enum.intersperse(" "),
      ")"
    ]
  end

  defp disposition([type, p]), do: ["(", string(type), " ", params(p), ")"]
  defp disposition(_), do: "NIL"

  defp language([tag]), do: string(tag)

  defp language([_ | _] = tags),
    do: ["(", tags |> Enum.map(&string/1) |> Enum.intersperse(" "), ")"]

  defp language(_), do: "NIL"

  defp addresses([]), do: "NIL"

  defp addresses(list) do
    [
      "(",
      for(
        a <- list,
        do: [
          "(",
          Enum.map_intersperse([a.name, a.adl, a.mailbox, a.host], " ", &string/1),
          ")"
        ]
      ),
      ")"
    ]
  end

  defp month_number(mon) do
    case Enum.find_index(@months, &(String.downcase(&1) == String.downcase(mon))) do
      nil -> nil
      i -> i + 1
    end
  end

  defp pad(n), do: n |> Integer.to_string() |> String.pad_leading(2, "0")
  defp int(s), do: String.to_integer(s)
end
